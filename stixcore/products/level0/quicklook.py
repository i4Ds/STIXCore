"""
High level STIX data products created from single stand alone packets or a sequence of packets.
"""
from itertools import chain

import numpy as np

import astropy.units as u
from astropy.table import unique, vstack

from stixcore.datetime.datetime import DateTime
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_energy_bins,
    _get_num_energies,
    _get_pixel_mask,
    _get_sub_spectrum_mask,
    rebin_proportional,
)
from stixcore.products.product import BaseProduct, Control, Data
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve', 'Background', 'Spectra']

logger = get_logger(__name__)


class QLProduct(BaseProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        """
        Generic product composed of control and data

        Parameters
        ----------
        control : stix_parser.products.quicklook.Control
            Table containing control information
        data : stix_parser.products.quicklook.Data
            Table containing data
        """

        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.type = 'ql'
        self.control = control
        self.data = data

        self.obs_beg = DateTime.from_float(self.data['time'][0]
                                           - self.control['integration_time'][0] / 2)
        self.obs_end = DateTime.from_float(self.data['time'][-1]
                                           + self.control['integration_time'][-1] / 2)
        self.obs_avg = self.obs_beg + (self.obs_end - self.obs_beg) / 2

    def __add__(self, other):
        """
        Combine two products stacking data along columns and removing duplicated data using time as
        the primary key.

        Parameters
        ----------
        other : A subclass of stix_parser.products.quicklook.QLProduct

        Returns
        -------
        A subclass of stix_parser.products.quicklook.QLProduct
            The combined data product
        """
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        # TODO reindex and update data control_index
        other.control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other.control))
        # control = unique(control, keys=['scet_coarse', 'scet_fine'])
        # control = control.group_by(['scet_coarse', 'scet_fine'])

        other.data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1
        data = vstack((self.data, other.data))
        data = unique(data, keys='time')
        # data = data.group_by('time')
        unique_control_inds = np.unique(data['control_index'])
        control = control[np.isin(control['index'], unique_control_inds)]

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data)

    def __repr__(self):
        return f'<{self.__class__.__name__}\n' \
               f' {self.control.__repr__()}\n' \
               f' {self.data.__repr__()}\n' \
               f'>'

    def to_days(self):
        days = range(int((self.obs_beg.as_float() / u.d).decompose().value),
                     int((self.obs_end.as_float() / u.d).decompose().value))
        # days = set([(t.year, t.month, t.day) for t in self.data['time'].to_datetime()])
        # date_ranges = [(datetime(*day), datetime(*day) + timedelta(days=1)) for day in days]
        for day in days:
            i = np.where((self.data['time'] >= day * u.day) &
                         (self.data['time'] < (day + 1) * u.day))

            data = self.data[i]
            control_indices = np.unique(data['control_index'])
            control = self.control[np.isin(self.control['index'], control_indices)]
            control_index_min = control_indices.min()

            data['control_index'] = data['control_index'] - control_index_min
            control['index'] = control['index'] - control_index_min
            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data)


class LightCurve(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-lightcurve'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):

        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)
        control['detector_mask'] = _get_detector_mask(packets)
        control['detector_mask'].meta = {'NIXS': 'NIX00407'}

        control['pixel_mask'] = _get_pixel_mask(packets)
        control['pixel_mask'].meta = {'NIXS': 'NIXD0407'}
        control['energy_bin_edge_mask'] = _get_energy_bins(packets, 'NIX00266', 'NIXD0107')
        control['energy_bin_edge_mask'].meta = {'NIXS': ['NIX00266', 'NIXD0107']}

        control['num_energies'] = _get_num_energies(packets)
        control['num_energies'].meta = {'NIXS': 'NIX00270'}
        control['num_samples'] = np.array(packets.get_value('NIX00271')).flatten()[
            np.cumsum(control['num_energies']) - 1]
        control['num_samples'].meta = {'NIXS': 'NIX00271'}

        time, duration = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        control['compression_scheme_counts_skm'], control['compression_scheme_counts_skm'].meta =\
            _get_compression_scheme(packets, ['NIXD0101', 'NIXD0102', 'NIXD0103'])
        counts = np.array(packets.get_value('NIX00272')).reshape(control['num_energies'][0],
                                                                 control['num_samples'][0])
        counts_var = np.array(packets.get_value('NIX00272', attr="error")).\
            reshape(control['num_energies'][0], control['num_samples'][0])

        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, ['NIXD0104', 'NIXD0105', 'NIXD0104'])

        triggers = np.hstack(packets.get_value('NIX00274'))
        triggers_var = np.hstack(packets.get_value('NIX00274', attr="error"))

        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data['triggers'] = triggers
        data['triggers'].meta = {'NIXS': 'NIX00274'}
        data['triggers_err'] = np.sqrt(triggers_var)
        data['rcr'] = np.hstack(packets.get_value('NIX00276')).flatten()
        data['rcr'].meta = {'NIXS': 'NIX00276'}
        data['counts'] = counts.T
        data['counts'].meta = {'NIXS': 'NIX00272'}
        data['counts_err'] = np.sqrt(counts_var).T * u.ct

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 30)


class Background(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-background'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)
        control['energy_bin_edge_mask'] = _get_energy_bins(packets, 'NIX00266', 'NIXD0111')
        control['energy_bin_edge_mask'].meta = {'NIXS': ['NIX00266', 'NIXD0111']}

        control['num_energies'] = _get_num_energies(packets)
        control['num_energies'].meta = {'NIXS': 'NIX00270'}
        control['num_samples'] = np.array(packets.get_value('NIX00277')).flatten()[
            np.cumsum(control['num_energies']) - 1]
        control['num_samples'].meta = {'NIXS': 'NIX00277'}

        time, duration = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        control['compression_scheme_counts_skm'], control['compression_scheme_counts_skm'].meta =\
            _get_compression_scheme(packets, ['NIXD0108', 'NIXD0109', 'NIXD0110'])

        counts = np.array(packets.get_value('NIX00278')).reshape(control['num_energies'][0],
                                                                 control['num_samples'].sum())
        counts_var = np.array(packets.get_value('NIX00278', attr="error")).\
            reshape(control['num_energies'][0], control['num_samples'].sum())

        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, ['NIXD0112', 'NIXD0113', 'NIXD0114'])

        triggers = packets.get_value('NIX00274')
        triggers_var = packets.get_value('NIX00274', attr="error")

        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data['triggers'] = triggers
        data['triggers'].meta = {'NIXS': 'NIX00274'}
        data['triggers_err'] = np.sqrt(triggers_var)
        data['counts'] = counts.T
        data['counts'].meta = {'NIXS': 'NIX00278'}
        data['counts_err'] = np.sqrt(counts_var).T * u.ct

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 31)


class Spectra(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-spectra'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)
        control['pixel_mask'] = _get_pixel_mask(packets)
        control['compression_scheme_spectra_skm'], control['compression_scheme_spectra_skm'].meta =\
            _get_compression_scheme(packets, ['NIXD0115', 'NIXD0116', 'NIXD0117'])
        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, ['NIXD0112', 'NIXD0113', 'NIXD0114'])

        # Fixed for spectra
        num_energies = 32
        control['num_energies'] = num_energies
        control['num_samples'] = packets.get_value('NIX00089')

        # Due to the way packets are split up full contiguous block of detector 1-32 are not always
        # down-linked to the ground so need to pad the array to write to table and later fits
        total_samples = control['num_samples'].sum()
        full, partial = divmod(total_samples, 32)
        pad_after = 0
        if partial != 0:
            pad_after = 32 - partial

        control_indices = np.pad(np.hstack([np.full(ns, cind) for ns, cind in
                                            control[['num_samples', 'index']]]), (0, pad_after),
                                 constant_values=-1)
        control_indices = control_indices.reshape(-1, 32)

        duration, time = cls._get_time(control, num_energies, packets, pad_after)

        # sample x detector x energy
        # counts = np.array([eng_packets.get('NIX00{}'.format(i)) for i in range(452, 484)],
        #                   np.uint32).T * u.ct

        counts = []
        counts_var = []
        for i in range(452, 484):
            counts.append(packets.get_value('NIX00{}'.format(i)))
            counts_var.append(packets.get_value('NIX00{}'.format(i)))
        counts = np.vstack(counts).T
        counts_var = np.vstack(counts_var).T

        counts = np.pad(counts, ((0, pad_after), (0, 0)), constant_values=0)
        counts_var = np.pad(counts_var, ((0, pad_after), (0, 0)), constant_values=0)

        triggers = packets.get_value('NIX00484').T.reshape(-1)
        triggers_var = packets.get_value('NIX00484', attr='error').T.reshape(-1)

        triggers = np.pad(triggers, (0, pad_after), constant_values=0)
        triggers_var = np.pad(triggers_var, (0, pad_after), constant_values=0)

        detector_index = np.pad(np.array(packets.get_value('NIX00100'), np.int16), (0, pad_after),
                                constant_values=-1)
        num_integrations = np.pad(np.array(packets.get_value('NIX00485'), np.uint16),
                                  (0, pad_after), constant_values=0)

        # Data
        data = Data()
        data['control_index'] = control_indices[:, 0]
        data['time'] = time[:, 0]
        data['timedel'] = duration[:, 0]
        data['detector_index'] = detector_index.reshape(-1, 32) * u.ct
        data['spectra'] = counts.reshape(-1, 32, num_energies) * u.ct
        data['spectra_err'] = np.sqrt(counts_var.reshape(-1, 32, num_energies))
        data['triggers'] = triggers.reshape(-1, num_energies)
        data['triggers_err'] = np.sqrt(triggers_var.reshape(-1, num_energies))
        data['num_integrations'] = num_integrations.reshape(-1, num_energies)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def _get_time(cls, control, num_energies, packets, pad_after):
        times = []
        durations = []
        start = 0
        for i, (ns, it) in enumerate(control['num_samples', 'integration_time']):
            off_sets = np.array(packets.get_value('NIX00485')[start:start + ns]) * it
            base_time = DateTime(control["scet_coarse"][i], control["scet_fine"][i])
            start_times = base_time.as_float() + off_sets
            end_times = base_time.as_float() + off_sets + it
            cur_time = start_times + (end_times - start_times) / 2
            times.extend(cur_time)
            durations.extend([it]*ns)
            start += ns
        time = np.hstack(times)
        time = np.pad(time, (0, pad_after), constant_values=time[-1])
        time = time.reshape(-1, num_energies)
        duration = np.pad(np.hstack(durations), (0, pad_after)).reshape(-1, num_energies)
        return duration, time

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 32)


class Variance(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-variance'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)

        # Control
        control['samples_per_variance'] = np.array(packets.get_value('NIX00279'), np.ubyte)
        control['pixel_mask'] = _get_pixel_mask(packets)
        control['detector_mask'] = _get_detector_mask(packets)
        control['compression_scheme_variance_skm'], control['compression_scheme_variance_skm'].meta\
            = _get_compression_scheme(packets, ['NIXD0118', 'NIXD0119', 'NIXD0120'])

        energy_masks = np.array([
            [bool(int(x)) for x in format(packets.get_value('NIX00282')[i], '032b')]
            for i in range(len(packets.get_value('NIX00282')))])

        control['energy_bin_mask'] = energy_masks
        control['num_energies'] = 1
        control['num_samples'] = packets.get_value('NIX00280')

        time, duration = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        variance = packets.get_value('NIX00281')
        variance_var = packets.get_value('NIX00281', attr='error')

        # Data
        data = Data()
        data['time'] = time
        data['timedel'] = duration
        data['control_index'] = control_indices
        data['variance'] = variance
        data['variance_err'] = np.sqrt(variance_var)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 33)


class FlareFlag(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-flareflag'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)

        control['num_samples'] = packets.get_value('NIX00089')

        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        time, duration = control._get_time()

        # DATA
        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['duration'] = duration
        data['loc_z'] = packets.get_value('NIX00283').astype(np.int16)
        data['loc_y'] = packets.get_value('NIX00284').astype(np.int16)
        data['thermal_index'] = packets.get_value('NIXD0061', attr='value').astype(np.uint16)
        data['non_thermal_index'] = packets.get_value('NIXD0060', attr='value').astype(np.uint16)
        data['location_status'] = packets.get_value('NIXD0059', attr='value').astype(np.uint16)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 34)


class EnergyCalibration(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-energycalibration'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)

        control['integration_time'] = (packets.get_value('NIX00122') + 1) * 0.1 * u.s
        # control['obs_beg'] = control['obs_utc']
        # control['.obs_end'] = control['obs_beg'] + timedelta(seconds=control[
        # 'duration'].astype('float'))
        # control['.obs_avg'] = control['obs_beg'] + (control['obs_end'] - control['obs_beg']) / 2

        # Control
        control['quiet_time'] = packets.get_value('NIX00123').astype(np.uint16)
        control['live_time'] = packets.get_value('NIX00124').astype(np.uint32)
        control['average_temperature'] = packets.get_value('NIX00125').astype(np.uint16)
        control['detector_mask'] = _get_detector_mask(packets)
        control['pixel_mask'] = _get_pixel_mask(packets)
        control['subspectrum_mask'] = _get_sub_spectrum_mask(packets)
        control['compression_scheme_counts_skm'], control['compression_scheme_counts_skm'].meta \
            = _get_compression_scheme(packets, ['NIXD0126', 'NIXD0127', 'NIXD0128'])
        subspec_data = {}
        j = 129
        for subspec, i in enumerate(range(300, 308)):
            subspec_data[subspec+1] = {'num_points': packets.get_value(f'NIXD0{j}')[0],
                                       'num_summed_channel': packets.get_value(f'NIXD0{j + 1}')[0],
                                       'lowest_channel': packets.get_value(f'NIXD0{j + 2}')[0]}
            j += 3

        control['num_samples'] = packets.get_value('NIX00159').astype(np.uint16)
        # control.remove_column('index')
        # control = unique(control)
        # control['index'] = np.arange(len(control))

        control['subspec_num_points'] = np.array(
            [v['num_points'] for v in subspec_data.values()]).reshape(1, -1)
        control['subspec_num_summed_channel'] = np.array(
            [v['num_summed_channel'] for v in subspec_data.values()]).reshape(1, -1)
        control['subspec_lowest_channel'] = np.array(
            [v['lowest_channel'] for v in subspec_data.values()]).reshape(1, -1)

        subspec_index = np.argwhere(control['subspectrum_mask'][0].flatten() == 1)
        num_sub_spectra = control['subspectrum_mask'].sum(axis=1)
        sub_channels = [np.arange(control['subspec_num_points'][0, index] + 1)
                        * (control['subspec_num_summed_channel'][0, index] + 1)
                        + control['subspec_lowest_channel'][0, index] for index in subspec_index]
        channels = list(chain(*[ch.tolist() for ch in sub_channels]))
        control['num_channels'] = len(channels)

        # Data
        data = Data()
        data['control_index'] = [0]
        data['time'] = (DateTime(control['scet_coarse'][0], control['scet_fine'][0]).as_float()
                        + control['integration_time'][0] / 2).reshape(1)
        data['timedel'] = control['integration_time'][0]
        # data['detector_id'] = np.array(packets.get('NIXD0155'), np.ubyte)
        # data['pixel_id'] = np.array(packets.get('NIXD0156'), np.ubyte)
        # data['subspec_id'] = np.array(packets.get('NIXD0157'), np.ubyte)
        np.array(packets.get('NIX00146'))

        counts = packets.get_value('NIX00158')
        counts_var = packets.get_value('NIX00158', attr='error')

        counts_rebinned = np.apply_along_axis(rebin_proportional, 1,
                                              counts.reshape(-1, len(channels)), channels,
                                              np.arange(1025))

        counts_var_rebinned = np.apply_along_axis(rebin_proportional, 1,
                                                  counts_var.reshape(-1, len(channels)), channels,
                                                  np.arange(1025))

        dids = np.array(packets.get_value('NIXD0155'),
                        np.ubyte).reshape(-1, num_sub_spectra[0])[:, 0]
        pids = np.array(packets.get_value('NIXD0156'),
                        np.ubyte).reshape(-1, num_sub_spectra[0])[:, 0]

        full_counts = np.zeros((32, 12, 1024))
        full_counts[dids, pids] = counts_rebinned
        full_counts_var = np.zeros((32, 12, 1024))
        full_counts_var[dids, pids] = counts_var_rebinned
        data['counts'] = full_counts.reshape((1, *full_counts.shape))
        data['counts_err'] = np.sqrt(full_counts_var).reshape((1, *full_counts_var.shape))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 41)


class TMStatusFlareList(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'ql-tmstatusflarelist'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['ubsd_counter'] = packets.get_value('NIX00285')
        control['pald_counter'] = packets.get_value('NIX00286')
        control['num_flares'] = packets.get_value('NIX00294')

        data = Data()
        if control['num_flares'].sum() > 0:
            data['start_scet_coarse'] = packets.get_value('NIX00287')
            data['end_scet_coarse'] = packets.get_value('NIX00287')

            data['time'] = DateTime(packets.get_value('NIX00287'), packets.get_value('NIX00287'))
            data['highest_flareflag'] = packets.get_value('NIX00289')
            data['tm_byte_volume'] = packets.get('NIX00290')
            data['average_z_loc'] = packets.get('NIX00291')
            data['average_y_loc'] = packets.get('NIX00292')
            data['processing_mask'] = packets.get('NIX00293')

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 43)

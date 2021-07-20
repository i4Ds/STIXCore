"""
High level STIX data products created from single stand alone packets or a sequence of packets.
"""
import logging
from itertools import chain
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.table import vstack
from astropy.table.operations import unique

from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_energies_from_mask,
    _get_energy_bins,
    _get_pixel_mask,
    _get_sub_spectrum_mask,
    rebin_proportional,
)
from stixcore.products.product import BaseProduct, Control, Data
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve', 'Background', 'Spectra']

logger = get_logger(__name__, level=logging.DEBUG)

QLNIX00405_offset = 0.1


class QLProduct(BaseProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
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
        self.idb_versions = idb_versions
        self.scet_timerange = kwargs.get('scet_timerange')

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
        other_control = other.control[:]
        other_data = other.data[:]
        other_control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other_control))
        # control = unique(control, keys=['scet_coarse', 'scet_fine'])
        # control = control.group_by(['scet_coarse', 'scet_fine'])

        other_data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        logger.debug('len self: %d, len other %d', len(self.data), len(other_data))

        data = vstack((self.data, other_data))

        logger.debug('len stacked %d', len(data))

        data['time_float'] = data['time'].as_float()

        data = unique(data, keys=['time_float'])

        logger.debug('len unique %d', len(data))

        data.remove_column('time_float')

        unique_control_inds = np.unique(data['control_index'])
        control = control[np.nonzero(control['index'][:, None] == unique_control_inds)[1]]

        for idb_key, date_range in other.idb_versions.items():
            self.idb_versions[idb_key].expand(date_range)

        scet_timerange = SCETimeRange(start=data['time'][0]-data['timedel'][0]/2,
                                      end=data['time'][-1]+data['timedel'][-1]/2)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data,
                          idb_versions=self.idb_versions, scet_timerange=scet_timerange)

    def __repr__(self):
        return f'<{self.__class__.__name__}, {self.scet_timerange.start} to ' \
               f'{self.scet_timerange.start}, {len(self.control)}, {len(self.data)}>'

    def to_days(self):
        start_day = int((self.scet_timerange.start.as_float() / u.d).decompose().value)
        end_day = int((self.scet_timerange.end.as_float() / u.d).decompose().value)
        days = range(start_day, end_day+1)
        # days = set([(t.year, t.month, t.day) for t in self.data['time'].to_datetime()])
        # date_ranges = [(datetime(*day), datetime(*day) + timedelta(days=1)) for day in days]
        for day in days:
            ds = SCETime(((day * u.day).to_value('s')).astype(int), 0)
            de = SCETime((((day + 1) * u.day).to_value('s')).astype(int), 0)
            i = np.where((self.data['time'] >= ds) & (self.data['time'] < de))

            scet_timerange = SCETimeRange(start=ds, end=de)

            if i[0].size > 0:
                data = self.data[i]
                control_indices = np.unique(data['control_index'])
                control = self.control[np.isin(self.control['index'], control_indices)]
                control_index_min = control_indices.min()

                data['control_index'] = data['control_index'] - control_index_min
                control['index'] = control['index'] - control_index_min
                yield type(self)(service_type=self.service_type,
                                 service_subtype=self.service_subtype,
                                 ssid=self.ssid, control=control, data=data,
                                 idb_versions=self.idb_versions, scet_timerange=scet_timerange)

    def get_energies(self):
        if 'energy_bin_edge_mask' in self.control.colnames:
            energies = _get_energies_from_mask(self.control['energy_bin_edge_mask'][0])
        elif 'energy_bin_mask' in self.control.colnames:
            energies = _get_energies_from_mask(self.control['energy_bin_mask'][0])
        else:
            energies = _get_energies_from_mask()

        return energies


class LightCurve(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'lightcurve'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):

        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets, NIX00405_offset=QLNIX00405_offset)
        control.add_data('detector_mask', _get_detector_mask(packets))
        control.add_data('pixel_mask', _get_pixel_mask(packets))
        control.add_data('energy_bin_edge_mask', _get_energy_bins(packets, 'NIX00266', 'NIXD0107'))
        control.add_basic(name='num_energies', nix='NIX00270', packets=packets)

        control['num_samples'] = np.array(packets.get_value('NIX00271')).flatten()[
            np.cumsum(control['num_energies']) - 1]
        control.add_meta(name='num_samples', nix='NIX00270', packets=packets)

        time, duration, scet_timerange = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00272'))

        counts_flat = packets.get_value('NIX00272')
        counts_var_flat = packets.get_value('NIX00272', attr='error')

        flat_indices = np.hstack((0, np.cumsum([*control['num_samples']]) *
                                  control['num_energies'])).astype(int)

        counts = np.hstack([
            counts_flat[flat_indices[i]:flat_indices[i + 1]].reshape(n_eng, n_sam)
            for i, (n_sam, n_eng) in enumerate(control[['num_samples', 'num_energies']])])

        counts_var = np.hstack([
            counts_var_flat[flat_indices[i]:flat_indices[i + 1]].reshape(n_eng, n_sam)
            for i, (n_sam, n_eng) in enumerate(control[['num_samples', 'num_energies']])])

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00274'))

        triggers = packets.get_value('NIX00274').T
        triggers_var = packets.get_value('NIX00274', attr="error").T

        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data.add_meta(name='timedel', nix='NIX00405', packets=packets)
        data['triggers'] = triggers
        data.add_meta(name='triggers', nix='NIX00274', packets=packets)
        data['triggers_err'] = np.sqrt(triggers_var)
        data['rcr'] = np.hstack(packets.get_value('NIX00276')).flatten()
        data.add_meta(name='rcr', nix='NIX00276', packets=packets)
        data['counts'] = counts.T
        data.add_meta(name='counts', nix='NIX00272', packets=packets)
        data['counts_err'] = np.sqrt(counts_var).T * u.ct

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    def __repr__(self):
        return f'<{self.__class__.__name__}, {self.level} ' \
               f'{self.scet_timerange.start}to {self.scet_timerange.end} ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 30)


class Background(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'background'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets, NIX00405_offset=QLNIX00405_offset)
        control.add_data('energy_bin_edge_mask', _get_energy_bins(packets, 'NIX00266', 'NIXD0111'))
        control.add_basic(name='num_energies', nix='NIX00270', packets=packets)

        control['num_samples'] = np.array(packets.get_value('NIX00277')).flatten()[
            np.cumsum(control['num_energies']) - 1]
        control['num_samples'].meta = {'NIXS': 'NIX00277'}

        time, duration, scet_timerange = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00278'))

        counts = np.array(packets.get_value('NIX00278')).reshape(control['num_energies'][0],
                                                                 control['num_samples'].sum())
        counts_var = np.array(packets.get_value('NIX00278', attr="error")).\
            reshape(control['num_energies'][0], control['num_samples'].sum())

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00274'))

        triggers = packets.get_value('NIX00274').T
        triggers_var = packets.get_value('NIX00274', attr="error").T

        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data.add_meta(name='timedel', nix='NIX00405', packets=packets)
        data['triggers'] = triggers
        data.add_meta(name='triggers', nix='NIX00274', packets=packets)
        data['triggers_err'] = np.sqrt(triggers_var)
        data['counts'] = counts.T
        data.add_meta(name='counts', nix='NIX00278', packets=packets)
        data['counts_err'] = np.sqrt(counts_var).T * u.ct

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 31)


class Spectra(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'spectra'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets, NIX00405_offset=QLNIX00405_offset)
        control.add_data('pixel_mask', _get_pixel_mask(packets))
        control.add_data('compression_scheme_spectra_skm',
                         _get_compression_scheme(packets, 'NIX00452'))
        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00484'))

        # Fixed for spectra
        num_energies = np.unique(packets.get_value('NIX00100')).size
        control['num_energies'] = num_energies
        control.add_meta(name='num_energies', nix='NIX00100', packets=packets)
        control.add_basic(name='num_samples', nix='NIX00089', packets=packets)

        # TODO Handel NIX00089 value of zero ie valid packet with no data

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

        duration, time, scet_timerange = cls._get_time(control, num_energies, packets, pad_after)

        # sample x detector x energy
        # counts = np.array([eng_packets.get('NIX00{}'.format(i)) for i in range(452, 484)],
        #                   np.uint32).T * u.ct

        counts = []
        counts_var = []
        for i in range(452, 484):
            counts.append(packets.get_value('NIX00{}'.format(i)))
            counts_var.append(packets.get_value('NIX00{}'.format(i), attr='error'))
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
        data.add_meta(name='timedel', nix='NIX00405', packets=packets)
        data['detector_index'] = detector_index.reshape(-1, 32) * u.ct
        data.add_meta(name='detector_index', nix='NIX00100', packets=packets)
        data['spectra'] = counts.reshape(-1, 32, num_energies) * u.ct
        data['spectra'].meta = {'NIXS': [f'NIX00{i}' for i in range(452, 484)],
                                'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
                                              for i in range(452, 484)]}
        data['spectra_err'] = np.sqrt(counts_var.reshape(-1, 32, num_energies))
        data['triggers'] = triggers.reshape(-1, num_energies)
        data.add_meta(name='triggers', nix='NIX00484', packets=packets)
        data['triggers_err'] = np.sqrt(triggers_var.reshape(-1, num_energies))
        data['num_integrations'] = num_integrations.reshape(-1, num_energies)
        data.add_meta(name='num_integrations', nix='NIX00485', packets=packets)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def _get_time(cls, control, num_energies, packets, pad_after):
        times = []
        durations = []
        start = 0
        for i, (ns, it) in enumerate(control['num_samples', 'integration_time']):
            off_sets = packets.get_value('NIX00485')[start:start + ns] * it
            base_time = SCETime(control["scet_coarse"][i], control["scet_fine"][i])
            start_times = base_time + off_sets
            end_times = base_time + off_sets + it
            cur_time = start_times + (end_times - start_times) / 2
            times.extend(cur_time)
            durations.extend([it]*ns)
            start += ns

        time = np.array([(t.coarse, t.fine) for t in times])
        time = np.pad(time, ((0, pad_after), (0, 0)), mode='edge')
        time = SCETime(time[:, 0], time[:, 1]).reshape(-1, num_energies)
        duration = SCETimeDelta(np.pad(np.hstack(durations),
                                       (0, pad_after)).reshape(-1, num_energies))

        scet_timerange = SCETimeRange(start=time[0, 0]-duration[0, 0]/2,
                                      end=time[-1, -1]+duration[-1, 0]/2)

        return duration, time, scet_timerange

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.scet_timerange.start}, {self.scet_timerange.end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 32)


class Variance(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'variance'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets, NIX00405_offset=QLNIX00405_offset)

        # Control
        control['samples_per_variance'] = np.array(packets.get_value('NIX00279'), np.ubyte)
        control.add_meta(name='samples_per_variance', nix='NIX00279', packets=packets)
        control.add_data('pixel_mask', _get_pixel_mask(packets))
        control.add_data('detector_mask', _get_detector_mask(packets))

        control.add_data('compression_scheme_variance_skm',
                         _get_compression_scheme(packets, 'NIX00281'))

        energy_masks = np.array([
            [bool(int(x)) for x in format(packets.get_value('NIX00282')[i], '032b')]
            for i in range(len(packets.get_value('NIX00282')))])

        control['energy_bin_mask'] = energy_masks
        control.add_meta(name='energy_bin_mask', nix='NIX00282', packets=packets)
        control['num_energies'] = 1
        control.add_basic(name='num_samples', nix='NIX00280', packets=packets)

        time, duration, scet_timerange = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        variance = packets.get_value('NIX00281').T
        variance_var = packets.get_value('NIX00281', attr='error').T

        # Data
        data = Data()
        data['time'] = time
        data['timedel'] = duration
        data.add_meta(name='timedel', nix='NIX00405', packets=packets)
        data['control_index'] = control_indices
        data['variance'] = variance
        data.add_meta(name='variance', nix='NIX00281', packets=packets)
        data['variance_err'] = np.sqrt(variance_var)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 33)


class FlareFlag(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'flareflag'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets, NIX00405_offset=QLNIX00405_offset)

        control.add_basic(name='num_samples', nix='NIX00089', packets=packets)

        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        time, duration, scet_timerange = control._get_time()

        # DATA
        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data.add_meta(name='timedel', nix='NIX00405', packets=packets)

        data.add_basic(name='loc_z', nix='NIX00283', packets=packets, dtype=np.int16)
        data.add_basic(name='loc_y', nix='NIX00284', packets=packets, dtype=np.int16)
        data['thermal_index'] = packets.get_value('NIXD0061', attr='value').astype(np.int16).T
        data.add_meta(name='thermal_index', nix='NIXD0061', packets=packets)
        data['non_thermal_index'] = packets.get_value('NIXD0060', attr='value').astype(np.int16).T
        data.add_meta(name='non_thermal_index', nix='NIXD0060', packets=packets)
        data['location_status'] = packets.get_value('NIXD0059', attr='value').astype(np.int16).T
        data.add_meta(name='location_status', nix='NIXD0059', packets=packets)
        data['flare_progress'] = packets.get_value('NIXD0449', attr='value').astype(np.int16).T
        data.add_basic(name='flare_progress', nix='NIXD0449', packets=packets)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 34)


class EnergyCalibration(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'energy'
        self.level = 'L0'
        self.type = 'cal'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control.from_packets(packets)

        control['integration_time'] = packets.get_value('NIX00122')
        control.add_meta(name='integration_time', nix='NIX00122', packets=packets)
        # control['obs_beg'] = control['obs_utc']
        # control['.obs_end'] = control['obs_beg'] + timedelta(seconds=control[
        # 'duration'].astype('float'))
        # control['.obs_avg'] = control['obs_beg'] + (control['obs_end'] - control['obs_beg']) / 2

        # Control
        control.add_basic(name='quiet_time', nix='NIX00123', packets=packets)
        control.add_basic(name='live_time', nix='NIX00124', packets=packets)
        control.add_basic(name='average_temperature', nix='NIX00125', packets=packets,
                          dtype=np.uint16)
        control.add_data('detector_mask', _get_detector_mask(packets))
        control.add_data('pixel_mask', _get_pixel_mask(packets))

        control.add_data('subspectrum_mask', _get_sub_spectrum_mask(packets))
        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00158'))
        subspec_data = {}
        j = 129
        for subspec, i in enumerate(range(300, 308)):
            subspec_data[subspec+1] = {'num_points': packets.get_value(f'NIXD0{j}')[0],
                                       'num_summed_channel': packets.get_value(f'NIXD0{j + 1}')[0],
                                       'lowest_channel': packets.get_value(f'NIXD0{j + 2}')[0]}
            j += 3

        control.add_basic(name='num_samples', nix='NIX00159', packets=packets, dtype=np.uint16)
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

        time = SCETime(control['scet_coarse'], control['scet_fine']) \
            + control['integration_time'] / 2
        duration = SCETimeDelta(control['integration_time'])

        scet_timerange = SCETimeRange(start=SCETime(control['scet_coarse'][0],
                                                    control['scet_fine'][0]),
                                      end=SCETime(control['scet_coarse'][0],
                                                  control['scet_fine'][0])
                                      + control['integration_time'][0])

        # Data
        data = Data()
        data['timedel'] = duration[0:1]
        data['control_index'] = [0]
        data['time'] = time[0:1]

        data.add_meta(name='timedel', nix='NIX00122', packets=packets)
        # data['detector_id'] = np.array(packets.get('NIXD0155'), np.ubyte)
        # data['pixel_id'] = np.array(packets.get('NIXD0156'), np.ubyte)
        # data['subspec_id'] = np.array(packets.get('NIXD0157'), np.ubyte)
        # np.array(packets.get('NIX00146'))

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
        data.add_meta(name='counts', nix='NIX00158', packets=packets)
        data['counts_err'] = np.sqrt(full_counts_var).reshape((1, *full_counts_var.shape))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 41)


class TMStatusFlareList(QLProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'tmstatusflarelist'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control.add_basic(name='ubsd_counter', nix='NIX00285', packets=packets)
        control.add_basic(name='pald_counter', nix='NIX00286', packets=packets)
        control.add_basic(name='num_flares', nix='NIX00294', packets=packets)

        data = Data()
        if control['num_flares'].sum() > 0:
            data.add_basic(name='start_scet_coarse', nix='NIX00287', packets=packets)
            data.add_basic(name='end_scet_coarse', nix='NIX00288', packets=packets)

            data['time'] = SCETime(packets.get_value('NIX00287'), 0)

            data.add_basic(name='highest_flareflag', nix='NIX00289', packets=packets, dtype=np.byte)
            data.add_basic(name='tm_byte_volume', nix='NIX00290', packets=packets, dtype=np.byte)
            data.add_basic(name='average_z_loc', nix='NIX00291', packets=packets, dtype=np.byte)
            data.add_basic(name='average_y_loc', nix='NIX00292', packets=packets, dtype=np.byte)
            data.add_basic(name='processing_mask', nix='NIX00293', packets=packets, dtype=np.byte)

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 43)

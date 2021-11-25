"""
High level STIX data products created from single stand alone packets or a sequence of packets.
"""
import logging
from itertools import chain, repeat
from collections import defaultdict

import numpy as np

import astropy.units as u

from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_energy_bins,
    _get_pixel_mask,
    _get_sub_spectrum_mask,
    get_min_uint,
    rebin_proportional,
)
from stixcore.products.product import Control, Data, EnergyChannelsMixin, GenericProduct
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve', 'Background', 'Spectra']

logger = get_logger(__name__, level=logging.WARNING)

QLNIX00405_off = 0.1


class QLProduct(GenericProduct, EnergyChannelsMixin):
    """Generic QL product class composed of control and data."""
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        """Create a generic QL product composed of control and data.

        Parameters
        ----------
        service_type : `int`
            21
        service_subtype : `int`
            6
        ssid : `int`
            ssid of the data product
        control : `stixcore.products.product.Control`
            Table containing control information
        data : `stixcore.products.product.Data`
            Table containing data
        idb_versions : dict<SCETimeRange, VersionLabel>, optional
            a time range lookup what IDB versions are used within this data,
            by default defaultdict(SCETimeRange)
        """
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.type = 'ql'
        self.control = control
        self.data = data
        self.idb_versions = idb_versions

    @classmethod
    def from_levelb(cls, levelb, *, parent='', NIX00405_offset=0):
        """Converts level binary packets to a L1 product.

        Parameters
        ----------
        levelb : `stixcore.products.levelb.binary.LevelB`
            The binary level product.
        parent : `str`, optional
            The parent data file name the binary packed comes from, by default ''
        NIX00405_offset : int, optional
            [description], by default 0

        Returns
        -------
        tuple (packets, idb_versions, control)
            the converted packets
            all used IDB versions and time periods
            initialized control table
        """
        packets, idb_versions = GenericProduct.getLeveL0Packets(levelb)

        control = Control.from_packets(packets, NIX00405_offset=NIX00405_offset)

        # When the packets are parse empty packets are dropped but in LB we don't parse this
        # is not known need to compare control and levelb.control and only use matching rows
        if len(levelb.control) > len(control):
            matching_index = np.argwhere(
                np.in1d(levelb.control['scet_coarse'], np.array(packets.get('scet_coarse'))))
            control['raw_file'] = levelb.control['raw_file'][matching_index].reshape(-1)
            control['packet'] = levelb.control['packet'][matching_index].reshape(-1)
        else:
            control['raw_file'] = levelb.control['raw_file'].reshape(-1)
            control['packet'] = levelb.control['packet'].reshape(-1)

        control['parent'] = parent

        return packets, idb_versions, control


class LightCurve(QLProduct):
    """Quick Look Light Curve data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'lightcurve'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent,
                                                               NIX00405_offset=QLNIX00405_off)

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
        data['triggers'] = triggers.astype(get_min_uint(triggers))
        data.add_meta(name='triggers', nix='NIX00274', packets=packets)
        data['triggers_err'] = np.float32(np.sqrt(triggers_var))
        data['rcr'] = np.hstack(packets.get_value('NIX00276')).flatten().astype(np.ubyte)
        data.add_meta(name='rcr', nix='NIX00276', packets=packets)
        data['counts'] = counts.T.astype(get_min_uint(counts))
        data.add_meta(name='counts', nix='NIX00272', packets=packets)
        data['counts_err'] = np.float32(np.sqrt(counts_var).T * u.ct)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 30)


class Background(QLProduct):
    """Quick Look Background Light Curve data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'background'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent,
                                                               NIX00405_offset=QLNIX00405_off)

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
        data['triggers'] = triggers.astype(get_min_uint(triggers))
        data.add_meta(name='triggers', nix='NIX00274', packets=packets)
        data['triggers_err'] = np.float32(np.sqrt(triggers_var))
        data['counts'] = counts.T.astype(get_min_uint(counts))
        data.add_meta(name='counts', nix='NIX00278', packets=packets)
        data['counts_err'] = np.float32(np.sqrt(counts_var).T * u.ct)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 31)


class Spectra(QLProduct):
    """Quick Look Spectra data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'spectra'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent,
                                                               NIX00405_offset=QLNIX00405_off)

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
        data['detector_index'] = detector_index.reshape(-1, 32).astype(np.ubyte)
        data.add_meta(name='detector_index', nix='NIX00100', packets=packets)
        data['spectra'] = (counts.reshape(-1, 32, num_energies) * u.ct).astype(get_min_uint(counts))

        # data['spectra'].meta = {'NIXS': [f'NIX00{i}' for i in range(452, 484)],
        #                        'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
        #                                      for i in range(452, 484)]}
        data['spectra'].meta = {'NIXS': 'NIX00452',
                                'PCF_CURTX': packets.get('NIX00452')[0].idb_info.PCF_CURTX}
        data['spectra_err'] = np.float32(np.sqrt(counts_var.reshape(-1, 32, num_energies)))
        data['triggers'] = triggers.reshape(-1, num_energies).astype(get_min_uint(triggers))
        data.add_meta(name='triggers', nix='NIX00484', packets=packets)
        data['triggers_err'] = np.float32(np.sqrt(triggers_var.reshape(-1, num_energies)))
        data['num_integrations'] = num_integrations.reshape(-1, num_energies).astype(np.ubyte)
        data.add_meta(name='num_integrations', nix='NIX00485', packets=packets)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

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

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 32)


class Variance(QLProduct):
    """Quick Look Variance data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'variance'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent,
                                                               NIX00405_offset=QLNIX00405_off)

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
        data['variance'] = variance.astype(get_min_uint(variance))
        data.add_meta(name='variance', nix='NIX00281', packets=packets)
        data['variance_err'] = np.float32(np.sqrt(variance_var))

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 33)


class FlareFlag(QLProduct):
    """Quick Look Flare Flag and Location data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'flareflag'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent,
                                                               NIX00405_offset=QLNIX00405_off)

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
        try:
            data['flare_progress'] = packets.get_value('NIXD0449', attr='value').astype(np.int16).T
            data.add_basic(name='flare_progress', nix='NIXD0449', packets=packets)
        except AttributeError:
            logger.warn('Missing NIXD0449')

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 34)


class EnergyCalibration(QLProduct):
    """Quick Look energy calibration data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'energy'
        self.level = 'L0'
        self.type = 'cal'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent)

        # Control
        control.add_basic(name='integration_time', nix='NIX00122', packets=packets,
                          dtype=np.uint32, attr='value')
        control.add_basic(name='quiet_time', nix='NIX00123', packets=packets,
                          dtype=np.uint16, attr='value')
        control.add_basic(name='live_time', nix='NIX00124', packets=packets,
                          dtype=np.uint32, attr='value')
        control.add_basic(name='average_temperature', nix='NIX00125', packets=packets,
                          dtype=np.uint16, attr='value')
        control.add_data('detector_mask', _get_detector_mask(packets))
        control.add_data('pixel_mask', _get_pixel_mask(packets))

        control.add_data('subspectrum_mask', _get_sub_spectrum_mask(packets))
        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00158'))

        subspec_data = {}
        j = 129
        for subspec, i in enumerate(range(300, 308)):
            subspec_data[subspec+1] = {'num_points': packets.get_value(f'NIXD0{j}'),
                                       'num_summed_channel': packets.get_value(f'NIXD0{j + 1}'),
                                       'lowest_channel': packets.get_value(f'NIXD0{j + 2}')}
            j += 3

        control.add_basic(name='num_samples', nix='NIX00159', packets=packets, dtype=np.uint16)

        control['subspec_num_points'] = (
                np.vstack([v['num_points'] for v in subspec_data.values()]).T + 1).astype(np.uint16)
        control['subspec_num_summed_channel'] = (np.vstack(
            [v['num_summed_channel'] for v in subspec_data.values()]).T + 1).astype(np.uint16)
        control['subspec_lowest_channel'] = (
            np.vstack([v['lowest_channel'] for v in subspec_data.values()]).T).astype(np.uint16)

        channels = []
        for i, subspectrum_mask in enumerate(control['subspectrum_mask']):
            subspec_index = np.argwhere(subspectrum_mask == 1)
            sub_channels = [np.arange(control['subspec_num_points'][i, index])
                            * (control['subspec_num_summed_channel'][i, index])
                            + control['subspec_lowest_channel'][i, index] for index in
                            subspec_index]
            channels.append(list(chain(*[ch.tolist() for ch in sub_channels])))
        control['num_channels'] = [len(c) for c in channels]

        duration = SCETimeDelta(packets.get_value('NIX00122').astype(np.uint32))
        time = SCETime(control['scet_coarse'], control['scet_fine']) + duration / 2

        dids = packets.get_value('NIXD0155')
        pids = packets.get_value('NIXD0156')
        ssids = packets.get_value('NIXD0157')
        num_spec_points = packets.get_value('NIX00146')

        unique_times, unique_time_indices = np.unique(time.as_float(), return_index=True)
        unique_times_lookup = {k: v for k, v in zip(unique_times, np.arange(unique_times.size))}

        # should really do the other way make a smaller lookup rather than repeating many many times
        tids = np.hstack([[unique_times_lookup[t.as_float()]] * n
                          for t, n in zip(time, control['num_samples'])])
        c_in = list(chain.from_iterable([repeat(c, n)
                                         for c, n in zip(channels, control['num_samples'])]))

        counts = packets.get_value('NIX00158')
        counts_var = packets.get_value('NIX00158', attr='error')

        c_out = np.arange(1025)
        start = 0
        count_map = defaultdict(list)
        counts_var_map = defaultdict(list)
        for tid, did, pid, ssid, nps, cin in zip(tids, dids, pids, ssids, num_spec_points, c_in):
            end = start + nps

            logger.debug('%d, %d, %d, %d, %d, %d', tid, did, pid, ssid, nps, end)
            count_map[tid, did, pid].append(counts[start:end])
            counts_var_map[tid, did, pid].append(counts_var[start:end])
            start = end

        full_counts = np.zeros((unique_times.size, 32, 12, 1024))
        full_counts_var = np.zeros((unique_times.size, 32, 12, 1024))

        for tid, did, pid in count_map.keys():
            cur_counts = count_map[tid, did, pid]
            cur_counts_var = counts_var_map[tid, did, pid]

            counts_rebinned = rebin_proportional(np.hstack(cur_counts), cin, c_out)
            counts_var_rebinned = rebin_proportional(np.hstack(cur_counts_var), cin, c_out)

            full_counts[tid, did, pid] = counts_rebinned
            full_counts_var[tid, did, pid] = counts_var_rebinned

        control = control[unique_time_indices]
        control['index'] = np.arange(len(control))

        # Data
        data = Data()
        data['time'] = time[unique_time_indices]
        data['timedel'] = duration[unique_time_indices]
        data.add_meta(name='timedel', nix='NIX00122', packets=packets)

        data['counts'] = full_counts.astype(get_min_uint(full_counts))
        data.add_meta(name='counts', nix='NIX00158', packets=packets)
        data['counts_err'] = np.sqrt(full_counts_var).astype(np.float32)
        data['control_index'] = np.arange(len(control)).astype(np.uint16)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 41)


class TMStatusFlareList(QLProduct):
    """Quick Look TM Management status and Flare list data product.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'tmstatusflarelist'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = QLProduct.from_levelb(levelb, parent=parent)

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

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 43)

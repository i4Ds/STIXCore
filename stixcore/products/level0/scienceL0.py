from pathlib import Path
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.table.operations import unique

from stixcore.config.reader import read_energy_channels
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_pixel_mask,
    _get_unique,
    get_min_uint,
    rebin_proportional,
)
from stixcore.products.product import ControlSci, Data, EnergyChannelsMixin, GenericProduct
from stixcore.time import SCETime, SCETimeRange
from stixcore.time.datetime import SCETimeDelta
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")


__all__ = ['ScienceProduct', 'RawPixelData', 'CompressedPixelData', 'SummedPixelData',
           'Visibility', 'Spectrogram', 'Aspect']

SUM_DMASK_SCET_COARSE_RANGE = range(659318490, 668863556)


def fix_detector_mask(control, detector_mask):
    """
    Update the detector mask in BSD data due to misconfiguration of SumDmask

    For a time the BKG and CFL event were not recorded so even if requested will be missing whihc
    could effect normalisation in terms of detector area etc. See issue for more information
    https://github.com/i4Ds/STIXCore/issues/115

    Parameters
    ----------
    control :  `products.product.ControlSci`
        Control data
    data : `products.product.Data`
        Data

    Returns
    -------
    Update detector mask
    """
    if control['time_stamp'].coarse not in SUM_DMASK_SCET_COARSE_RANGE:
        return detector_mask
    else:
        logger.info('Fixing detector mask for SumDmask misconfiguration')
        detector_mask[:, 8:10] = 0
        return detector_mask


class ScienceProduct(GenericProduct, EnergyChannelsMixin):
    """Generic science data product class composed of control and data."""
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        """Create a generic science data product composed of control and data.

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
        self.control = control
        self.data = data
        self.idb_versions = kwargs.get('idb_versions', None)

        self.type = 'sci'
        self.level = 'L0'

    @property
    def raw(self):
        return np.unique(self.control['raw_file'])

    @property
    def parent(self):
        return np.unique(self.control['parent'])

    def __add__(self, other):
        # if (np.all(self.control == other.control) and self.scet_timerange == other.scet_timerange
        #         and len(self.data) == len(other.data)):
        #     return self
        # combined_control_index = other.control['index'] + self.control['index'].max() + 1
        # control = vstack((self.control, other.control))
        # cnames = control.colnames
        # cnames.remove('index')
        # control = unique(control, cnames)
        #
        # combined_data_index = other.data['control_index'] + self.control['index'].max() + 1
        # data = vstack((self.data, other.data))
        #
        # data_ind = np.isin(combined_data_index, combined_control_index)
        # data = data[data_ind]
        #
        # return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
        #                   ssid=self.ssid, data=data, control=control)
        raise(ValueError(f"Tried to combine 2 BSD products: {self} and {other}"))

    def split_to_files(self):
        """Splits the entire data into data products separated be the unique request ID.

        Yields
        -------
        `ScienceProduct`
            the next ScienceProduct defined by the unique request ID
        """

        key_cols = ['request_id']
        if 'tc_packet_seq_control' in self.control.colnames:
            key_cols.insert(0, 'tc_packet_seq_control')

        for ci in unique(self.control, keys=key_cols)['index']:
            control = self.control[self.control['index'] == ci]
            data = self.data[self.data['control_index'] == ci]
            # for req_id in self.control['request_id']:
            #     ctrl_inds = np.where(self.control['request_id'] == req_id)
            #     control = self.control[ctrl_inds]
            #     data_index = control['index'][0]
            #     data_inds = np.where(self.data['control_index'] == data_index)
            #     data = self.data[data_inds]

            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data)

    @classmethod
    def from_levelb(cls, levelb, *, parent=''):
        """Converts level binary science packets to a L1 product.

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

        control = ControlSci.from_packets(packets)
        # control.remove_column('num_structures')

        control['index'] = np.ubyte(0)
        control['packet'] = levelb.control['packet'].reshape(1, -1)
        control['packet'].dtype = get_min_uint(control['packet'])
        control['raw_file'] = np.unique(levelb.control['raw_file']).reshape(1, -1)
        control['parent'] = parent

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        return packets, idb_versions, control


class RawPixelData(ScienceProduct):
    """Raw X-ray pixel counts: compression level 0. No aggregation.

    In level 0 format.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-rpd'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        data = Data()
        data['start_time'] = packets.get_value('NIX00404').astype(np.uint32)
        data.add_meta(name='start_time', nix='NIX00404', packets=packets)
        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets, dtype=np.ubyte)
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = packets.get_value('NIX00405').astype(np.uint16)
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)
        data.add_data('pixel_masks', _get_pixel_mask(packets, 'NIXD0407'))
        data.add_data('detector_masks', fix_detector_mask(control, _get_detector_mask(packets)))
        data['triggers'] = np.array([packets.get_value(f'NIX00{i}') for i in range(408, 424)]).T
        data['triggers'].dtype = get_min_uint(data['triggers'])
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(408, 424)]}
        data.add_basic(name='num_samples', nix='NIX00406', packets=packets, dtype=np.uint16)

        num_detectors = 32
        num_energies = 32
        num_pixels = 12

        # Data
        tmp = dict()
        tmp['pixel_id'] = np.array(packets.get_value('NIXD0158'), np.ubyte)
        tmp['detector_id'] = np.array(packets.get_value('NIXD0153'), np.ubyte)
        tmp['channel'] = np.array(packets.get_value('NIXD0154'), np.ubyte)
        tmp['continuation_bits'] = np.array(packets.get_value('NIXD0159'), np.ubyte)

        control['energy_bin_mask'] = np.full((1, 32), False, np.ubyte)
        all_energies = set(tmp['channel'])
        control['energy_bin_mask'][:, list(all_energies)] = True

        # Find contiguous time indices
        unique_times = np.unique(data['start_time'])
        time_indices = np.searchsorted(unique_times, data['start_time'])

        counts_1d = packets.get_value('NIX00065')

        end_inds = np.cumsum(data['num_samples'])
        start_inds = np.hstack([0, end_inds[:-1]])
        dd = [(tmp['pixel_id'][s:e], tmp['detector_id'][s:e], tmp['channel'][s:e], counts_1d[s:e])
              for s, e in zip(start_inds.astype(int), end_inds)]

        counts = np.zeros((len(unique_times), num_detectors, num_pixels, num_energies), np.uint32)
        for i, (pid, did, cid, cc) in enumerate(dd):
            counts[time_indices[i], did, pid, cid] = cc

        sub_index = np.searchsorted(data['start_time'], unique_times)
        data = data[sub_index]
        data['time'] = control["time_stamp"][0] \
            + data['start_time'] + data['integration_time'] / 2
        data['timedel'] = SCETimeDelta(data['integration_time'])
        data['counts'] = (counts * u.ct).astype(get_min_uint(counts))
        # data.add_meta(name='counts', nix='NIX00065', packets=packets)
        data['control_index'] = control['index'][0]

        data.remove_columns(['start_time', 'integration_time', 'num_samples'])

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 20)


class CompressedPixelData(ScienceProduct):
    """Aggregated (over time and/or energies) X-ray pixel counts: compression level 1.

    In level 0 format.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-cpd'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00260'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00242'))

        data = Data()
        try:
            data['delta_time'] = np.uint32(packets.get_value('NIX00441').to(u.ds))
            data.add_meta(name='delta_time', nix='NIX00441', packets=packets)
        except AttributeError:
            data['delta_time'] = np.uint32(packets.get_value('NIX00404').to(u.ds))
            data.add_meta(name='delta_time', nix='NIX00404', packets=packets)
        unique_times = np.unique(data['delta_time'])

        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets, dtype=np.ubyte)
        data['num_pixel_sets'] = np.atleast_1d(_get_unique(packets, 'NIX00442', np.ubyte))
        data.add_meta(name='num_pixel_sets', nix='NIX00442', packets=packets)
        pixel_masks, pm_meta = _get_pixel_mask(packets, 'NIXD0407')
        pixel_masks = pixel_masks.reshape(-1, data['num_pixel_sets'][0], 12)
        if packets.ssid == 21 and data['num_pixel_sets'][0] != 12:
            pixel_masks = np.pad(pixel_masks, ((0, 0), (0, 12 - data['num_pixel_sets'][0]), (0, 0)))
        data.add_data('pixel_masks', (pixel_masks, pm_meta))
        data.add_data('detector_masks', fix_detector_mask(control, _get_detector_mask(packets)))
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = SCETimeDelta(packets.get_value('NIX00405'))
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = np.array([packets.get_value(f'NIX00{i}') for i in range(242, 258)])
        triggers_var = np.array([packets.get_value(f'NIX00{i}', attr='error')
                                 for i in range(242, 258)])

        data['triggers'] = triggers.T.astype(get_min_uint(triggers))
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)]}
        data['triggers_err'] = np.float32(np.sqrt(triggers_var).T)
        data.add_basic(name='num_energy_groups', nix='NIX00258', packets=packets, dtype=np.ubyte)

        tmp = dict()
        tmp['e_low'] = np.array(packets.get_value('NIXD0016'), np.ubyte)
        tmp['e_high'] = np.array(packets.get_value('NIXD0017'), np.ubyte)
        tmp['num_data_elements'] = np.array(packets.get_value('NIX00259'))
        unique_energies_low = np.unique(tmp['e_low'])
        unique_energies_high = np.unique(tmp['e_high'])

        counts = np.array(packets.get_value('NIX00260'))
        counts_var = np.array(packets.get_value('NIX00260', attr='error'))

        counts = counts.reshape(unique_times.size, unique_energies_low.size,
                                data['detector_masks'][0].sum(), data['num_pixel_sets'][0].sum())

        counts_var = counts_var.reshape(unique_times.size, unique_energies_low.size,
                                        data['detector_masks'][0].sum(),
                                        data['num_pixel_sets'][0].sum())
        # t x e x d x p -> t x d x p x e
        counts = counts.transpose((0, 2, 3, 1))

        out_counts = None
        out_var = None

        counts_var = np.sqrt(counts_var.transpose((0, 2, 3, 1)))
        if packets.ssid == 21:
            out_counts = np.zeros((unique_times.size, 32, 12, 32))
            out_var = np.zeros((unique_times.size, 32, 12, 32))
        elif packets.ssid == 22:
            out_counts = np.zeros((unique_times.size, 32, 12, 32))
            out_var = np.zeros((unique_times.size, 32, 12, 32))

        dl_energies = np.array([[ENERGY_CHANNELS[lch].e_lower, ENERGY_CHANNELS[hch].e_upper]
                                for lch, hch in
                                zip(unique_energies_low, unique_energies_high)]).reshape(-1)
        dl_energies = np.unique(dl_energies)
        sci_energies = np.hstack([[ENERGY_CHANNELS[ch].e_lower for ch in range(32)],
                                  ENERGY_CHANNELS[31].e_upper])

        # If there is any onboard summing of energy channels rebin back to standard sci channels
        if (unique_energies_high - unique_energies_low).sum() > 0:
            rebinned_counts = np.zeros((*counts.shape[:-1], 32))
            rebinned_counts_var = np.zeros((*counts_var.shape[:-1], 32))
            e_ch_start = 0
            e_ch_end = counts.shape[-1]
            if dl_energies[0] == 0.0:
                rebinned_counts[..., 0] = counts[..., 0]
                rebinned_counts_var[..., 0] = counts_var[..., 0]
                e_ch_start += 1
            elif dl_energies[-1] == np.inf:
                rebinned_counts[..., -1] = counts[..., -1]
                rebinned_counts_var[..., -1] = counts_var[..., -1]
                e_ch_end -= 1

            torebin = np.where((dl_energies >= 4.0) & (dl_energies <= 150.0))
            rebinned_counts[..., 1:-1] = np.apply_along_axis(
                rebin_proportional, -1,
                counts[..., e_ch_start:e_ch_end].reshape(-1, e_ch_end - e_ch_start),
                dl_energies[torebin],  sci_energies[1:-1]).reshape((*counts.shape[:-1], 30))

            rebinned_counts_var[..., 1:-1] = np.apply_along_axis(
                rebin_proportional, -1,
                counts_var[..., e_ch_start:e_ch_end].reshape(-1, e_ch_end - e_ch_start),
                dl_energies[torebin], sci_energies[1:-1]).reshape((*counts_var.shape[:-1], 30))

            energy_indices = np.full(32, True)
            energy_indices[[0, -1]] = False

            ix = np.ix_(np.full(unique_times.size, True), data['detector_masks'][0].astype(bool),
                        np.ones(data['num_pixel_sets'][0], dtype=bool), np.full(32, True))

            out_counts[ix] = rebinned_counts
            out_var[ix] = rebinned_counts_var
        else:
            energy_indices = np.full(32, False)
            energy_indices[unique_energies_low.min():unique_energies_high.max() + 1] = True

            ix = np.ix_(np.full(unique_times.size, True),
                        data['detector_masks'][0].astype(bool),
                        np.ones(data['num_pixel_sets'][0], dtype=bool),
                        energy_indices)

            out_counts[ix] = counts
            out_var[ix] = counts_var

        #     if (high - low).sum() > 0:
        #         raise NotImplementedError()
        #         #full_counts = rebin_proportional(dl_energies, cur_counts, sci_energies)
        #
        #     dids2 = data[inds[0][0]]['detector_masks']
        #     cids2 = np.full(32, False)
        #     cids2[low] = True
        #     tids2 = time == unique_times
        #
        #     if ssid == 21:
        #         out_counts[np.ix_(tids2, cids2, dids2, pids)] = cur_counts
        #     elif ssid == 22:
        #         out_counts[np.ix_(tids2, cids2, dids2)] = cur_counts

        if counts.sum() != out_counts.sum():
            raise ValueError('Original and reformatted count totals do not match')

        control['energy_bin_mask'] = np.full((1, 32), False, np.ubyte)
        all_energies = set(np.hstack([tmp['e_low'], tmp['e_high']]))
        control['energy_bin_mask'][:, list(all_energies)] = True
        # time x energy x detector x pixel
        # counts = np.array(
        #     eng_packets['NIX00260'], np.uint16).reshape(unique_times.size, num_energies,
        #                                                 num_detectors, num_pixels)
        # time x channel x detector x pixel need to transpose to time x detector x pixel x channel

        sub_index = np.searchsorted(data['delta_time'], unique_times)
        data = data[sub_index]

        data['time'] = (control['time_stamp'][0]
                        + data['delta_time'] + data['integration_time']/2)
        data['timedel'] = data['integration_time']
        data['counts'] = \
            (out_counts * u.ct).astype(get_min_uint(out_counts))[..., :tmp['e_high'].max()+1]
        data.add_meta(name='counts', nix='NIX00260', packets=packets)
        data['counts_err'] = np.float32(out_var * u.ct)[..., :tmp['e_high'].max()+1]
        data['control_index'] = control['index'][0]

        data = data['time', 'timedel', 'rcr', 'pixel_masks', 'detector_masks', 'num_pixel_sets',
                    'num_energy_groups', 'triggers', 'triggers_err', 'counts', 'counts_err']
        data['control_index'] = np.ubyte(0)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 21)


class SummedPixelData(CompressedPixelData):
    """Aggregated (over time and/or energies and pixelsets) X-ray pixel counts: compression level 2.

    In level 0 format.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'xray-spd'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 22)


class Visibility(ScienceProduct):
    """
    X-ray Visibilities or compression Level 3 data

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-vis'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00263'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00242'))

        data = Data()
        try:
            data['delta_time'] = packets.get_value('NIX00441')
            data.add_meta(name='delta_time', nix='NIX00441', packets=packets)
        except AttributeError:
            data['delta_time'] = packets.get_value('NIX00404')
            data.add_meta(name='delta_time', nix='NIX00404', packets=packets)
        data['control_index'] = np.full(len(data['delta_time']), 0)
        unique_times = np.unique(data['delta_time'])

        # time = np.array([])
        # for dt in set(self.delta_time):
        #     i, = np.where(self.delta_time == dt)
        #     nt = sum(np.array(packets['NIX00258'])[i])
        #     time = np.append(time, np.repeat(dt, nt))
        # self.time = time

        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets)

        data.add_data('pixel_mask1', _get_pixel_mask(packets, 'NIXD0407'))
        data.add_data('pixel_mask2', _get_pixel_mask(packets, 'NIXD0444'))
        data.add_data('pixel_mask3', _get_pixel_mask(packets, 'NIXD0445'))
        data.add_data('pixel_mask4', _get_pixel_mask(packets, 'NIXD0446'))
        data.add_data('pixel_mask5', _get_pixel_mask(packets, 'NIXD0447'))
        data.add_data('detector_masks', fix_detector_mask(control, _get_detector_mask(packets)))
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = packets.get_value('NIX00405')
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = []
        triggers_var = []
        for i in range(242, 258):
            triggers.extend(packets.get_value(f'NIX00{i}'))
            triggers_var.extend(packets.get_value(f'NIX00{i}', attr='error'))

        data['triggers'] = np.array(triggers).reshape(-1, 16)
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)]}  # ,
        #                         'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
        #                                       for i in range(242, 258)]}
        data['triggers_err'] = np.sqrt(triggers_var).reshape(-1, 16)

        tids = np.searchsorted(data['delta_time'], unique_times)
        data = data[tids]

        # sum(packets.get_value('NIX00258'))

        # Data
        e_low = np.array(packets.get_value('NIXD0016'))
        e_high = np.array(packets.get_value('NIXD0017'))

        # TODO create energy bin mask
        control['energy_bin_mask'] = np.full((1, 32), False, np.ubyte)
        all_energies = set(np.hstack([e_low, e_high]))
        control['energy_bin_mask'][:, list(all_energies)] = True

        data['flux'] = np.array(packets.get_value('NIX00261')).reshape(unique_times.size, -1)
        data.add_meta(name='flux', nix='NIX00261', packets=packets)
        num_detectors = packets.get_value('NIX00262')[0]
        data['detector_id'] = np.array(packets.get_value('NIX00100')).reshape(unique_times.size, -1,
                                                                              num_detectors)

        data['real'] = packets.get_value('NIX00263').reshape(unique_times.size, num_detectors, -1)
        data.add_meta(name='real', nix='NIX00263', packets=packets)
        data['real_err'] = np.sqrt(
            packets.get_value('NIX00263', attr='error').reshape(unique_times.size,
                                                                num_detectors, -1))
        data.add_meta(name='real_err', nix='NIX00263', packets=packets)
        data['imaginary'] = packets.get_value('NIX00264').reshape(unique_times.size,
                                                                  num_detectors, -1)
        data.add_meta(name='imaginary', nix='NIX00264', packets=packets)
        data['imaginary_err'] = np.sqrt(
            packets.get_value('NIX00264', attr='error').reshape(unique_times.size,
                                                                num_detectors, -1))
        data.add_meta(name='imaginary', nix='NIX00264', packets=packets)

        data['time'] = (control["time_stamp"][0]
                        + data['delta_time'] + data['integration_time'] / 2)
        data['timedel'] = SCETimeDelta(data['integration_time'])

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct):
    """
    X-ray Spectrogram or compression Level 2 data

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00268'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00267'))

        control['pixel_mask'] = np.unique(_get_pixel_mask(packets)[0], axis=0)
        control.add_meta(name='pixel_mask', nix='NIXD0407', packets=packets)
        control['detector_mask'] = np.unique(
            fix_detector_mask(control, _get_detector_mask(packets)[0]), axis=0)
        control.add_meta(name='detector_mask', nix='NIX00407', packets=packets)
        raw_rcr = packets.get_value('NIX00401', attr='value')

        e_min = np.array(packets.get_value('NIXD0442'))
        e_max = np.array(packets.get_value('NIXD0443'))
        energy_unit = np.array(packets.get_value('NIXD0019')) + 1
        num_times = np.array(packets.get_value('NIX00089'))
        total_num_times = num_times.sum()

        rcr = np.hstack([np.full(nt, rcr) for rcr, nt in zip(raw_rcr, num_times)]).astype(np.ubyte)

        counts = np.array(packets.get_value('NIX00268'))
        counts_var = np.array(packets.get_value('NIX00268', attr='error'))

        counts = counts.reshape(total_num_times, -1)
        counts_var = counts_var.reshape(total_num_times, -1)

        full_counts = np.zeros((total_num_times, 32))
        full_counts_var = np.zeros((total_num_times, 32))

        cids = [np.arange(emin, emax + 1, eunit) for (emin, emax, eunit)
                in zip(e_min, e_max, energy_unit)]

        control['energy_bin_mask'] = np.full((1, 32), False, np.ubyte)
        control['energy_bin_mask'][:, cids] = True

        dl_energies = np.array([[ENERGY_CHANNELS[ch].e_lower for ch in chs]
                                + [ENERGY_CHANNELS[chs[-1]].e_upper] for chs in cids][0])

        sci_energies = np.hstack([[ENERGY_CHANNELS[ch].e_lower for ch in range(32)],
                                  ENERGY_CHANNELS[31].e_upper])
        ind = 0
        for nt in num_times:
            e_ch_start = 0
            e_ch_end = counts.shape[1]
            if dl_energies[0] == 0:
                full_counts[ind:ind + nt, 0] = counts[ind:ind + nt, 0]
                full_counts_var[ind:ind + nt, 0] = counts_var[ind:ind + nt, 0]
                e_ch_start = 1
            if dl_energies[-1] == np.inf:
                full_counts[ind:ind + nt, -1] = counts[ind:ind + nt, -1]
                full_counts_var[ind:ind + nt, -1] = counts[ind:ind + nt, -1]
                e_ch_end -= 1

            torebin = np.where((dl_energies >= 4.0) & (dl_energies <= 150.0))
            full_counts[ind:ind + nt, 1:-1] = np.apply_along_axis(
                rebin_proportional, 1, counts[ind:ind + nt, e_ch_start:e_ch_end],
                dl_energies[torebin], sci_energies[1:-1])

            full_counts_var[ind:ind + nt, 1:-1] = np.apply_along_axis(
                rebin_proportional, 1, counts_var[ind:ind + nt, e_ch_start:e_ch_end],
                dl_energies[torebin], sci_energies[1:-1])

            ind += nt

        if counts.sum() != full_counts.sum():
            raise ValueError('Original and reformatted count totals do not match')

        try:
            delta_time = packets.get_value('NIX00441')
        except AttributeError:
            delta_time = packets.get_value('NIX00404')

        closing_time_offset = packets.get_value('NIX00269')

        # TODO incorporate into main loop above
        centers = []
        deltas = []
        last = 0
        for i, nt in enumerate(num_times):
            edge = np.hstack(
                [delta_time[last:last + nt], delta_time[last + nt - 1] + closing_time_offset[i]])
            delta = np.diff(edge)
            center = edge[:-1] + delta / 2
            centers.append(center)
            deltas.append(delta)
            last = last + nt

        centers = np.hstack(centers)
        deltas = np.hstack(deltas)
        deltas = SCETimeDelta(deltas)

        # Data
        data = Data()
        data['time'] = control['time_stamp'][0] + centers
        data['timedel'] = deltas
        data['timedel'].meta = {'NIXS': ['NIX00441', 'NIX00269']}
        data.add_basic(name='triggers', nix='NIX00267', packets=packets)
        data['rcr'] = rcr
        data.add_meta(name='rcr', nix='NIX00401', packets=packets)
        data['triggers'].dtype = get_min_uint(data['triggers'])
        data.add_basic(name='triggers_err', nix='NIX00267', attr='error', packets=packets)
        data['triggers_err'] = np.float32(data['triggers_err'])
        data['counts'] = (full_counts * u.ct).astype(get_min_uint(full_counts))[..., :e_max.max()+1]
        data.add_meta(name='counts', nix='NIX00268', packets=packets)
        data['counts_err'] = np.float32(np.sqrt(full_counts_var) * u.ct)[..., :e_max.max()+1]
        data['control_index'] = np.ubyte(0)

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 24)


class Aspect(ScienceProduct):
    """Bulk Aspect data.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'aspect-burst'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions = GenericProduct.getLeveL0Packets(levelb)
        if len(packets.data) == 0:
            logger.warning('No data all packets empty %s', levelb)
            return None
        control = ControlSci()

        scet_coarse = packets.get_value('NIX00445')
        scet_fine = packets.get_value('NIX00446')

        control.add_basic(name='summing_value', nix='NIX00088', packets=packets, dtype=np.uint8)
        control.add_basic(name='samples', nix='NIX00089', packets=packets, dtype=np.uint16)
        try:
            control.add_basic(name='averaging_value', nix='NIX00490',
                              packets=packets, dtype=np.uint16)
        except AttributeError:
            control['averaging_value'] = np.uint16(1)

        try:
            control.add_basic(name='request_id', nix='NIX00037', packets=packets,
                              dtype=np.uint32)
        except AttributeError:
            control['request_id'] = np.uint32(0)

        control['raw_file'] = np.unique(levelb.control['raw_file']).reshape(1, -1)
        control['packet'] = levelb.control['packet'].reshape(1, -1)
        control['parent'] = parent

        control['index'] = np.arange(len(control)).astype(get_min_uint(len(control)))

        delta_time = ((control['summing_value'] * control['averaging_value']) / 1000.0) * u.s
        samples = packets.get_value('NIX00089')

        offsets = SCETimeDelta(np.concatenate(
            [delta_time[i] * np.arange(0, ns) for i, ns in enumerate(samples)]))
        timedel = SCETimeDelta(
            np.concatenate([delta_time[i] * np.ones(ns) for i, ns in enumerate(samples)]))
        ctimes = np.concatenate([np.full(ns, scet_coarse[i]) for i, ns in enumerate(samples)])
        ftimes = np.concatenate([np.full(ns, scet_fine[i]) for i, ns in enumerate(samples)])
        starts = SCETime(ctimes, ftimes)
        time = starts + offsets

        # Data
        try:
            data = Data()
            data['time'] = time
            data['timedel'] = timedel
            data.add_basic(name='cha_diode0', nix='NIX00090', packets=packets, dtype=np.uint16)
            data.add_basic(name='cha_diode1', nix='NIX00091', packets=packets, dtype=np.uint16)
            data.add_basic(name='chb_diode0', nix='NIX00092', packets=packets, dtype=np.uint16)
            data.add_basic(name='chb_diode1', nix='NIX00093', packets=packets, dtype=np.uint16)
            data['control_index'] = np.hstack([np.full(ns, i) for i, ns in enumerate(samples)])
        except ValueError as e:
            logger.warning(e)
            raise e

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 42)

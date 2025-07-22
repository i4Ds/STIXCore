from pathlib import Path
from collections import defaultdict

import numpy as np

import astropy.units as u

from stixcore.config.reader import read_energy_channels
from stixcore.io.RidLutManager import RidLutManager
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_pixel_mask,
    get_min_uint,
    unscale_triggers,
)
from stixcore.products.product import (
    ControlSci,
    CountDataMixin,
    Data,
    EnergyChannelsMixin,
    FitsHeaderMixin,
    GenericProduct,
)
from stixcore.time import SCETime, SCETimeRange
from stixcore.time.datetime import SCETimeDelta
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")


__all__ = ['ScienceProduct', 'RawPixelData', 'CompressedPixelData', 'SummedPixelData',
           'Visibility', 'Spectrogram', 'Aspect']

SUM_DMASK_SCET_COARSE_RANGE = (659318490, 668863556)
PIXEL_MASK_LOOKUP = np.zeros(2049)
PIXEL_MASK_LOOKUP[[int(2**i) for i in range(12)]] = np.arange(0, 12, dtype=int)


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
    if not np.any((control['time_stamp'].coarse >= SUM_DMASK_SCET_COARSE_RANGE[0])
                  & (control['time_stamp'].coarse <= SUM_DMASK_SCET_COARSE_RANGE[1])):
        return detector_mask
    else:
        logger.info('Fixing detector mask for SumDmask misconfiguration')
        detector_mask[:, 8:10] = 0
        return detector_mask


class NotCombineException(Exception):
    pass


class ScienceProduct(CountDataMixin, GenericProduct, EnergyChannelsMixin, FitsHeaderMixin):
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
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, **kwargs)
        self.idb_versions = kwargs.get('idb_versions', None)
        self.level = 'L0'
        self.type = 'sci'

    @property
    def fits_daily_file(self):
        return False

    @property
    def raw(self):
        return np.unique(self.control['raw_file'])

    @property
    def parent(self):
        return np.unique(self.control['parent'])

    def __add__(self, other):
        raise NotCombineException(f"Tried to combine 2 BSD products: \n{self} and \n{other}")

        # if (np.all(self.control == other.control) and self.scet_timerange == other.scet_timerange
        #         and len(self.data) == len(other.data)):
        #     return self
        # combined_control_index = other.control['index'] + self.control['index'].max() + 1
        # control = vstack((self.control, other.control))
        # cnames = control.colnames
        # cnames.remove('index')
        # control = unique(control, cnames)

        # combined_data_index = other.data['control_index'] + self.control['index'].max() + 1
        # data = vstack((self.data, other.data))
        # data_ind = np.isin(combined_data_index, combined_control_index)
        # data = data[data_ind]

        # return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
        #                   ssid=self.ssid, data=data, control=control)

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

        for control in self.control.group_by(key_cols).groups:
            data = self.data[np.in1d(self.data['control_index'], control['index'])]

            file_chunk = type(self)(service_type=self.service_type,
                                    service_subtype=self.service_subtype,
                                    ssid=self.ssid, control=control, data=data,
                                    idb_versions=self.idb_versions, comment=self.comment,
                                    history=self.history)
            if hasattr(self, 'get_additional_extensions'):
                for ext, name in self.get_additional_extensions():
                    # Copy all extension data tables to the new product
                    if ext is not None:
                        setattr(file_chunk, name, getattr(self, name)[:])
            yield file_chunk

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
        packet_ids = levelb.control['packet'].reshape(1, -1)
        control['packet'] = packet_ids.astype(get_min_uint(packet_ids))
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
        data.add_data('detector_masks', _get_detector_mask(packets))
        triggers = np.array([packets.get_value(f'NIX00{i}') for i in range(408, 424)]).T
        data['triggers'] = triggers.astype(get_min_uint(triggers))
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

        emask = np.full(33, False, np.ubyte)
        all_energies = set(tmp['channel'])
        eids = np.array(list(all_energies))
        emask[eids] = 1
        emask[eids+1] = 1
        control['energy_bin_edge_mask'] = emask.reshape(1, -1)

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

        # SumDMask config issue
        data['detector_masks'] = fix_detector_mask(control, data['detector_masks'])
        # now slice counts by updated mask if necessary
        orig_sum = counts.sum()
        if counts.shape[1] != data['detector_masks'][0].sum():
            non_zero_detectors, *_ = np.where(counts.sum(axis=(0, 2, 3)) > 0)
            counts = counts[:, non_zero_detectors, ...]
            new_sum = counts.sum()
            if new_sum != orig_sum:
                raise ValueError('Subscribed counts sum does not match original sum')

        # Slice counts to only include requested energy channels
        counts = counts[..., eids]

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
                   idb_versions=idb_versions,
                   packets=packets)

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
        header_comments = []
        header_history = []
        additional_header_keywords = []

        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        c_skm, c_skm_meta = _get_compression_scheme(packets, 'NIX00260')
        control.add_data('compression_scheme_counts_skm', (c_skm[0].reshape(1, 3), c_skm_meta))

        t_skm, t_skm_meta = _get_compression_scheme(packets, 'NIX00242')
        control.add_data('compression_scheme_triggers_skm', (t_skm[0].reshape(1, 3), t_skm_meta))

        if np.unique(t_skm, axis=0).shape[0] != 1:
            additional_header_keywords.append(('DATAWARN', 1, 'See comments'))
            header_comments.append('Multiple compression schemes detected, '
                                   'trigger values maybe incorrect')

        data = Data()
        try:
            data['delta_time'] = np.uint32(packets.get_value('NIX00441').to(u.ds))
            data.add_meta(name='delta_time', nix='NIX00441', packets=packets)
        except AttributeError:
            data['delta_time'] = np.uint32(packets.get_value('NIX00404').to(u.ds))
            data.add_meta(name='delta_time', nix='NIX00404', packets=packets)
        unique_times = np.unique(data['delta_time'])

        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets, dtype=np.ubyte)
        data.add_basic(name='num_pixel_sets', nix='NIX00442', attr='value', packets=packets,
                       dtype=np.ubyte)

        pixel_mask_ints = packets.get_value('NIXD0407')
        if cls is CompressedPixelData:
            pixel_indices = PIXEL_MASK_LOOKUP[pixel_mask_ints]
            start = 0
            end = 0
            res = []
            for npx in data['num_pixel_sets']:
                end += npx
                cur_pm = pixel_indices[start:end].astype(int).tolist()
                full_pm = np.full((12), 0, dtype=np.ubyte)
                full_pm[cur_pm] = 1
                res.append(full_pm)
                start = end
            pixel_masks = np.array(res, dtype=np.uint8)
        elif cls is SummedPixelData:
            pixel_masks = np.array([list(map(int, format(pm, '012b')))[::-1]
                                    for pm in pixel_mask_ints])
            pixel_masks = pixel_masks.astype(np.uint8).reshape(-1, data['num_pixel_sets'][0], 12)
        param = packets.get('NIXD0407')[0]
        pixel_meta = {'NIXS': 'NIXD0407', 'PCF_CURTX': param.idb_info.PCF_CURTX}
        data.add_data('pixel_masks', (pixel_masks, pixel_meta))
        data.add_data('detector_masks', _get_detector_mask(packets))
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = SCETimeDelta(packets.get_value('NIX00405'))
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = np.array([packets.get_value(f'NIX00{i}') for i in range(242, 258)])
        triggers_var = np.array([packets.get_value(f'NIX00{i}', attr='error')
                                 for i in range(242, 258)])

        if control['compression_scheme_triggers_skm'].tolist() == [[0, 0, 7]]:
            factor = RidLutManager.instance.get_scaling_factor(control['request_id'][0])
            header_history.append(f"trigger descaled with {factor}")
            logger.debug(f'Unscaling trigger: {factor}')
            additional_header_keywords.append(("TRIG_SCA", factor, 'used trigger descale factor'))
            triggers, triggers_var = unscale_triggers(
                triggers, integration=data['integration_time'],
                detector_masks=data['detector_masks'], ssid=levelb.ssid, factor=factor)

        data['triggers'] = triggers.T.astype(get_min_uint(triggers))
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)]}
        data['triggers_comp_err'] = np.float32(np.sqrt(triggers_var).T)
        # data.add_basic(name='num_energy_groups', nix='NIX00258', packets=packets, dtype=np.ubyte)

        tmp = dict()
        tmp['e_low'] = np.array(packets.get_value('NIXD0016'), np.ubyte)
        tmp['e_high'] = np.array(packets.get_value('NIXD0017'), np.ubyte)
        tmp['num_data_elements'] = np.array(packets.get_value('NIX00259'))
        unique_energies_low = np.unique(tmp['e_low'])
        unique_energies_high = np.unique(tmp['e_high'])

        counts_flat = np.array(packets.get_value('NIX00260'))
        counts_var_flat = np.array(packets.get_value('NIX00260', attr='error'))

        if cls is CompressedPixelData:
            n_detectors = data['detector_masks'][0].sum()
            start = 0
            end = 0
            counts = []
            counts_var = []
            pixel_mask_index = -1
            for i, nc in enumerate(tmp['num_data_elements']):
                if i % unique_energies_low.size == 0:
                    pixel_mask_index += 1
                end += nc
                cur_counts = counts_flat[start:end].reshape(n_detectors, -1)
                cur_counts_var = counts_var_flat[start:end].reshape(n_detectors, -1)
                if cur_counts.shape[1] != 12:
                    full_counts = np.zeros((n_detectors, 12))
                    full_counts_var = np.zeros((n_detectors, 12))
                    pix_m = data['pixel_masks'][pixel_mask_index].astype(bool)
                    # Sometimes the change in pixel mask is reflected in the mask before the actual
                    # count data so try the correct pixel mask but if this fails user most recent
                    # matching value
                    try:
                        full_counts[:, pix_m] = cur_counts
                        full_counts_var[:, pix_m] = cur_counts_var
                    except ValueError:
                        last_match_index = np.where(data['pixel_masks'].sum(axis=1)
                                                    == cur_counts.shape[1])
                        pix_m = data['pixel_masks'][last_match_index[0][-1]].astype(bool)
                        full_counts[:, pix_m] = cur_counts
                        full_counts_var[:, pix_m] = cur_counts_var

                    counts.append(full_counts)
                    counts_var.append(full_counts_var)
                else:
                    counts.append(cur_counts)
                    counts_var.append(cur_counts_var)
                start = end

            counts = np.array(counts).reshape(unique_times.size, unique_energies_low.size,
                                              data['detector_masks'].sum(axis=1).max(),
                                              12)
            counts_var = np.array(counts_var).reshape(unique_times.size, unique_energies_low.size,
                                                      data['detector_masks'].sum(axis=1).max(),
                                                      12)
        elif cls is SummedPixelData:
            counts = counts_flat.reshape(unique_times.size, unique_energies_low.size,
                                         data['detector_masks'].sum(axis=1).max(),
                                         data['num_pixel_sets'][0].sum())

            counts_var = counts_var_flat.reshape(unique_times.size, unique_energies_low.size,
                                                 data['detector_masks'].sum(axis=1).max(),
                                                 data['num_pixel_sets'][0].sum())
        # t x e x d x p -> t x d x p x e
        counts = counts.transpose((0, 2, 3, 1))
        counts_var = np.sqrt(counts_var.transpose((0, 2, 3, 1)))

        # No longer re-binning back to science energy channels leaving here for incase
        # dl_energies = np.array([[ENERGY_CHANNELS[lch].e_lower, ENERGY_CHANNELS[hch].e_upper]
        #                         for lch, hch in
        #                         zip(unique_energies_low, unique_energies_high)]).reshape(-1)
        # dl_energies = np.unique(dl_energies)
        # sci_energies = np.hstack([[ENERGY_CHANNELS[ch].e_lower for ch in range(32)],
        #                           ENERGY_CHANNELS[31].e_upper])
        #
        # # If there is any onboard summing of energy channels rebin back to standard sci channels
        # if (unique_energies_high - unique_energies_low).sum() > 0:
        #     r
        #     rebinned_counts = np.zeros((*counts.shape[:-1], 32))
        #     rebinned_counts_var = np.zeros((*counts_var.shape[:-1], 32))
        #     e_ch_start = 0
        #     e_ch_end = counts.shape[-1]
        #     if dl_energies[0] == 0.0:
        #         rebinned_counts[..., 0] = counts[..., 0]
        #         rebinned_counts_var[..., 0] = counts_var[..., 0]
        #         e_ch_start += 1
        #     elif dl_energies[-1] == np.inf:
        #         rebinned_counts[..., -1] = counts[..., -1]
        #         rebinned_counts_var[..., -1] = counts_var[..., -1]
        #         e_ch_end -= 1
        #
        #     torebin = np.where((dl_energies >= 4.0) & (dl_energies <= 150.0))
        #     rebinned_counts[..., 1:-1] = np.apply_along_axis(
        #         rebin_proportional, -1,
        #         counts[..., e_ch_start:e_ch_end].reshape(-1, e_ch_end - e_ch_start),
        #         dl_energies[torebin],  sci_energies[1:-1]).reshape((*counts.shape[:-1], 30))
        #
        #     rebinned_counts_var[..., 1:-1] = np.apply_along_axis(
        #         rebin_proportional, -1,
        #         counts_var[..., e_ch_start:e_ch_end].reshape(-1, e_ch_end - e_ch_start),
        #         dl_energies[torebin], sci_energies[1:-1]).reshape((*counts_var.shape[:-1], 30))
        #
        #     energy_indices = np.full(32, True)
        #     energy_indices[[0, -1]] = False
        #
        #     e_min_idx = unique_energies_low.min()
        #     e_max_idx = unique_energies_high.max()
        #
        #     if counts.sum() != rebinned_counts[...,e_min_idx:e_max_idx+1].sum():
        #         raise ValueError('Original and reformatted count totals do not match')
        #
        #     counts = rebinned_counts[...,e_min_idx:e_max_idx+1]
        #     counts_var = rebinned_counts_var[...,e_min_idx:e_max_idx+1]

        # check pixel mask and subscript down to match max
        if levelb.ssid == 21:
            pids, *_ = np.where(data['pixel_masks'].sum(axis=0) > 0)
            counts = counts[..., pids, :]
            counts_var = counts_var[..., pids, :]

        control['energy_bin_edge_mask'] = np.full((1, 33), False, np.ubyte)
        edges = set(np.hstack([unique_energies_low, unique_energies_high+1]))
        control['energy_bin_edge_mask'][:, list(edges)] = 1

        # only fix here as data is needed for extraction but will be all zeros
        data['detector_masks'] = fix_detector_mask(control, data['detector_masks'])
        # now slice counts by updated mask if necessary
        orig_sum = counts.sum()
        if counts.shape[1] != data['detector_masks'][0].sum():
            non_zero_detectors, *_ = np.where(counts.sum(axis=(0, 2, 3)) > 0)
            counts = counts[:, non_zero_detectors, ...]
            counts_var = counts_var[:, non_zero_detectors, ...]
            new_sum = counts.sum()
            if new_sum != orig_sum:
                raise ValueError('Subscribed counts sum does not match original sum')

        sub_index = np.searchsorted(data['delta_time'], unique_times)
        data = data[sub_index]

        data['time'] = (control['time_stamp'][0]
                        + data['delta_time'] + data['integration_time']/2)
        data['timedel'] = data['integration_time']
        data['counts'] = \
            (counts * u.ct).astype(
                get_min_uint(counts))
        data.add_meta(name='counts', nix='NIX00260', packets=packets)
        data['counts_comp_err'] = np.float32(
            counts_var * u.ct)
        data['control_index'] = control['index'][0]

        data = data['time', 'timedel', 'rcr', 'pixel_masks', 'detector_masks', 'num_pixel_sets',
                    'triggers', 'triggers_comp_err',
                    'counts', 'counts_comp_err']
        data['control_index'] = np.ubyte(0)

        prod = cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions,
                   packets=packets,
                   history=header_history,
                   comment=header_comments)

        prod.add_additional_header_keywords(additional_header_keywords)
        return prod

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
        self.name = 'xray-scpd'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 22)


class Visibility(ScienceProduct):
    """
    X-ray Visibilities or compression Level 3 data.

    In level 0 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-vis'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        c_skm, c_skm_meta = _get_compression_scheme(packets, 'NIX00263')
        control.add_data('compression_scheme_counts_skm', (c_skm[0].reshape(1, 3), c_skm_meta))

        t_skm, t_skm_meta = _get_compression_scheme(packets, 'NIX00242')
        control.add_data('compression_scheme_triggers_skm', (t_skm[0].reshape(1, 3), t_skm_meta))

        data = Data()
        header_history = []
        additional_header_keywords = []

        try:
            data['delta_time'] = packets.get_value('NIX00441')
            data.add_meta(name='delta_time', nix='NIX00441', packets=packets)
        except AttributeError:
            data['delta_time'] = packets.get_value('NIX00404')
            data.add_meta(name='delta_time', nix='NIX00404', packets=packets)
        data['control_index'] = np.full(len(data['delta_time']), 0)
        unique_times = np.unique(data['delta_time'])

        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets)

        data.add_data('pixel_mask1', _get_pixel_mask(packets, 'NIXD0407'))
        data.add_data('pixel_mask2', _get_pixel_mask(packets, 'NIXD0444'))
        data.add_data('pixel_mask3', _get_pixel_mask(packets, 'NIXD0445'))
        data.add_data('pixel_mask4', _get_pixel_mask(packets, 'NIXD0446'))
        data.add_data('pixel_mask5', _get_pixel_mask(packets, 'NIXD0447'))
        data.add_data('detector_masks', _get_detector_mask(packets))
        data['detector_masks'] = fix_detector_mask(control, data['detector_masks'])
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = packets.get_value('NIX00405')
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = []
        triggers_var = []
        for i in range(242, 258):
            triggers.extend(packets.get_value(f'NIX00{i}'))
            triggers_var.extend(packets.get_value(f'NIX00{i}', attr='error'))
        triggers = np.array(triggers).reshape(-1, 16).T

        if control['compression_scheme_triggers_skm'].tolist() == [[0, 0, 7]]:
            factor = RidLutManager.instance.get_scaling_factor(control['request_id'][0])
            header_history.append(f"trigger descaled with {factor}")
            logger.debug(f'Unscaling trigger: {factor}')
            additional_header_keywords.append(("TRIG_SCA", factor, 'used trigger descale factor'))
            triggers, triggers_var = unscale_triggers(
                triggers, integration=SCETimeDelta(data['integration_time']),
                detector_masks=data['detector_masks'], ssid=levelb.ssid, factor=factor)

        data['triggers'] = triggers.T
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)]}  # ,
        #                         'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
        #                                       for i in range(242, 258)]}
        data['triggers_comp_err'] = np.sqrt(triggers_var).reshape(-1, 16)

        tids = np.searchsorted(data['delta_time'], unique_times)
        data = data[tids]

        # Data
        e_low = np.array(packets.get_value('NIXD0016'))
        e_high = np.array(packets.get_value('NIXD0017'))

        control['energy_bin_edge_mask'] = np.full((1, 33), False, np.ubyte)
        edges = set(np.hstack([e_low, e_high+1]))
        control['energy_bin_edge_mask'][:, list(edges)] = 1

        data['flux'] = np.array(packets.get_value('NIX00261')).reshape(unique_times.size, -1)
        data.add_meta(name='flux', nix='NIX00261', packets=packets)
        num_detectors = packets.get_value('NIX00262')[0]
        data['detector_id'] = np.array(packets.get_value('NIX00100')).reshape(unique_times.size, -1,
                                                                              num_detectors)

        data['real'] = packets.get_value('NIX00263').reshape(unique_times.size, num_detectors, -1)
        data.add_meta(name='real', nix='NIX00263', packets=packets)
        data['real_comp_err'] = np.sqrt(
            packets.get_value('NIX00263', attr='error').reshape(unique_times.size,
                                                                num_detectors, -1))
        data.add_meta(name='real_comp_err', nix='NIX00263', packets=packets)
        data['imaginary'] = packets.get_value('NIX00264').reshape(unique_times.size,
                                                                  num_detectors, -1)
        data.add_meta(name='imaginary', nix='NIX00264', packets=packets)
        data['imaginary_comp_err'] = np.sqrt(
            packets.get_value('NIX00264', attr='error').reshape(unique_times.size,
                                                                num_detectors, -1))
        data.add_meta(name='imaginary', nix='NIX00264', packets=packets)

        data['time'] = (control["time_stamp"][0]
                        + data['delta_time'] + data['integration_time'] / 2)
        data['timedel'] = SCETimeDelta(data['integration_time'])

        prod = cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions,
                   packets=packets,
                   history=header_history)

        prod.add_additional_header_keywords(additional_header_keywords)
        return prod

    @property
    def dmin(self):
        # TODO define columns for dmin/max
        return 0.0

    @property
    def dmax(self):
        # TODO define columns for dmin/max
        return 0.0

    @property
    def bunit(self):
        # TODO define columns for dmin/max
        return ' '

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct):
    """
    X-ray Spectrogram or compression Level 2 data

    In level 0 format.
    """
    PRODUCT_PROCESSING_VERSION = 4

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'

    @classmethod
    def from_levelb(cls, levelb, parent=''):
        header_comments = []
        header_history = []
        additional_header_keywords = []

        packets, idb_versions, control = ScienceProduct.from_levelb(levelb, parent=parent)

        c_skm, c_skm_meta = _get_compression_scheme(packets, 'NIX00268')
        control.add_data('compression_scheme_counts_skm', (c_skm[0].reshape(1, 3), c_skm_meta))

        t_skm, t_skm_meta = _get_compression_scheme(packets, 'NIX00267')
        control.add_data('compression_scheme_triggers_skm', (t_skm[0].reshape(1, 3), t_skm_meta))

        if np.unique(t_skm, axis=0).shape[0] != 1:
            additional_header_keywords.append(('DATAWARN', 1, 'See comments'))
            header_comments.append('Multiple compression schemes detected, '
                                   'trigger values maybe incorrect.')

        control['detector_masks'] = np.unique(_get_detector_mask(packets)[0], axis=0)
        control['detector_masks'] = fix_detector_mask(control, control['detector_masks'])
        control.add_meta(name='detector_masks', nix='NIX00407', packets=packets)
        raw_rcr = packets.get_value('NIX00401', attr='value')

        e_min = packets.get_value('NIXD0442')
        e_max = packets.get_value('NIXD0443') + 1
        energy_unit = packets.get_value('NIXD0019') + 1
        num_times = packets.get_value('NIX00089')
        total_num_times = num_times.sum()

        rcr = np.hstack([np.full(nt, rcr) for rcr, nt in zip(raw_rcr, num_times)]).astype(np.ubyte)

        counts = np.array(packets.get_value('NIX00268'))
        counts_var = np.array(packets.get_value('NIX00268', attr='error'))

        counts = counts.reshape(total_num_times, -1)
        counts_var = counts_var.reshape(total_num_times, -1)

        # orig_counts_sum = counts.sum()

        cids = [np.arange(emin, emax + 1, eunit) for (emin, emax, eunit)
                in zip(e_min, e_max, energy_unit)]

        control['energy_bin_edge_mask'] = np.full((1, 33), 0, np.ubyte)
        control['energy_bin_edge_mask'][:, cids] = 1

        try:
            delta_time = packets.get_value('NIX00441')
        except AttributeError:
            delta_time = packets.get_value('NIX00404')

        closing_time_offset = packets.get_value('NIX00269')

        time_edges = np.hstack([delta_time, delta_time[-1]+closing_time_offset[-1]])
        deltas = np.diff(time_edges)
        centers = time_edges[:-1] + 0.5 * deltas
        deltas = SCETimeDelta(deltas)

        pixel_masks_orig = _get_pixel_mask(packets)
        pixel_masks_expanded = [np.repeat(pm.reshape(-1, 1), n, 1) for pm, n in
                                zip(pixel_masks_orig[0], num_times)]
        pixel_masks = np.hstack(pixel_masks_expanded).T

        triggers = packets.get_value('NIX00267')
        triggers_var = packets.get_value('NIX00267', attr='error')

        if control['compression_scheme_triggers_skm'].tolist() == [[0, 0, 7]]:
            factor = RidLutManager.instance.get_scaling_factor(control['request_id'][0])
            header_history.append(f"trigger descaled with {factor}")
            additional_header_keywords.append(("TRIG_SCA", factor, 'used trigger descale factor'))
            logger.debug(f'Unscaling trigger: {factor}')
            triggers, triggers_var = unscale_triggers(
                triggers, integration=deltas,
                detector_masks=control['detector_masks'], ssid=levelb.ssid, factor=factor)

        # Data
        data = Data()
        data['time'] = control['time_stamp'][0] + centers
        data['timedel'] = deltas
        data['timedel'].meta = {'NIXS': ['NIX00441', 'NIX00269']}

        data['triggers'] = triggers.astype(get_min_uint(triggers))
        data.add_meta(name='triggers', nix='NIX00267', packets=packets)
        data['triggers_comp_err'] = np.float32(np.sqrt(triggers_var))
        data.add_meta(name='triggers_comp_err', nix='NIX00267', packets=packets)

        data['rcr'] = rcr
        data.add_meta(name='rcr', nix='NIX00401', packets=packets)
        data['pixel_masks'] = pixel_masks
        data.add_meta(name='pixel_masks', nix='NIXD0407', packets=packets)

        data['counts'] = (counts * u.ct).astype(get_min_uint(counts))
        data.add_meta(name='counts', nix='NIX00268', packets=packets)
        data['counts_comp_err'] = np.float32(np.sqrt(counts_var) * u.ct)
        data['control_index'] = np.ubyte(0)

        prod = cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions,
                   packets=packets,
                   history=header_history,
                   comment=header_comments)

        prod.add_additional_header_keywords(additional_header_keywords)
        return prod

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
                   idb_versions=idb_versions,
                   packets=packets)

    @property
    def dmin(self):
        return np.nanmin([self.data['cha_diode0'].min(),
                          self.data['cha_diode1'].min(),
                          self.data['chb_diode0'].min(),
                          self.data['chb_diode1'].min()])

    @property
    def dmax(self):
        return np.nanmax([self.data['cha_diode0'].max(),
                          self.data['cha_diode1'].max(),
                          self.data['chb_diode0'].max(),
                          self.data['chb_diode1'].max()])

    @property
    def bunit(self):
        return ' '

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 42)

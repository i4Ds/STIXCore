from pathlib import Path
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.table.operations import unique, vstack

from stixcore.config.reader import read_energy_channels
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_energies_from_mask,
    _get_pixel_mask,
    _get_unique,
    rebin_proportional,
)
from stixcore.products.product import BaseProduct, ControlSci, Data
from stixcore.time import SCETime, SCETimeRange
from stixcore.time.datetime import SCETimeDelta
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")


__all__ = ['ScienceProduct', 'RawPixelData', 'CompressedPixelData', 'SummedPixelData',
           'Visibility', 'Spectrogram', 'Aspect']


class ScienceProduct(BaseProduct):
    """

    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.type = 'sci'
        self.control = control
        self.data = data
        self.idb_versions = kwargs.get('idb_versions', None)
        self.scet_timerange = kwargs['scet_timerange']

    def __add__(self, other):
        combined_control_index = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other.control))
        cnames = control.colnames
        cnames.remove('index')
        control = unique(control, cnames)

        combined_data_index = other.data['control_index'] + self.control['index'].max() + 1
        data = vstack((self.data, other.data))

        data_ind = np.isin(combined_data_index, combined_control_index)
        data = data[data_ind]

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, data=data, control=control)

    def __repr__(self):
        return f'<{self.__class__.__name__}\n' \
               f' {self.control.__repr__()}\n' \
               f' {self.data.__repr__()}\n' \
               f'>'

    # @staticmethod
    # def get_energies():
    #     return get_energies_from_mask()
    #
    # @classmethod
    # def from_fits(cls, fitspath):
    #     header = fits.getheader(fitspath)
    #     control = QTable.read(fitspath, hdu='CONTROL')
    #     data = QTable.read(fitspath, hdu='DATA')
    #     obs_beg = Time(header['DATE_OBS'])
    #     data['time'] = (data['time'] + obs_beg)
    #     return cls(control=control, data=data)

    def to_requests(self):
        for ci in unique(self.control, keys=['tc_packet_seq_control', 'request_id'])['index']:
            control = self.control[self.control['index'] == ci]
            data = self.data[self.data['control_index'] == ci]
            # for req_id in self.control['request_id']:
            #     ctrl_inds = np.where(self.control['request_id'] == req_id)
            #     control = self.control[ctrl_inds]
            #     data_index = control['index'][0]
            #     data_inds = np.where(self.data['control_index'] == data_index)
            #     data = self.data[data_inds]

            scet_timerange = SCETimeRange(start=data['time'][0] - data['timedel'][0]/2,
                                          end=data['time'][-1] + data['timedel'][-1]/2)

            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data,
                             scet_timerange=scet_timerange)

    def get_energies(self):
        if 'energy_bin_edge_mask' in self.control.colnames:
            energies = _get_energies_from_mask(self.control['energy_bin_edge_mask'][0])
        elif 'energy_bin_mask' in self.control.colnames:
            energies = _get_energies_from_mask(self.control['energy_bin_mask'][0])
        else:
            energies = _get_energies_from_mask()

        return energies


class RawPixelData(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-rpd'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        # control.remove_column('num_structures')
        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['index'] = 0

        data = Data()
        data['start_time'] = packets.get_value('NIX00404')
        data.add_meta(name='start_time', nix='NIX00404', packets=packets)
        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets, dtype=np.ubyte)
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = packets.get_value('NIX00405')
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)
        data.add_data('pixel_masks', _get_pixel_mask(packets, 'NIXD0407'))
        data.add_data('detector_masks', _get_detector_mask(packets))
        data['triggers'] = np.array([packets.get_value(f'NIX00{i}') for i in range(408, 424)],
                                    np.int64).T
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(408, 424)]}  # ,
        #                       #  'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
        #                       #                for i in range(408, 424)]}
        data.add_basic(name='num_samples', nix='NIX00406', packets=packets, dtype=np.int16)

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

        # Create final count array with 4 dimensions: unique times, 32 det, 32 energies, 12 pixels

        # for i in range(self.num_samples):
        #     tid = np.argwhere(self.raw_counts == unique_times)

        # start_index = 0
        # for i, time_index in enumerate(time_indices):
        #     end_index = np.uint32(start_index + np.sum(data['num_samples'][time_index]))
        #
        #     for did, cid, pid in zip(tmp['detector_id'], tmp['channel'], tmp['pixel_id']):
        #         index_1d = ((tmp['detector_id'] == did) & (tmp['channel'] == cid)
        #                     & (tmp['pixel_id'] == pid))
        #         cur_count = counts_1d[start_index:end_index][index_1d[start_index:end_index]]
        #         # If we have a count assign it other wise do nothing as 0
        #         if cur_count:
        #             counts[i, did, cid, pid] = cur_count[0]
        #
        #     start_index = end_index

        scet_timerange = SCETimeRange(start=control["time_stamp"][0] + data['start_time'][0],
                                      end=control["time_stamp"][-1] + data['start_time'][-1]
                                      + data['integration_time'][-1])

        sub_index = np.searchsorted(data['start_time'], unique_times)
        data = data[sub_index]
        data['time'] = control["time_stamp"][0] \
            + data['start_time'] + data['integration_time'] / 2
        data['timedel'] = SCETimeDelta(data['integration_time'])
        data['counts'] = counts * u.ct
        # data.add_meta(name='counts', nix='NIX00065', packets=packets)
        data['control_index'] = control['index'][0]

        data.remove_columns(['start_time', 'integration_time', 'num_samples'])

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 20)


class CompressedPixelData(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-cpd'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00260'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00242'))

        # control.remove_column('num_structures')
        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['index'] = 0

        data = Data()
        try:
            data['delta_time'] = packets.get_value('NIX00441')
            data.add_meta(name='delta_time', nix='NIX00441', packets=packets)
        except AttributeError:
            data['delta_time'] = packets.get_value('NIX00404')
            data.add_meta(name='delta_time', nix='NIX00404', packets=packets)
        unique_times = np.unique(data['delta_time'])

        data.add_basic(name='rcr', nix='NIX00401', attr='value', packets=packets, dtype=np.ubyte)
        data['num_pixel_sets'] = np.atleast_1d(_get_unique(packets, 'NIX00442', np.byte))
        data.add_meta(name='num_pixel_sets', nix='NIX00442', packets=packets)
        pixel_masks, pm_meta = _get_pixel_mask(packets, 'NIXD0407')
        pixel_masks = pixel_masks.reshape(-1, data['num_pixel_sets'][0], 12)
        if ssid == 21 and data['num_pixel_sets'][0] != 12:
            pixel_masks = np.pad(pixel_masks, ((0, 0), (0, 12 - data['num_pixel_sets'][0]), (0, 0)))
        data.add_data('pixel_masks', (pixel_masks, pm_meta))
        data.add_data('detector_masks', _get_detector_mask(packets))
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = SCETimeDelta(packets.get_value('NIX00405'))
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = np.array([packets.get_value(f'NIX00{i}') for i in range(242, 258)])
        triggers_var = np.array([packets.get_value(f'NIX00{i}', attr='error')
                                 for i in range(242, 258)])

        data['triggers'] = triggers.T
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)]}
        data['triggers_err'] = np.sqrt(triggers_var).T
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
        if ssid == 21:
            out_counts = np.zeros((unique_times.size, 32, data['num_pixel_sets'][0], 32))
            out_var = np.zeros((unique_times.size, 32, data['num_pixel_sets'][0], 32))
        elif ssid == 22:
            out_counts = np.zeros((unique_times.size, 32, data['num_pixel_sets'][0], 32))
            out_var = np.zeros((unique_times.size, 32, data['num_pixel_sets'][0], 32))

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

        scet_timerange = SCETimeRange(start=control['time_stamp'][0] + data['delta_time'][0],
                                      end=control['time_stamp'][-1] + data['delta_time'][-1]
                                      + data['integration_time'][-1])

        data['time'] = (control['time_stamp'][0]
                        + data['delta_time'] + data['integration_time']/2)
        data['timedel'] = data['integration_time']
        data['counts'] = out_counts * u.ct
        data.add_meta(name='counts', nix='NIX00260', packets=packets)
        data['counts_err'] = out_var * u.ct
        data['control_index'] = control['index'][0]

        data = data['time', 'timedel', 'rcr', 'pixel_masks', 'detector_masks', 'num_pixel_sets',
                    'num_energy_groups', 'triggers', 'triggers_err', 'counts', 'counts_err']
        data['control_index'] = 0

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 21)


class SummedPixelData(CompressedPixelData):
    """
    X-ray Summed Pixels or compression Level 2 data
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'xray-spd'
        self.level = 'L0'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 22)


class Visibility(ScienceProduct):
    """
    X-ray Visibilities or compression Level 3 data
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-vis'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00263'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00242'))

        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['index'] = range(len(control))

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
        data.add_data('detector_masks', _get_detector_mask(packets))
        # NIX00405 in BSD is 1 indexed
        data['integration_time'] = packets.get_value('NIX00405')
        data.add_meta(name='integration_time', nix='NIX00405', packets=packets)

        triggers = []
        triggers_var = []
        for i in range(242, 258):
            triggers.extend(packets.get_value(f'NIX00{i}'))
            triggers_var.extend(packets.get_value(f'NIX00{i}', attr='error'))

        data['triggers'] = np.array(triggers).reshape(-1, 16)
        data['triggers'].meta = {'NIXS': [f'NIX00{i}' for i in range(242, 258)],
                                 'PCF_CURTX': [packets.get(f'NIX00{i}')[0].idb_info.PCF_CURTX
                                               for i in range(242, 258)]}
        data['triggers_err'] = np.sqrt(triggers_var).reshape(-1, 16)

        tids = np.searchsorted(data['delta_time'], unique_times)
        data = data[tids]

        sum(packets.get_value('NIX00258'))

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

        scet_timerange = SCETimeRange(start=control["time_stamp"][0] + data['delta_time'][0],
                                      end=control["time_stamp"][-1] + data['delta_time'][-1]
                                      + data['integration_time'][-1])

        data['time'] = (control["time_stamp"][0]
                        + data['delta_time'] + data['integration_time'] / 2)
        data['timedel'] = SCETimeDelta(data['integration_time'])

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct):
    """
    X-ray Spectrogram or compression Level 2 data
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control.add_data('compression_scheme_counts_skm',
                         _get_compression_scheme(packets, 'NIX00268'))

        control.add_data('compression_scheme_triggers_skm',
                         _get_compression_scheme(packets, 'NIX00267'))

        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['pixel_mask'] = np.unique(_get_pixel_mask(packets)[0], axis=0)
        control.add_meta(name='pixel_mask', nix='NIXD0407', packets=packets)
        control['detector_mask'] = np.unique(_get_detector_mask(packets)[0], axis=0)
        control.add_meta(name='detector_mask', nix='NIX00407', packets=packets)
        control['rcr'] = np.unique(packets.get_value('NIX00401', attr='value'))[0]
        control.add_meta(name='rcr', nix='NIX00401', packets=packets)
        control['index'] = range(len(control))

        e_min = np.array(packets.get_value('NIXD0442'))
        e_max = np.array(packets.get_value('NIXD0443'))
        energy_unit = np.array(packets.get_value('NIXD0019')) + 1
        num_times = np.array(packets.get_value('NIX00089'))
        total_num_times = num_times.sum()

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

        scet_timerange = SCETimeRange(start=control['time_stamp'][0] + centers[0] - deltas[0]/2,
                                      end=control['time_stamp'][0] + centers[-1] + deltas[-1]/2)

        # Data
        data = Data()
        data['time'] = control['time_stamp'][0] + centers
        data['timedel'] = deltas
        data['timedel'].meta = {'NIXS': ['NIX00441', 'NIX00269']}
        data.add_basic(name='triggers', nix='NIX00267', packets=packets)
        data.add_basic(name='triggers', nix='NIX00267', attr='error', packets=packets)
        data['counts'] = full_counts * u.ct
        data.add_meta(name='counts', nix='NIX00268', packets=packets)
        data['counts_err'] = np.sqrt(full_counts_var) * u.ct
        data['control_index'] = 0

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 24)


class Aspect(ScienceProduct):
    """
    Aspect
    """
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'aspect-burst'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci()
        scet_coarse = packets.get_value('NIX00445')
        scet_fine = packets.get_value('NIX00446')
        SCETime(scet_coarse, scet_fine)

        # TODO add case for older IDB
        control.add_basic(name='summing_value', nix='NIX00088', packets=packets)
        control.add_basic(name='averaging_value', nix='NIX00490', packets=packets)
        control.add_basic(name='samples', nix='NIX00089', packets=packets)
        control['index'] = range(len(control))

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

        scet_timerange = SCETimeRange(start=time[0] - timedel[0]/2,
                                      end=time[-1] + timedel[-1]/2)

        # Data
        try:
            data = Data()
            data['time'] = time
            data['timedel'] = timedel
            data.add_basic(name='cha_diode0', nix='NIX00090', packets=packets)
            data.add_basic(name='cha_diode1', nix='NIX00091', packets=packets)
            data.add_basic(name='chb_diode0', nix='NIX00092', packets=packets)
            data.add_basic(name='chb_diode1', nix='NIX00093', packets=packets)
            data['control_index'] = np.hstack([np.full(ns, i) for i, ns in enumerate(samples)])
        except ValueError as e:
            logger.warning(e)
            raise e

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    def to_days(self):
        start_day = int((self.scet_timerange.start.as_float() / u.d).decompose().value)
        end_day = int((self.scet_timerange.end.as_float() / u.d).decompose().value)
        if start_day == end_day:
            end_day += 1
        days = range(start_day, end_day)
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

        # min_day = np.floor((self.data['time'] / (1 * u.day)).min())
        # max_day = np.ceil((self.data['time'] / (1 * u.day)).max())
        #
        # days = np.arange(min_day, max_day) * u.day
        # date_ranges = [(day, day+1*u.day) for day in days]
        # for dstart, dend in date_ranges:
        #     i = np.where((self.data['time'] >= dstart) &
        #                  (self.data['time'] < dend))
        #
        #     # Implement slice on parent or add mixin
        #     if i[0].size > 0:
        #         data = self.data[i]
        #         control_indices = np.unique(data['control_index'])
        #         control = self.control[np.isin(self.control['index'], control_indices)]
        #         control_index_min = control_indices.min()
        #
        #         data['control_index'] = data['control_index'] - control_index_min
        #         control['index'] = control['index'] - control_index_min
        #         yield type(self)(service_type=self.service_type,
        #                          service_subtype=self.service_subtype, ssid=self.ssid,
        #                          control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 42)

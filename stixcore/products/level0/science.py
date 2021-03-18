from pathlib import Path

import numpy as np

import astropy.units as u
from astropy.table.operations import unique, vstack

from stixcore.config.reader import read_energy_channels
from stixcore.datetime.datetime import DateTime
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_pixel_mask,
    _get_unique,
    rebin_proportional,
)
from stixcore.products.product import BaseProduct, ControlSci, Data
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")

__all__ = ['ScienceProduct', 'CompressedPixelData', 'SummedPixelData', 'Spectrogram', 'Aspect']


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

        self.obs_beg = DateTime.from_float(self.data['time'][0] - self.data['timedel'][0] / 2)
        self.obs_end = DateTime.from_float(self.data['time'][-1] + self.data['timedel'][-1] / 2)
        self.obs_avg = self.obs_beg + (self.obs_end - self.obs_beg) / 2

    def __add__(self, other):
        other.control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other.control))
        cnames = control.colnames
        cnames.remove('index')
        control = unique(control, cnames)

        other.data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1
        data = vstack((self.data, other.data))

        data_ind = np.isin(data['control_index'], control['index'])
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

            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data)


class CompressedPixelData(ScienceProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'xray-cpd'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control['compression_scheme_counts_skm'], \
            control['compression_scheme_counts_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00260')

        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00242')

        # control.remove_column('num_structures')
        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['index'] = 0

        data = Data()
        # TODO remove after solved https://github.com/i4Ds/STIXCore/issues/59
        data['delta_time'] = (np.array(packets.get_value('NIX00441'), np.int32)) * 0.1 * u.s
        unique_times = np.unique(data['delta_time'])

        data['rcr'] = np.array(packets.get_value('NIX00401', attr='value'), np.ubyte)
        data['num_pixel_sets'] = np.atleast_1d(_get_unique(packets, 'NIX00442', np.byte))
        pixel_masks = _get_pixel_mask(packets, 'NIXD0407')
        pixel_masks = pixel_masks.reshape(-1, data['num_pixel_sets'][0], 12)
        if ssid == 21 and data['num_pixel_sets'][0] != 12:
            pixel_masks = np.pad(pixel_masks, ((0, 0), (0, 12 - data['num_pixel_sets'][0]), (0, 0)))
        data['pixel_masks'] = pixel_masks
        data['detector_masks'] = _get_detector_mask(packets)
        data['integration_time'] = (np.array(1, np.uint16)) * 0.1 * u.s

        triggers = np.array([packets.get_value(f'NIX00{i}') for i in range(242, 258)])
        triggers_var = np.array([packets.get_value(f'NIX00{i}', attr='error')
                                 for i in range(242, 258)])

        data['triggers'] = triggers.T
        data['triggers_err'] = np.sqrt(triggers_var).T
        data['num_energy_groups'] = np.array(packets.get_value('NIX00258'), np.ubyte)

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
            out_counts = np.zeros((unique_times.size, 32, 12, 32))
            out_var = np.zeros((unique_times.size, 32, 12, 32))
        elif ssid == 22:
            out_counts = np.zeros((unique_times.size, 32, 4, 32))
            out_var = np.zeros((unique_times.size, 32, 4, 32))

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

        data['time'] = (DateTime(coarse=int(control["time_stamp"]), fine=0).as_float()
                        + data['delta_time'] + data['integration_time']/2)
        data['timedel'] = data['integration_time']
        data['counts'] = out_counts * u.ct
        data['counts_err'] = out_var * u.ct
        data['control_index'] = control['index'][0]

        data = data['time', 'timedel', 'rcr', 'pixel_masks', 'detector_masks', 'num_pixel_sets',
                    'num_energy_groups', 'triggers', 'triggers_err', 'counts', 'counts_err']
        data['control_index'] = 0

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

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
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'xray-visibility'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control['compression_scheme_counts_skm'], \
            control['compression_scheme_counts_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00263')

        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00242')

        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['index'] = range(len(control))

        data = Data()
        data['control_index'] = np.full(len(packets.get_value('NIX00441')), 0)
        data['delta_time'] = (np.array(packets.get_value('NIX00441'), np.uint16)) * 0.1 * u.s
        unique_times = np.unique(data['delta_time'])

        # time = np.array([])
        # for dt in set(self.delta_time):
        #     i, = np.where(self.delta_time == dt)
        #     nt = sum(np.array(packets['NIX00258'])[i])
        #     time = np.append(time, np.repeat(dt, nt))
        # self.time = time

        data['rcr'] = packets.get_value('NIX00401', attr='value')
        data['pixel_mask1'] = _get_pixel_mask(packets, 'NIXD0407')
        data['pixel_mask2'] = _get_pixel_mask(packets, 'NIXD0444')
        data['pixel_mask3'] = _get_pixel_mask(packets, 'NIXD0445')
        data['pixel_mask4'] = _get_pixel_mask(packets, 'NIXD0446')
        data['pixel_mask5'] = _get_pixel_mask(packets, 'NIXD0447')
        data['detector_masks'] = _get_detector_mask(packets)
        data['integration_time'] = (np.array(packets.get_value('NIX00405'))) * 0.1 * u.s

        triggers = []
        triggers_var = []
        for i in range(242, 258):
            triggers.extend(packets.get_value(f'NIX00{i}'))
            triggers_var.extend(packets.get_value(f'NIX00{i}', attr='error'))

        data['triggers'] = np.array(triggers).reshape(-1, 16)
        data['triggers_err'] = np.sqrt(triggers_var).reshape(-1, 16)

        tids = np.searchsorted(data['delta_time'], unique_times)
        data = data[tids]

        sum(packets.get_value('NIX00258'))

        # Data
        vis = np.zeros((unique_times.size, 32, 32), dtype=complex)
        vis_err = np.zeros((unique_times.size, 32, 32), dtype=complex)
        e_low = np.array(packets.get_value('NIXD0016'))
        e_high = np.array(packets.get_value('NIXD0017'))

        # TODO create energy bin mask
        control['energy_bin_mask'] = np.full((1, 32), False, np.ubyte)
        all_energies = set(np.hstack([e_low, e_high]))
        control['energy_bin_mask'][:, list(all_energies)] = True

        data['flux'] = np.array(packets.get_value('NIX00261')).reshape(unique_times.size, -1)
        num_detectors = packets.get_value('NIX00262')[0]
        detector_id = np.array(packets.get_value('NIX00100')).reshape(unique_times.size, -1,
                                                                      num_detectors)

        # vis[:, detector_id[0], e_low.reshape(unique_times.size, -1)[0]] = (
        #         np.array(packets['NIX00263']) + np.array(packets['NIX00264'])
        #         * 1j).reshape(unique_times.size, num_detectors, -1)

        real = packets.get_value('NIX00263')
        real_var = packets.get_value('NIX00263', attr='error')
        imaginary = packets.get_value('NIX00264')
        imaginary_var = packets.get_value('NIX00264', attr='error')

        mesh = np.ix_(np.arange(unique_times.size), detector_id[0][0],
                      e_low.reshape(unique_times.size, -1)[0])
        vis[mesh] = (real + imaginary * 1j).reshape(unique_times.size, num_detectors, -1)

        # TODO this doesn't seem correct prob need combine in a better
        vis_err[mesh] = (np.sqrt(real_var) + np.sqrt(imaginary_var) * 1j).reshape(unique_times.size,
                                                                                  num_detectors, -1)

        data['visibility'] = vis
        data['visibility_err'] = vis_err

        data['time'] = DateTime(int(control["time_stamp"][0]), 0).as_float()\
            + data['delta_time'] + data['integration_time'] / 2
        data['timedel'] = data['integration_time']

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct):
    """
    X-ray Spectrogram or compression Level 2 data
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'xray-spectrogram'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci.from_packets(packets)

        control['compression_scheme_counts_skm'], \
            control['compression_scheme_counts_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00268')

        control['compression_scheme_triggers_skm'], \
            control['compression_scheme_triggers_skm'].meta = \
            _get_compression_scheme(packets, 'NIX00267')

        control = unique(control)

        if len(control) != 1:
            raise ValueError('Creating a science product form packets from multiple products')

        control['pixel_mask'] = np.unique(_get_pixel_mask(packets), axis=0)
        control['detector_mask'] = np.unique(_get_detector_mask(packets), axis=0)
        control['rcr'] = np.unique(packets.get_value('NIX00401', attr='value')).astype(np.int16)
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

        delta_time = (np.array(packets.get_value('NIX00441'), np.uint16)) * 0.1 * u.s
        closing_time_offset = (np.array(packets.get_value('NIX00269'), np.uint16)) * 0.1 * u.s

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

        # Data
        data = Data()
        data['time'] = DateTime(control["time_stamp"][0], 0).as_float() + centers
        data['timedel'] = deltas

        triggers = packets.get_value('NIX00267')
        triggers_var = packets.get_value('NIX00267', attr='error')

        data['triggers'] = triggers
        data['triggers_err'] = np.sqrt(triggers_var)
        data['counts'] = full_counts * u.ct
        data['counts_err'] = np.sqrt(full_counts_var) * u.ct
        data['control_index'] = 0

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 24)


class Aspect(ScienceProduct):
    """
    Aspect
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, **kwargs)
        self.name = 'burst-aspect'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):
        packets = BaseProduct.from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = ControlSci()
        scet_coarse = packets.get_value('NIX00445')
        scet_fine = packets.get_value('NIX00446')
        start_times = [DateTime(c, f) for c, f in zip(scet_coarse, scet_fine)]

        # TODO add cuase for older IDB
        control['summing_value'] = packets.get_value('NIX00088')
        control['averaging_value'] = packets.get_value('NIX00490')
        control['index'] = range(len(control))

        delta_time = ((control['summing_value'] * control['averaging_value']) / 1000.0)
        samples = packets.get_value('NIX00089')

        offsets = [delta_time[i] * 0.5 * np.arange(ns) * u.s for i, ns in enumerate(samples)]
        time = np.hstack([start_times[i].as_float() + offsets[i] for i in range(len(offsets))])
        timedel = np.hstack(offsets)

        # Data
        try:
            data = Data()
            data['time'] = time
            data['timedel'] = timedel
            data['cha_diode0'] = packets.get_value('NIX00090')
            data['cha_diode0'].meta = {'NIXS': 'NIX00090'}
            data['cha_diode1'] = packets.get_value('NIX00091')
            data['cha_diode1'].meta = {'NIXS': 'NIX00091'}
            data['chb_diode0'] = packets.get_value('NIX00092')
            data['chb_diode0'].meta = {'NIXS': 'NIX00092'}
            data['chb_diode1'] = packets.get_value('NIX00093')
            data['chb_diode1'].meta = {'NIXS': 'NIX00093'}
            data['control_index'] = np.hstack([np.full(ns, i) for i, ns in enumerate(samples)])
        except ValueError as e:
            logger.warning(e)
            return None

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data)

    def to_days(self):
        min_day = np.floor((self.data['time'] / (1 * u.day).to('s')).min())
        max_day = np.ceil((self.data['time'] / (1 * u.day).to('s')).max())

        days = np.arange(min_day, max_day) * u.day
        date_ranges = [(day, day+1*u.day) for day in days]
        for dstart, dend in date_ranges:
            i = np.where((self.data['time'] >= dstart) &
                         (self.data['time'] < dend))

            # Implement slice on parent or add mixin
            data = self.data[i]
            control_indices = np.unique(data['control_index'])
            control = self.control[np.isin(self.control['index'], control_indices)]
            control_index_min = control_indices.min()

            data['control_index'] = data['control_index'] - control_index_min
            control['index'] = control['index'] - control_index_min
            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L0' and service_type == 21
                and service_subtype == 6 and ssid == 42)

"""
High level STIX data products created from single stand alone packets or a sequence of packets.
"""

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.table import QTable, unique, vstack
from astropy.time import Time

from stixcore.calibration.compression import decompress
from stixcore.datetime.datetime import DateTime
from stixcore.products.common import (
    _get_compression_scheme,
    _get_detector_mask,
    _get_energy_bins,
    _get_num_energies,
    _get_pixel_mask,
)
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

# __all__ = ['LightCurve', 'Background', 'Spectra', 'Variance', 'CalibrationSpectra',
#            'FlareFlagAndLocation', 'TMManagementAndFlareList', 'get_energies_from_mask']

ENERGY_CHANNELS = {
    0: {'channel_edge': 0, 'energy_edge': 0, 'e_lower': 0.0, 'e_upper': 4.0, 'bin_width': 4.0,
        'dE_E': 2.000, 'ql_channel': None},
    1: {'channel_edge': 1, 'energy_edge': 4, 'e_lower': 4.0, 'e_upper': 5.0, 'bin_width': 1.0,
        'dE_E': 0.222, 'ql_channel': 0},
    2: {'channel_edge': 2, 'energy_edge': 5, 'e_lower': 5.0, 'e_upper': 6.0, 'bin_width': 1.0,
        'dE_E': 0.182, 'ql_channel': 0},
    3: {'channel_edge': 3, 'energy_edge': 6, 'e_lower': 6.0, 'e_upper': 7.0, 'bin_width': 1.0,
        'dE_E': 0.154, 'ql_channel': 0},
    4: {'channel_edge': 4, 'energy_edge': 7, 'e_lower': 7.0, 'e_upper': 8.0, 'bin_width': 1.0,
        'dE_E': 0.133, 'ql_channel': 0},
    5: {'channel_edge': 5, 'energy_edge': 8, 'e_lower': 8.0, 'e_upper': 9.0, 'bin_width': 1.0,
        'dE_E': 0.118, 'ql_channel': 0},
    6: {'channel_edge': 6, 'energy_edge': 9, 'e_lower': 9.0, 'e_upper': 10.0, 'bin_width': 1.0,
        'dE_E': 0.105, 'ql_channel': 0},
    7: {'channel_edge': 7, 'energy_edge': 10, 'e_lower': 10.0, 'e_upper': 11.0, 'bin_width': 1.0,
        'dE_E': 0.095, 'ql_channel': 1},
    8: {'channel_edge': 8, 'energy_edge': 11, 'e_lower': 11.0, 'e_upper': 12.0, 'bin_width': 1.0,
        'dE_E': 0.087, 'ql_channel': 1},
    9: {'channel_edge': 9, 'energy_edge': 12, 'e_lower': 12.0, 'e_upper': 13.0, 'bin_width': 1.0,
        'dE_E': 0.080, 'ql_channel': 1},
    10: {'channel_edge': 10, 'energy_edge': 13, 'e_lower': 13.0, 'e_upper': 14.0, 'bin_width': 1.0,
         'dE_E': 0.074, 'ql_channel': 1},
    11: {'channel_edge': 11, 'energy_edge': 14, 'e_lower': 14.0, 'e_upper': 15.0, 'bin_width': 1.0,
         'dE_E': 0.069, 'ql_channel': 1},
    12: {'channel_edge': 12, 'energy_edge': 15, 'e_lower': 15.0, 'e_upper': 16.0, 'bin_width': 1.0,
         'dE_E': 0.065, 'ql_channel': 2},
    13: {'channel_edge': 13, 'energy_edge': 16, 'e_lower': 16.0, 'e_upper': 18.0, 'bin_width': 1.0,
         'dE_E': 0.061, 'ql_channel': 2},
    14: {'channel_edge': 14, 'energy_edge': 18, 'e_lower': 18.0, 'e_upper': 20.0, 'bin_width': 2.0,
         'dE_E': 0.105, 'ql_channel': 2},
    15: {'channel_edge': 15, 'energy_edge': 20, 'e_lower': 20.0, 'e_upper': 22.0, 'bin_width': 2.0,
         'dE_E': 0.095, 'ql_channel': 2},
    16: {'channel_edge': 16, 'energy_edge': 22, 'e_lower': 22.0, 'e_upper': 25.0, 'bin_width': 3.0,
         'dE_E': 0.128, 'ql_channel': 2},
    17: {'channel_edge': 17, 'energy_edge': 25, 'e_lower': 25.0, 'e_upper': 28.0, 'bin_width': 3.0,
         'dE_E': 0.113, 'ql_channel': 3},
    18: {'channel_edge': 18, 'energy_edge': 28, 'e_lower': 28.0, 'e_upper': 32.0, 'bin_width': 4.0,
         'dE_E': 0.133, 'ql_channel': 3},
    19: {'channel_edge': 19, 'energy_edge': 32, 'e_lower': 32.0, 'e_upper': 36.0, 'bin_width': 4.0,
         'dE_E': 0.118, 'ql_channel': 3},
    20: {'channel_edge': 20, 'energy_edge': 36, 'e_lower': 36.0, 'e_upper': 40.0, 'bin_width': 4.0,
         'dE_E': 0.105, 'ql_channel': 3},
    21: {'channel_edge': 21, 'energy_edge': 40, 'e_lower': 40.0, 'e_upper': 45.0, 'bin_width': 5.0,
         'dE_E': 0.118, 'ql_channel': 3},
    22: {'channel_edge': 22, 'energy_edge': 45, 'e_lower': 45.0, 'e_upper': 50.0, 'bin_width': 5.0,
         'dE_E': 0.105, 'ql_channel': 3},
    23: {'channel_edge': 23, 'energy_edge': 50, 'e_lower': 50.0, 'e_upper': 56.0, 'bin_width': 6.0,
         'dE_E': 0.113, 'ql_channel': 4},
    24: {'channel_edge': 24, 'energy_edge': 56, 'e_lower': 56.0, 'e_upper': 63.0, 'bin_width': 7.0,
         'dE_E': 0.118, 'ql_channel': 4},
    25: {'channel_edge': 25, 'energy_edge': 63, 'e_lower': 63.0, 'e_upper': 70.0, 'bin_width': 7.0,
         'dE_E': 0.105, 'ql_channel': 4},
    26: {'channel_edge': 26, 'energy_edge': 70, 'e_lower': 70.0, 'e_upper': 76.0, 'bin_width': 6.0,
         'dE_E': 0.082, 'ql_channel': 4},
    27: {'channel_edge': 27, 'energy_edge': 76, 'e_lower': 76.0, 'e_upper': 84.0, 'bin_width': 8.0,
         'dE_E': 0.100, 'ql_channel': 4},
    28: {'channel_edge': 28, 'energy_edge': 84, 'e_lower': 84.0, 'e_upper': 100.0,
         'bin_width': 16.0, 'dE_El': 0.174, 'ql_channel': 4},
    29: {'channel_edge': 29, 'energy_edge': 100, 'e_lower': 100.0, 'e_upper': 120.0,
         'bin_width': 20.0, 'dE_El': 0.182, 'ql_channel': 4},
    30: {'channel_edge': 30, 'energy_edge': 120, 'e_lower': 120.0, 'e_upper': 150.0,
         'bin_width': 30.0, 'dE_El': 0.222, 'ql_channel': 4},
    31: {'channel_edge': 31, 'energy_edge': 150, 'e_lower': 150.0, 'e_upper': np.inf,
         'bin_width': np.inf, 'dE_E': np.inf, 'ql_channel': None}
}


class Control(QTable):

    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate integration time for each sample in each packet
        base_times = np.hstack(
            [np.full(ns, DateTime(coarse=ct, fine=ft).as_float().value)
             for ns, ct, ft in self[['num_samples', 'scet_coarse', 'scet_fine']]]) * u.s
        start_delta = np.hstack(
            [(np.arange(ns) * it) for ns, it in self[['num_samples', 'integration_time']]])

        durations = np.hstack([np.ones(num_sample) * int_time for num_sample, int_time in
                              self[['num_samples', 'integration_time']]])

        # Add the delta time to base times and convert to relative from start time
        times = base_times + start_delta + (durations / 2)

        return times, durations

    @classmethod
    def from_packets(cls, packets):
        # Header
        control = cls()
        # self.energy_bin_mask = None
        # self.samples = None
        control['scet_coarse'] = np.array(packets.get('NIX00445'), np.uint32)
        # Not all QL data have fine time in TM default to 0 if no present
        scet_fine = packets.get('NIX00446')
        if scet_fine:
            control['scet_fine'] = np.array(scet_fine, np.uint32)
        else:
            control['scet_fine'] = np.zeros_like(control['scet_coarse'], np.uint32)

        integration_time = packets.get('NIX00405')
        if integration_time:
            control['integration_time'] = (np.array(integration_time, np.float) + 1) * 0.1 * u.s
        else:
            control['integration_time'] = np.zeros_like(control['scet_coarse'], np.float) * u.s

        # control = unique(control)
        control['index'] = np.arange(len(control))

        return control


class Data(QTable):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    @classmethod
    def from_packets(cls, packets):
        raise NotImplementedError


class Product:
    def __init__(self, control, data):
        """
        Generic product composed of control and data

        Parameters
        ----------
        control : stix_parser.products.quicklook.Control
            Table containing control information
        data : stix_parser.products.quicklook.Data
            Table containing data
        """
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
        other : A subclass of stix_parser.products.quicklook.Product

        Returns
        -------
        A subclass of stix_parser.products.quicklook.Product
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

        return type(self)(control, data)

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
            yield type(self)(control=control, data=data)

    @classmethod
    def from_packets(cls, packets, eng_packets):
        control = Control.from_packets(packets)
        data = Data.from_packets(packets)
        return cls(control, data)

    @classmethod
    def from_fits(cls, fitspath):
        header = fits.getheader(fitspath)
        control = QTable.read(fitspath, hdu='CONTROL')
        data = QTable.read(fitspath, hdu='DATA')
        obs_beg = Time(header['DATE_OBS'])
        data['time'] = (data['time'] + obs_beg)
        return cls(control=control, data=data)

    def get_energies(self):
        if 'energy_bin_edge_mask' in self.control.colnames:
            energies = get_energies_from_mask(self.control['energy_bin_edge_mask'][0])
        elif 'energy_bin_mask' in self.control.colnames:
            energies = get_energies_from_mask(self.control['energy_bin_mask'][0])
        else:
            energies = get_energies_from_mask()

        return energies


class LightCurve(Product):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, control=None, data=None):
        super().__init__(control=control, data=data)
        self.name = 'LightCurve'
        self.level = 'L0'

    @classmethod
    def from_packets(cls, packets):
        control = Control.from_packets(packets)
        control['detector_mask'] = _get_detector_mask(packets)
        control['pixel_mask'] = _get_pixel_mask(packets)
        control['energy_bin_edge_mask'] = _get_energy_bins(packets, 'NIX00266', 'NIXD0107')
        control['compression_scheme_counts_skm'] = \
            _get_compression_scheme(packets, 'NIXD0101', 'NIXD0102', 'NIXD0103')
        control['compression_scheme_triggers_skm'] = \
            _get_compression_scheme(packets, 'NIXD0104', 'NIXD0105', 'NIXD0106')
        control['num_energies'] = _get_num_energies(packets)
        control['num_samples'] = np.array(packets.get('NIX00271')).flatten()[
            np.cumsum(control['num_energies']) - 1]

        time, duration = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        cs, ck, cm = control['compression_scheme_counts_skm'][0]
        counts, counts_var = decompress(packets.get('NIX00272'),
                                        s=cs, k=ck, m=cm, return_variance=True)

        ts, tk, tm = control['compression_scheme_triggers_skm'][0]
        triggers, triggers_var = decompress(packets.get('NIX00274'),
                                            s=ts, k=tk, m=tm, return_variance=True)
        # this may no longer be needed
        # flat_indices = np.hstack((0, np.cumsum([*control['num_samples']]) *
        #                           control['num_energies'])).astype(int)

        counts_reformed = np.hstack(c for c in counts).T
        counts_var_reformed = np.hstack(c for c in counts_var).T
        # counts_reformed = [
        #     np.array(counts[flat_indices[i]:flat_indices[i + 1]]).reshape(n_eng, n_sam)
        #     for i, (n_sam, n_eng) in enumerate(control[['num_samples', 'num_energies']])]

        # counts_var_reformed = [
        #     np.array(counts_var[flat_indices[i]:flat_indices[i + 1]]).reshape(n_eng, n_sam)
        #     for i, (n_sam, n_eng) in enumerate(control[['num_samples', 'num_energies']])]

        # counts = np.hstack(counts_reformed).T
        # counts_var = np.hstack(counts_var_reformed).T

        data = Data()
        data['control_index'] = control_indices
        data['time'] = time
        data['timedel'] = duration
        data['triggers'] = triggers.flatten()
        data['triggers_err'] = np.sqrt(triggers_var.flatten())
        data['rcr'] = np.array(packets.get('NIX00276')).flatten()
        data['counts'] = counts_reformed * u.ct
        data['counts_err'] = np.sqrt(counts_var_reformed) * u.ct

        return cls(control=control, data=data)

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'


def get_energies_from_mask(mask=None):
    """
    Return energy channels for
    Parameters
    ----------
    mask : list or array
        Energy bin mask

    Returns
    -------
    tuple
        Lower and high energy edges
    """

    if mask is None:
        low = [ENERGY_CHANNELS[edge]['e_lower'] for edge in range(32)]
        high = [ENERGY_CHANNELS[edge]['e_upper'] for edge in range(32)]
    elif len(mask) == 33:
        edges = np.where(np.array(mask) == 1)[0]
        channel_edges = [edges[i:i + 2].tolist() for i in range(len(edges) - 1)]
        low = []
        high = []
        for edge in channel_edges:
            l, h = edge
            low.append(ENERGY_CHANNELS[l]['e_lower'])
            high.append(ENERGY_CHANNELS[h - 1]['e_upper'])
    elif len(mask) == 32:
        edges = np.where(np.array(mask) == 1)
        low_ind = np.min(edges)
        high_ind = np.max(edges)
        low = [ENERGY_CHANNELS[low_ind]['e_lower']]
        high = [ENERGY_CHANNELS[high_ind]['e_upper']]
    else:
        raise ValueError(f'Energy mask or edges must have a length of 32 or 33 not {len(mask)}')

    return low, high

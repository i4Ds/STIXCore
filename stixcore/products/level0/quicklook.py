"""
High level STIX data products created from single stand alone packets or a sequence of packets.
"""
from binascii import unhexlify

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
)
from stixcore.products.product import BaseProduct, Control, Data
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import PacketSequence
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve']

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
        self.name = 'LightCurve'
        self.level = 'L0'

    @classmethod
    def from_levelb(cls, levelb):

        packets = [Packet(unhexlify(d)) for d in levelb.data['data']]
        packets = PacketSequence(packets)

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
        control['compression_scheme_counts_skm'] = \
            _get_compression_scheme(packets, 'NIXD0101', 'NIXD0102', 'NIXD0103')
        control['compression_scheme_counts_skm'].meta = {'NIXS': ['NIXD0101', 'NIXD0102',
                                                                  'NIXD0103']}
        control['compression_scheme_triggers_skm'] = \
            _get_compression_scheme(packets, 'NIXD0104', 'NIXD0105', 'NIXD0106')
        control['compression_scheme_triggers_skm'].meta = {'NIXS': ['NIXD0104', 'NIXD0105',
                                                                    'NIXD0106']}
        control['num_energies'] = _get_num_energies(packets)
        control['num_energies'].meta = {'NIXS': 'NIX00270'}
        control['num_samples'] = np.array(packets.get('NIX00271')).flatten()[
            np.cumsum(control['num_energies']) - 1]
        control['num_samples'].meta = {'NIXS': 'NIX00271'}

        time, duration = control._get_time()
        # Map a given entry back to the control info through index
        control_indices = np.hstack([np.full(ns, cind) for ns, cind in
                                     control[['num_samples', 'index']]])

        # cs, ck, cm = control['compression_scheme_counts_skm'][0]
        counts = np.hstack(packets.get('NIX00272'))
        # counts, counts_var = decompress(counts, s=cs, k=ck, m=cm, return_variance=True)

        # ts, tk, tm = control['compression_scheme_triggers_skm'][0]
        triggers = np.hstack(packets.get('NIX00274'))
        # triggers, triggers_var = decompress(triggers, s=ts, k=tk, m=tm, return_variance=True)
        # this may no longer be needed
        # flat_indices = np.hstack((0, np.cumsum([*control['num_samples']]) *
        #                           control['num_energies'])).astype(int)

        # counts_reformed = np.hstack(c for c in counts).T
        # counts_var_reformed = np.hstack(c for c in counts_var).T
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
        data['triggers'] = triggers
        data['triggers'].meta = {'NIXS': 'NIX00274'}
        # data['triggers_err'] = np.sqrt(triggers_var)
        data['rcr'] = np.hstack(packets.get('NIX00276')).flatten()
        data['rcr'].meta = {'NIXS': 'NIX00276'}
        data['counts'] = counts.T
        data['counts'].meta = {'NIXS': 'NIX00272'}
        # data['counts_err'] = np.sqrt(counts_var).T * u.ct

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

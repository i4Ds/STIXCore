"""
.
"""
from datetime import datetime, timedelta

import numpy as np

import astropy.units as u
from astropy.table import unique, vstack
from astropy.table.table import QTable
from astropy.time.core import Time

from stixcore.products.common import _get_energies_from_mask
from stixcore.products.product import BaseProduct
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve']

logger = get_logger(__name__)


class QLProduct(BaseProduct):
    def __init__(self, service_type, service_subtype, ssid, data, control, **kwargs):
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
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.control = control
        self.data = data

        energies = kwargs.get('energies', False)
        if not energies:
            energies = self.get_energies()
        self.energies = energies

        self.obs_beg = self.data['time'][0] - (self.control['integration_time'][0] / 2)
        self.obs_end = self.data['time'][-1] + (self.control['integration_time'][-1] / 2)
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

        energies = unique(vstack(self.energies, other.energies))

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data, energies=energies)

    def __repr__(self):
        return f'<{self.__class__.__name__}\n' \
               f' {self.control.__repr__()}\n' \
               f' {self.data.__repr__()}\n' \
               f'>'

    # @classmethod
    # def from_fits(cls, fitspath):
    #     header = fits.getheader(fitspath)
    #     header_info = {'service_type': int(header.get('stype')),
    #                    'service_subtype': int(header.get('sstype')),
    #                    'pi1_val': int(header.get('ssid')),
    #                    'spid': int(header.get('spid'))}
    #     control = QTable.read(fitspath, hdu='CONTROL')
    #     data = QTable.read(fitspath, hdu='DATA')
    #     energies = QTable.read(fitspath, hdu='ENERGIES')
    #     if header['level'] == 'L0':
    #         obs_beg = Time(DateTime.from_string(header['DATE_OBS']).to_datetime())
    #     elif header['level'] == 'L1':
    #         obs_beg = Time(header['DATE_OBS'])
    #     data['time'] = (obs_beg + data['time'])
    #     return cls(**header_info, control=control, data=data, energies=energies)

    def to_days(self):
        days = set([(t.year, t.month, t.day) for t in self.data['time'].to_datetime()])
        date_ranges = [(datetime(*day), datetime(*day) + timedelta(days=1)) for day in days]
        for dstart, dend in date_ranges:
            i = np.where((self.data['time'] >= dstart) &
                         (self.data['time'] < dend))

            data = self.data[i]
            control_indices = np.unique(data['control_index'])
            control = self.control[np.isin(self.control['index'], control_indices)]
            control_index_min = control_indices.min()

            data['control_index'] = data['control_index'] - control_index_min
            control['index'] = control['index'] - control_index_min
            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data,
                             energies=self.energies)

    def get_energies(self):
        if 'energy_bin_edge_mask' in self.control.colnames:
            elow, ehigh = _get_energies_from_mask(self.control['energy_bin_edge_mask'][0])
        elif 'energy_bin_mask' in self.control.colnames:
            elow, ehigh = _get_energies_from_mask(self.control['energy_bin_mask'][0])
        else:
            elow, ehigh = _get_energies_from_mask()

        energies = QTable()
        energies['channel'] = range(len(elow))
        energies['e_low'] = elow * u.keV
        energies['e_high'] = ehigh * u.keV

        return energies


class LightCurve(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, **kwargs)
        self.name = 'LightCurve'
        self.level = 'L1'

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def from_level0(cls, level0):
        delta_times = level0.data['time'] - level0.obs_beg.as_float()
        level0.data['time'] = Time(level0.obs_beg.to_datetime()) + delta_times
        return cls(service_type=level0.service_type, service_subtype=level0.service_subtype,
                   ssid=level0.ssid, control=level0.control, data=level0.data)

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 30)

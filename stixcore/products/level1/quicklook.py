"""
.
"""
from datetime import datetime, timedelta

import numpy as np

from astropy.io import fits
from astropy.table import QTable, unique, vstack
from astropy.time.core import Time

from stixcore.datetime.datetime import DateTime
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Product:
    def __init__(self, *, service_type, service_subtype, pi1_val, spid, control, data, energies):
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
        self.pi1_val = pi1_val
        self.spid = spid
        self.control = control
        self.data = data
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

        energies = unique(vstack(self.energies, other.energies))

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          pi1_val=self.pi1_val, spid=self.spid, control=control, data=data,
                          energies=energies)

    def __repr__(self):
        return f'<{self.__class__.__name__}\n' \
               f' {self.control.__repr__()}\n' \
               f' {self.data.__repr__()}\n' \
               f'>'

    @classmethod
    def from_fits(cls, fitspath):
        header = fits.getheader(fitspath)
        header_info = {'service_type': int(header.get('stype')),
                       'service_subtype': int(header.get('sstype')),
                       'pi1_val': int(header.get('ssid')),
                       'spid': int(header.get('spid'))}
        control = QTable.read(fitspath, hdu='CONTROL')
        data = QTable.read(fitspath, hdu='DATA')
        energies = QTable.read(fitspath, hdu='ENERGIES')
        if header['level'] == 'L0':
            obs_beg = Time(DateTime.from_string(header['DATE_OBS']).to_datetime())
        elif header['level'] == 'L1':
            obs_beg = Time(header['DATE_OBS'])
        data['time'] = (obs_beg + data['time'])
        return cls(**header_info, control=control, data=data, energies=energies)

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
                             pi1_val=self.pi1_val, spid=self.spid, control=control, data=data,
                             energies=self.energies)


class LightCurve(Product):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = 'LightCurve'
        self.level = 'L1'

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

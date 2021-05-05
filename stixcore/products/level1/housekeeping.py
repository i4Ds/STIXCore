"""
House Keeping data products
"""
from collections import defaultdict

import numpy as np

import astropy.units as u

from stixcore.products.level1.quicklook import QLProduct
from stixcore.time import SCETimeRange

__all__ = ['MiniReport', 'MaxiReport']


class ToDaysMixin:
    def to_days(self):
        for day_start in self.utc_timerange.get_dates():
            day_end = day_start + 1*u.d
            i = np.where((self.data['time'] >= day_start) & (self.data['time'] < day_end))

            data = self.data[i]
            control_indices = np.unique(data['control_index'])
            control = self.control[np.isin(self.control['index'], control_indices)]
            control_index_min = control_indices.min()

            # scet_timerange = SCETimeRange(start=, end=de)
            # utc_timerange =

            data['control_index'] = data['control_index'] - control_index_min
            control['index'] = control['index'] - control_index_min
            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data,
                             idb_versions=self.idb_versions, scet_timerange=self.scet_timerange)


class MiniReport(QLProduct):
    """
    Mini house keeping reported during start up of the flight software.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data,
                         idb_versions=idb_versions, **kwargs)
        self.name = 'mini'
        self.level = 'L1'
        self.type = 'hk'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 3
                and service_subtype == 25 and ssid == 1)


class MaxiReport(QLProduct):
    """
    Maxi house keeping reported in all modes while the flight software is running.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'maxi'
        self.level = 'L1'
        self.type = 'hk'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 3
                and service_subtype == 25 and ssid == 2)

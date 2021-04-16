"""
House Keeping data products
"""
from collections import defaultdict

from stixcore.datetime.datetime import SCETimeRange
from stixcore.products.level1.quicklook import QLProduct

__all__ = ['MiniReport', 'MaxiReport']


class MiniReport(QLProduct):
    """
    Mini house keeping reported during start up of the flight software.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data,
                         idb=defaultdict(SCETimeRange), **kwargs)
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
                 idb=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb=idb, **kwargs)
        self.name = 'maxi'
        self.level = 'L1'
        self.type = 'hk'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 3
                and service_subtype == 25 and ssid == 2)

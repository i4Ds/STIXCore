"""
.
"""

from collections import defaultdict

from stixcore.processing.engineering import raw_to_engineering_product
from stixcore.products.level0.quicklook import QLProduct as QLProductL0
from stixcore.time import SCETimeRange
from stixcore.tmtc.packets import GenericPacket
from stixcore.util.logging import get_logger

__all__ = ['QLProduct', 'LightCurve']

logger = get_logger(__name__)


class QLProduct(QLProductL0):
    def __init__(self, service_type, service_subtype, ssid, data, control,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

    @classmethod
    def from_level0(cls, l0product, idbm=GenericPacket.idb_manager):
        l1 = cls(service_type=l0product.service_type,
                 service_subtype=l0product.service_subtype,
                 ssid=l0product.ssid,
                 control=l0product.control,
                 data=l0product.data,
                 idb_versions=l0product.idb_versions,
                 scet_timerange=l0product.scet_timerange)

        raw_to_engineering_product(l1, idbm)

        return l1


class LightCurve(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.obs_beg = self.data['time'][0] - (self.control['integration_time'][0] / 2)
        self.obs_end = self.data['time'][-1] + (self.control['integration_time'][-1] / 2)
        self.obs_avg = self.obs_beg + (self.obs_end - self.obs_beg) / 2
        self.name = 'ql-lightcurve'
        self.level = 'L1'

    def __repr__(self):
        return f'{self.name}, {self.level}\n' \
               f'{self.obs_beg}, {self.obs_end}\n ' \
               f'{len(self.control)}, {len(self.data)}'

    @classmethod
    def from_level0(cls, level0):
        # delta_times = level0.data['time'] - level0.obs_beg.as_float()
        # level0.data['time'] = Time(level0.obs_beg.to_datetime()) + delta_times
        return cls(service_type=level0.service_type, service_subtype=level0.service_subtype,
                   ssid=level0.ssid, control=level0.control, data=level0.data,
                   idb_versions=level0.idb_versions)

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 30)

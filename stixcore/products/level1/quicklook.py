"""
.
"""

from collections import defaultdict

import numpy as np

import astropy.units as u

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

    @property
    def utc_timerange(self):
        return self.scet_timerange.to_timerange()

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

    def to_days(self):

        utc_timerange = self.scet_timerange.to_timerange()

        for day in utc_timerange.get_dates():
            ds = day
            de = day + 1 * u.day
            utc_times = self.data['time'].to_time()
            i = np.where((utc_times >= ds) & (utc_times < de))

            if len(i[0]) > 0:
                scet_timerange = SCETimeRange(start=self.data['time'][i[0]]
                                              - self.data['timedel'][i[0]]/2,
                                              end=self.data['time'][i[-1]]
                                              + self.data['timedel'][i[-1]]/2)

                data = self.data[i]
                control_indices = np.unique(data['control_index'])
                control = self.control[np.isin(self.control['index'], control_indices)]
                control_index_min = control_indices.min()

                data['control_index'] = data['control_index'] - control_index_min
                control['index'] = control['index'] - control_index_min
                yield type(self)(service_type=self.service_type,
                                 service_subtype=self.service_subtype, ssid=self.ssid,
                                 control=control, data=data, idb_versions=self.idb_versions,
                                 scet_timerange=scet_timerange)

    def __repr__(self):
        return f'{self.__class__.__name__}, {self.level}\n' \
               f'{self.scet_timerange}\n ' \
               f'{len(self.control)}, {len(self.data)}'


class LightCurve(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'lightcurve'
        self.level = 'L1'

    # @classmethod
    # def from_level0(cls, level0):
    #
    #     # delta_times = level0.data['time'] - level0.obs_beg.as_float()
    #     # level0.data['time'] = Time(level0.obs_beg.to_datetime()) + delta_times
    #     return cls(service_type=level0.service_type, service_subtype=level0.service_subtype,
    #                ssid=level0.ssid, control=level0.control, data=level0.data,
    #                idb_versions=level0.idb_versions, scet_timerange=level0.scet_timerange)

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 30)


class Background(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'background'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 31)


class Spectra(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'spectra'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 32)


class Variance(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'variance'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 33)


class FlareFlag(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'flareflag'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 34)


class EnergyCalibration(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'energy'
        self.level = 'L1'
        self.type = 'cal'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 41)


class TMStatusFlareList(QLProduct):
    """
    Quick Look Light Curve data product.
    """
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'ql-tmstatusflarelist'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 43)

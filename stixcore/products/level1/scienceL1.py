from collections import defaultdict

from stixcore.products.level0.scienceL0 import ScienceProduct
from stixcore.products.product import L1Mixin
from stixcore.time import SCETimeRange

__all__ = ['RawPixelData', 'CompressedPixelData', 'SummedPixelData', 'Visibility', 'Spectrogram',
           'Aspect']


class RawPixelData(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-rpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 20)


class CompressedPixelData(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-cpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 21)


class SummedPixelData(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-scpd'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 22)


class Visibility(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-vis'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 23)


class Spectrogram(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 24)


class Aspect(ScienceProduct, L1Mixin):
    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'aspect-burst'
        self.level = 'L1'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L1' and service_type == 21
                and service_subtype == 6 and ssid == 42)

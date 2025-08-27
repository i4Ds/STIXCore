"""
House Keeping data products
"""

from collections import defaultdict

from stixcore.products.level0.housekeepingL0 import HKProduct
from stixcore.products.product import L1Mixin
from stixcore.time import SCETimeRange

__all__ = ["MiniReport", "MaxiReport"]


class MiniReport(HKProduct, L1Mixin):
    """Mini house keeping reported during start up of the flight software.

    In level 1 format.
    """

    PRODUCT_PROCESSING_VERSION = 2

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )
        self.name = "mini"
        self.level = "L1"
        self.type = "hk"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L1" and service_type == 3 and service_subtype == 25 and ssid == 1


class MaxiReport(HKProduct, L1Mixin):
    """Maxi house keeping reported in all modes while the flight software is running.

    In level 1 format.
    """

    PRODUCT_PROCESSING_VERSION = 2

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )
        self.name = "maxi"
        self.level = "L1"
        self.type = "hk"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L1" and service_type == 3 and service_subtype == 25 and ssid == 2

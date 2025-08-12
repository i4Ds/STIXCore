from collections import defaultdict

from stixcore.products.product import GenericProduct
from stixcore.time.datetime import SCETimeRange


class LightCurve(GenericProduct):
    """ "Low Latency Quick Look Light Curve data product.

    for Low Latency Processing
    """

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
        self.name = "lightcurve"
        self.level = kwargs.get("level", "LL01")
        self.type = "ql"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "LL01" and service_type == 21 and service_subtype == 6 and ssid == 30


class FlareFlag(GenericProduct):
    """Low Latency Quick Look Flare Flag and Location data product.

    for Low Latency Processing
    """

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
        self.name = "flareflag"
        self.level = kwargs.get("level", "LL01")
        self.type = "ql"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "LL01" and service_type == 21 and service_subtype == 6 and ssid == 34

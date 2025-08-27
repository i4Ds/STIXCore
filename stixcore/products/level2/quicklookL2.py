"""
.
"""

from collections import defaultdict

from stixcore.products.level0.quicklookL0 import QLProduct
from stixcore.products.product import L2Mixin
from stixcore.time import SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ["LightCurve", "Background", "Spectra", "Variance", "FlareFlag", "EnergyCalibration", "TMStatusFlareList"]

logger = get_logger(__name__)


class LightCurve(QLProduct, L2Mixin):
    """Quick Look Light Curve data product.

    In level 2 format.
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
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 30


class Background(QLProduct, L2Mixin):
    """Quick Look Background Light Curve data product.

    In level 2 format.
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

        self.name = "background"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 31


class Spectra(QLProduct, L2Mixin):
    """Quick Look Spectra data product.

    In level 2 format.
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

        self.name = "spectra"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 32


class Variance(QLProduct, L2Mixin):
    """Quick Look Variance data product.

    In level 2 format.
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

        self.name = "variance"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 33


class FlareFlag(QLProduct, L2Mixin):
    """Quick Look Flare Flag and Location data product.

    In level 2 format.
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
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 34


class EnergyCalibration(QLProduct, L2Mixin):
    """Quick Look energy calibration data product.

    In level 2 format.
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

        self.name = "energy"
        self.level = "L2"
        self.type = "cal"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 41


class TMStatusFlareList(QLProduct, L2Mixin):
    """Quick Look TM Management status and Flare list data product.

    In level 2 format.
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

        self.name = "ql-tmstatusflarelist"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 43

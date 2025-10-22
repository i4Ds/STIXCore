"""
.
"""

from collections import defaultdict

import numpy as np

from stixcore.idb.manager import IDBManager
from stixcore.processing import engineering
from stixcore.products.level0.quicklookL0 import QLProduct
from stixcore.products.product import L1Mixin
from stixcore.time import SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ["LightCurve", "Background", "Spectra", "Variance", "FlareFlag", "EnergyCalibration", "TMStatusFlareList"]

logger = get_logger(__name__)


class LightCurve(QLProduct, L1Mixin):
    """Quick Look Light Curve data product.

    In level 1 format.
    """

    NAME = "lightcurve"

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

        self.name = LightCurve.NAME
        self.level = LightCurve.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == LightCurve.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 30


class Background(QLProduct, L1Mixin):
    """Quick Look Background Light Curve data product.

    In level 1 format.
    """

    NAME = "background"

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

        self.name = Background.NAME
        self.level = Background.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == Background.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 31


class Spectra(QLProduct, L1Mixin):
    """Quick Look Spectra data product.

    In level 1 format.
    """

    NAME = "spectra"

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

        self.name = Spectra.NAME
        self.level = Spectra.LEVEL

    @property
    def dmin(self):
        return self.data["spectra"].min().value

    @property
    def dmax(self):
        return self.data["spectra"].max().value

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == Spectra.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 32


class Variance(QLProduct, L1Mixin):
    """Quick Look Variance data product.

    In level 1 format.
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
        self.level = "L1"

    @property
    def dmin(self):
        return self.data["variance"].min()

    @property
    def dmax(self):
        return self.data["variance"].max()

    @property
    def bunit(self):
        # TODO define
        return " "

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L1" and service_type == 21 and service_subtype == 6 and ssid == 33


class FlareFlag(QLProduct, L1Mixin):
    """Quick Look Flare Flag and Location data product.

    In level 1 format.
    """

    NAME = "flareflag"

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

        self.name = FlareFlag.NAME
        self.level = FlareFlag.LEVEL

    @property
    def dmin(self):
        return np.nanmin([self.data["loc_y"].min(), self.data["loc_z"].min()])

    @property
    def dmax(self):
        return np.nanmax([self.data["loc_y"].max(), self.data["loc_z"].max()])

    @property
    def bunit(self):
        # TODO define
        return " "

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == FlareFlag.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 34


class EnergyCalibration(QLProduct, L1Mixin):
    """Quick Look energy calibration data product.

    In level 1 format.
    """

    NAME = "energy"
    PRODUCT_PROCESSING_VERSION = 3

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

        self.name = EnergyCalibration.NAME
        self.level = EnergyCalibration.LEVEL
        self.type = "cal"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == EnergyCalibration.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 41

    @classmethod
    def from_level0(cls, l0product, parent=""):
        l1 = cls(
            service_type=l0product.service_type,
            service_subtype=l0product.service_subtype,
            ssid=l0product.ssid,
            control=l0product.control,
            data=l0product.data,
            idb_versions=l0product.idb_versions,
            comment=l0product.comment,
            history=l0product.history,
        )

        l1.control.replace_column("parent", [parent] * len(l1.control))
        l1.level = "L1"
        engineering.raw_to_engineering_product(l1, IDBManager.instance)

        # fix for wrong calibration in IDB https://github.com/i4Ds/STIXCore/issues/432
        # nix00122 was wrong assumed to be in ds but it is plain s
        l1.control["integration_time"] = l1.control["integration_time"] * 10
        # nix00124 was wrong assumed to be in ds but it is unscaled ms
        l1.control["live_time"] = (l1.control["live_time"] / 100.0).to("ms").astype(np.uint32)
        # nix00124 was wrong assumed to be in s but it is us
        l1.control["quiet_time"] = (l1.control["quiet_time"] / 100000.0).to("us")
        return l1


class TMStatusFlareList(QLProduct, L1Mixin):
    """Quick Look TM Management status and Flare list data product.

    In level 1 format.
    """

    NAME = "tmstatusflarelist"

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

        self.name = TMStatusFlareList.NAME
        self.level = TMStatusFlareList.LEVEL

    @property
    def dmin(self):
        # TODO define
        return 0.0

    @property
    def dmax(self):
        # TODO define
        return 0.0

    @property
    def bunit(self):
        # TODO define
        return ""

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == TMStatusFlareList.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 43

"""
.
"""

from collections import defaultdict

import numpy as np

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

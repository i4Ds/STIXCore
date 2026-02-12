from collections import defaultdict

import numpy as np

from stixcore.products.level0.scienceL0 import ScienceProduct
from stixcore.products.product import L1Mixin
from stixcore.time import SCETimeRange

__all__ = ["RawPixelData", "CompressedPixelData", "SummedPixelData", "Visibility", "Spectrogram", "Aspect"]


class RawPixelData(ScienceProduct, L1Mixin):
    """Raw X-ray pixel counts: compression level 0. No aggregation.

    In level 1 format.
    """

    NAME = "xray-rpd"

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
        self.name = RawPixelData.NAME
        self.level = RawPixelData.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == RawPixelData.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 20


class CompressedPixelData(ScienceProduct, L1Mixin):
    """Aggregated (over time and/or energies) X-ray pixel counts: compression level 1.

    In level 1 format.
    """

    NAME = "xray-cpd"

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
        self.name = CompressedPixelData.NAME
        self.level = CompressedPixelData.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (
            kwargs["level"] == CompressedPixelData.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 21
        )


class SummedPixelData(ScienceProduct, L1Mixin):
    """Aggregated (over time and/or energies and pixelsets) X-ray pixel counts: compression level 2.

    In level 1 format.
    """

    NAME = "xray-scpd"

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
        self.name = SummedPixelData.NAME
        self.level = SummedPixelData.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == SummedPixelData.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 22


class Visibility(ScienceProduct, L1Mixin):
    """
    X-ray Visibilities or compression Level 3 data

    In level 1 format.
    """

    NAME = "xray-vis"

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
        self.name = Visibility.NAME
        self.level = Visibility.LEVEL

    @property
    def dmin(self):
        # TODO define columns for dmin/max
        return 0.0

    @property
    def dmax(self):
        # TODO define columns for dmin/max
        return 0.0

    @property
    def bunit(self):
        # TODO define columns for dmin/max
        return " "

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == Visibility.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 23


class Spectrogram(ScienceProduct, L1Mixin):
    """
    X-ray Spectrogram or compression Level 2 data

    In level 1 format.
    """

    NAME = "xray-spec"
    PRODUCT_PROCESSING_VERSION = 4

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
        self.name = Spectrogram.NAME
        self.level = Spectrogram.LEVEL

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == Spectrogram.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 24


class Aspect(ScienceProduct, L1Mixin):
    """Bulk Aspect data.

    In level 1 format.
    """

    NAME = "aspect-burst"

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
        self.name = Aspect.NAME
        self.level = Aspect.LEVEL

    @property
    def dmin(self):
        return np.nanmin(
            [
                self.data["cha_diode0"].min(),
                self.data["cha_diode1"].min(),
                self.data["chb_diode0"].min(),
                self.data["chb_diode1"].min(),
            ]
        )

    @property
    def dmax(self):
        return np.nanmax(
            [
                self.data["cha_diode0"].max(),
                self.data["cha_diode1"].max(),
                self.data["chb_diode0"].max(),
                self.data["chb_diode1"].max(),
            ]
        )

    @property
    def bunit(self):
        return " "

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == Aspect.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 42

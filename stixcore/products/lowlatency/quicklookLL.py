from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
from stixpy.timeseries import quicklook  # noqa  This registers the STIX timeseries with sunpy
from sunpy.timeseries import TimeSeries

from stixcore.products.level1.quicklookL1 import QLProduct
from stixcore.products.product import GenericProduct, L1Mixin
from stixcore.time.datetime import SCETimeRange

__all__ = ["LightCurve", "FlareFlag", "LightCurveL3"]


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


class LightCurveL3(QLProduct, L1Mixin):
    LEVEL = "LL03"
    TYPE = "ql"
    PRODUCT_PROCESSING_VERSION = 2
    NAME = "lightcurve"

    """"Low Latency Quick Look Light Curve data product.

    for Low Latency Processing
    Level 3 format - svg chart
    """
    def __init__(self, *, control, data, parent_file_path: Path, **kwargs):
        super().__init__(service_type=21, service_subtype=6,
                         ssid=34, control=control, data=data, **kwargs)
        self.name = LightCurveL3.NAME
        self.level = LightCurveL3.LEVEL
        self.type = LightCurveL3.TYPE
        self.parent_file_path = parent_file_path
        if "header" in kwargs:
            self.fits_header = kwargs.get("header")

    def get_plot(self):
        ql_lightcurves = TimeSeries(self.parent_file_path)
        fig = plt.figure(figsize=(12, 5), layout="tight")
        ax = ql_lightcurves.plot()
        ax.set_xlabel("Time [UTC]")
        fig.add_axes(ax)
        return fig


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

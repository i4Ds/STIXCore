from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
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
        super().__init__(service_type=21, service_subtype=6, ssid=34, control=control, data=data, **kwargs)
        self.name = LightCurveL3.NAME
        self.level = LightCurveL3.LEVEL
        self.type = LightCurveL3.TYPE
        self.parent_file_path = parent_file_path
        if "header" in kwargs:
            self.fits_header = kwargs.get("header")

    def get_plot(self):
        ql_lightcurves = TimeSeries(self.parent_file_path)

        fig, ax_lc = plt.subplots(figsize=(12, 6), layout="tight")
        ql_lightcurves.plot(axes=ax_lc)
        ax_lc.set_xlabel("Time [UTC]")

        # Add RCR plot in case of RCR > 0
        max_rcr = self.data["rcr"].max()
        if max_rcr > 0:
            # extend y-range on the right for more whitespace
            ymin, ymax = ax_lc.get_ylim()
            extra = (ymax - ymin) * 0.5  # ~20% more space at the top
            ax_lc.set_ylim(ymin, ymax + extra)

            time = ql_lightcurves.time.datetime
            rcr = self.data["rcr"].value.astype(np.float16)
            rcr[rcr == 0] = np.nan  # do not plot zero values
            ax_rcr = ax_lc.twinx()

            ax_rcr.plot(time, rcr, color="tab:cyan", linewidth=2, label="_nolabel")

            ax_rcr.set_ylabel("RCR >= 1: Attenuator inserted", color="tab:cyan")
            ax_rcr.set_ylim(-50, max_rcr + 3)  # full axis range
            ax_rcr.set_yticks([r for r in [1, 3, 5, 7] if r <= (max_rcr + 1)])  # only these ticks
            ax_rcr.tick_params(axis="y", labelcolor="tab:cyan")

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

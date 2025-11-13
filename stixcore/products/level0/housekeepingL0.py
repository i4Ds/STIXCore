"""
House Keeping data products
"""

from collections import defaultdict

from stixcore.products.product import Control, Data, GenericProduct
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

__all__ = ["MiniReport", "MaxiReport"]


class HKProduct(GenericProduct):
    """Generic house keeping product class composed of control and data."""

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        """Create a generic HK product composed of control and data.

        Parameters
        ----------
        service_type : `int`
            3
        service_subtype : `int`
            25
        ssid : `int`
            ssid of the data product
        control : `stixcore.products.product.Control`
            Table containing control information
        data : `stixcore.products.product.Data`
            Table containing data
        idb_versions : dict<SCETimeRange, VersionLabel>, optional
            a time range lookup what IDB versions are used within this data,
            by default defaultdict(SCETimeRange)
        """
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )
        self.level = "L0"
        self.type = "hk"

    @property
    def fits_daily_file(self):
        return True

    @classmethod
    def from_levelb(cls, levelb, *, parent="", keep_parse_tree=True):
        """Converts level binary HK packets to a L1 product.

        Parameters
        ----------
        levelb : `stixcore.products.levelb.binary.LevelB`
            The binary level product.
        parent : `str`, optional
            The parent data file name the binary packed comes from, by default ''
        NIX00405_offset : int, optional
            [description], by default 0

        Returns
        -------
        tuple (packets, idb_versions, control)
            the converted packets
            all used IDB versions and time periods
            initialized control table
        """
        packets, idb_versions = GenericProduct.getLeveL0Packets(levelb, keep_parse_tree=keep_parse_tree)

        control = Control()
        control["scet_coarse"] = packets.get("scet_coarse")
        control["scet_fine"] = packets.get("scet_fine")
        control["integration_time"] = 0
        control["index"] = range(len(control))
        control["raw_file"] = levelb.control["raw_file"]
        control["packet"] = levelb.control["packet"]
        control["parent"] = parent

        return packets, idb_versions, control


class MiniReport(HKProduct):
    """Mini house keeping reported during start up of the flight software.

    In level 0 format.
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

    @classmethod
    def from_levelb(cls, levelb, parent="", keep_parse_tree=True):
        packets, idb_versions, control = super().from_levelb(levelb, parent=parent, keep_parse_tree=keep_parse_tree)

        # Create array of times as dt from date_obs
        times = SCETime(control["scet_coarse"], control["scet_fine"])

        # Data
        data = Data()
        data["time"] = times
        data["timedel"] = SCETimeDelta(0, 0)
        for nix, param in packets.data[0].__dict__.items():
            if nix.startswith("NIXG") or nix == "NIX00020":
                continue

            name = param.idb_info.get_product_attribute_name()
            data.add_basic(name=name, nix=nix, attr="value", packets=packets)

        data["control_index"] = range(len(control))

        return cls(
            service_type=packets.service_type,
            service_subtype=packets.service_subtype,
            ssid=packets.ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            packets=packets,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L0" and service_type == 3 and service_subtype == 25 and ssid == 1


class MaxiReport(HKProduct):
    """
    Maxi house keeping reported in all modes while the flight software is running.

    In level 0 format.
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

    @classmethod
    def from_levelb(cls, levelb, parent="", keep_parse_tree=True):
        packets, idb_versions, control = super().from_levelb(levelb, parent=parent, keep_parse_tree=keep_parse_tree)

        # Create array of times as dt from date_obs
        times = SCETime(control["scet_coarse"], control["scet_fine"])

        # Data
        data = Data()
        data["time"] = times
        data["timedel"] = SCETimeDelta(0, 0)

        for nix, param in packets.data[0].__dict__.items():
            if nix.startswith("NIXG") or nix == "NIX00020":
                continue

            name = param.idb_info.get_product_attribute_name()
            data.add_basic(name=name, nix=nix, attr="value", packets=packets)

            if nix in ["NIX00078", "NIX00079", "NIX00080", "NIX00081"]:
                data[name].description = "accumulated over time from last report (reference is time bin end)"

        data["control_index"] = range(len(control))

        return cls(
            service_type=packets.service_type,
            service_subtype=packets.service_subtype,
            ssid=packets.ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            packets=packets,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L0" and service_type == 3 and service_subtype == 25 and ssid == 2

import numpy as np
from sunpy.time.timerange import TimeRange

from stixcore.ephemeris.manager import Spice
from stixcore.products.product import GenericProduct, L3Mixin
from stixcore.time.datetime import SCETime, SCETimeRange

__all__ = ["FlareListProduct", "FlareOverviewImage"]


class FlareListProduct(GenericProduct, L3Mixin):
    """Product not based on direct TM data but on time ranges defined in flare lists.

    In level 3 format.
    """

    @classmethod
    def from_timerange(cls, timerange: SCETimeRange, *, flarelistparent: str = ""):
        pass


class PeekPreviewImage(FlareListProduct):
    PRODUCT_PROCESSING_VERSION = 1
    Level = "L3"
    Type = "sci"
    Name = "peekpreviewimg"

    def __init__(self, control, data, energy, maps, parents, *, product_name_suffix="", **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=5, control=control,
                         data=data, energy=energy, **kwargs)
        self.name = f"{PeekPreviewImage.Name}-{product_name_suffix}"
        self.level = PeekPreviewImage.Level
        self.type = PeekPreviewImage.Type
        self.energy = energy
        self.maps = maps
        self.parents = parents

        self.add_additional_header_keyword(('NR_MAPS', len(maps) if maps else 0,
                                            'number of maps in file'))

    @property
    def parent(self):
        return np.atleast_1d(self.parents)

    @property
    def utc_timerange(self):
        return TimeRange(self.data["preview_start_UTC"][0], self.data["preview_end_UTC"][0])

    @property
    def scet_timerange(self):
        tr = self.utc_timerange
        start = SCETime.from_string(Spice.instance.datetime_to_scet(tr.start)[2:])
        end = SCETime.from_string(Spice.instance.datetime_to_scet(tr.end)[2:])
        return SCETimeRange(start=start, end=end)

    def split_to_files(self):
        return [self]

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == PeekPreviewImage.Level and service_type == 0
                and service_subtype == 0 and ssid == 5)

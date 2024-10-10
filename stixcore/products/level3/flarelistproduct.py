from stixcore.products.product import GenericProduct, L3Mixin
from stixcore.time.datetime import SCETimeRange


class FlareListProduct(GenericProduct, L3Mixin):
    """Product not based on direct TM data but on time ranges defined in flare lists.

    In level 3 format.
    """

    @classmethod
    def from_timerange(cls, timerange: SCETimeRange, *, flarelistparent: str = ''):
        pass


class FlareOverviewImage(FlareListProduct):
    PRODUCT_PROCESSING_VERSION = 1

    def __init__(self, *, service_type=0, service_subtype=0, ssid=2, **kwargs):
        self.name = 'overview'
        self.level = 'L3'
        self.type = 'img'
        self.ssid = ssid
        self.service_subtype = service_subtype
        self.service_type = service_type

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L3' and service_type == 0
                and service_subtype == 0 and ssid == 2)

from collections import defaultdict

from stixcore.products.level0.scienceL0 import ScienceProduct
from stixcore.products.product import L2Mixin
from stixcore.time import SCETimeRange

__all__ = ['Spectrogram']


class Spectrogram(ScienceProduct, L2Mixin):
    """
    X-ray Spectrogram data product.

    In level 2 format.
    """
    PRODUCT_PROCESSING_VERSION = 2

    def __init__(self, *, service_type, service_subtype, ssid, control,
                 data, idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = 'xray-spec'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 24)

    def get_additional_extensions(self):
        return [(self.data_corrected if (hasattr(self, "data_corrected") and
                                         len(self.data_corrected) > 0) else None,
                 "data_corrected")]

    @classmethod
    def from_level1(cls, l1product, parent='', idlprocessor=None):
        l2 = super().from_level1(l1product, parent=parent)[0]
        l2.data['test'] = 1

        # interpolate counts if RCR > 0 at any time point
        if l2.data['rcr'].sum() > 0:
            l2.data_corrected = l2.data[:]
        return [l2]

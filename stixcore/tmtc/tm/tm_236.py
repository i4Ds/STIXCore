"""Service 236 – STIX configuration."""
from stixcore.tmtc.packets import GenericTMPacket


class TM_236_16(GenericTMPacket):
    """TM(236, 16) Read STIX subsystem configuration report."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 16


class TM_236_19(GenericTMPacket):
    """TM(236, 19) FDIR parameter reported."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 19

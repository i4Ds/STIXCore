"""Service 6 – Memory management."""
from stixcore.tmtc.packets import GenericTMPacket


class TM_6_6(GenericTMPacket):
    """TM(6, 6) Memory Dump Report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 6 and dh.service_subtype == 6


class TM_6_10(GenericTMPacket):
    """TM(6, 10) Memory Check Report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 6 and dh.service_subtype == 10

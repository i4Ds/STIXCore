"""Service 17 – Connection Test."""
from stixcore.tmtc.packets import GenericTMPacket

__all__ = ['TM_17_2']


class TM_17_2(GenericTMPacket):
    """TM(17, 2) Connection Test Report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 17 and dh.service_subtype == 2

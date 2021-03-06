"""Service 237 – Parameters management."""
from stixcore.tmtc.packets import GenericTMPacket

__all__ = ['TM_237_12', 'TM_237_20']


class TM_237_12(GenericTMPacket):
    """TM(237, 12) SuSW Parameter report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 237 and dh.service_subtype == 12


class TM_237_20(GenericTMPacket):
    """TM(237, 20) ASW Parameter report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 237 and dh.service_subtype == 20

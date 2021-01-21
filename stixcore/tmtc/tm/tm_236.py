"""Service 236 – STIX configuration."""
from stixcore.tmtc.packets import GenericTMPacket

__all__ = ['TM_236_16', 'TM_236_19']


class TM_236_16(GenericTMPacket):
    """TM(236, 16) Read STIX subsystem configuration report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 16


class TM_236_19(GenericTMPacket):
    """TM(236, 19) FDIR parameter reported."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 19

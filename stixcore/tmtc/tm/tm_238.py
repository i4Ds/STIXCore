"""Service 238 – STIX Archive memory management."""

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ["TM_238_3", "TM_238_7"]


class TM_238_3(GenericTMPacket):
    """TM(238, 3) User data selections report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 238 and dh.service_subtype == 3


class TM_238_7(GenericTMPacket):
    """TM(238, 7) Filesystem check report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 238 and dh.service_subtype == 7

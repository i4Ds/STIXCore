"""Service 5 – Event reporting."""

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ["TM_5_1", "TM_5_2", "TM_5_3", "TM_5_4"]


class TM_5_1(GenericTMPacket):
    """TM(5, 1) Normal/Progress Report (info)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 1


class TM_5_2(GenericTMPacket):
    """TM(5, 2) Error/Anomaly Report – Low Severity (warning)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 2


class TM_5_3(GenericTMPacket):
    """TM(5, 3) Error/Anomaly Report – Medium Severity (ground action requested)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 3


class TM_5_4(GenericTMPacket):
    """TM(5, 4) Error/Anomaly Report – High Severity (on-board action requested)."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 4

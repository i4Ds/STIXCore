"""Service 3 – housekeeping telemetry report."""

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ["TM_3_25_1", "TM_3_25_2", "TM_3_25_4"]


class TM_3_25_1(GenericTMPacket):
    """TM(3, 25) SID 1: HK data “mini” report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and tm_packet.pi1_val == 1


class TM_3_25_2(GenericTMPacket):
    """TM(3, 25) SID 2: HK data "maxi” report."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and tm_packet.pi1_val == 2


class TM_3_25_4(GenericTMPacket):
    """TM(3, 25) SID 4: Instrument heartbeat."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and tm_packet.pi1_val == 4

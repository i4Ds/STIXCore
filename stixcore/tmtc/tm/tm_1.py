"""Service 1 – Telecommand Verification."""

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ["TM_1_1", "TM_1_2", "TM_1_7", "TM_1_8"]


class TM_1_1(GenericTMPacket):
    """TM(1, 1) Telecommand acceptance report – success."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 1


class TM_1_2(GenericTMPacket):
    """TM(1, 2) Telecommand acceptance report – failure."""

    def __init__(self, data):
        super().__init__(data)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 2


class TM_1_7(GenericTMPacket):
    """TM(1, 7) Telecommand execution completed report – success."""

    def __init__(self, data):
        super().__init__(data)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 7


class TM_1_8(GenericTMPacket):
    """TM(1, 8) Telecommand execution completed report – failure."""

    def __init__(self, data):
        super().__init__(data)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 8

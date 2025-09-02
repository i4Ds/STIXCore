"""Service 239 – STIX testing and debugging."""

from stixcore.tmtc.packets import GenericTMPacket

__all__ = ["TM_239_3", "TM_239_6", "TM_239_8", "TM_239_10", "TM_239_12", "TM_239_14", "TM_239_18", "TM_239_21"]


class TM_239_3(GenericTMPacket):
    """TM(239, 3) Report ASIC temperature."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 3


class TM_239_6(GenericTMPacket):
    """TM(239, 6) Report on-demand ASIC ADC readout."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 6


class TM_239_8(GenericTMPacket):
    """TM(239, 8) Report ASIC baseline."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 8


class TM_239_10(GenericTMPacket):
    """TM(239, 10) Report channel dark current."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 10


class TM_239_12(GenericTMPacket):
    """TM(239, 12) Report threshold scan."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 12


class TM_239_14(GenericTMPacket):
    """TM(239, 14) Report ASIC ADC read."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 14


class TM_239_18(GenericTMPacket):
    """TM(239, 18) Report ASIC register read."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 18


class TM_239_21(GenericTMPacket):
    """TM(239, 21) Report stored attenuator data."""

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 21

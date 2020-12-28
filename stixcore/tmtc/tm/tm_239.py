from stixcore.tmtc.packets import GenericTMPacket


class TM_239_3(GenericTMPacket):
    """
    TM(239, 3) Service 239 – STIX testing and debugging

    Report ASIC temperature
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 3


class TM_239_6(GenericTMPacket):
    """
    TM(239, 6) Service 239 – STIX testing and debugging

    Report on-demand ASIC ADC readout
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 6


class TM_239_8(GenericTMPacket):
    """
    TM(239, 8) Service 239 – STIX testing and debugging

    Report ASIC baseline
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 8


class TM_239_10(GenericTMPacket):
    """
    TM(239, 10) Service 239 – STIX testing and debugging

    Report channel dark current
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 10


class TM_239_12(GenericTMPacket):
    """
    TM(239, 12) Service 239 – STIX testing and debugging

    Report threshold scan
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 12


class TM_239_14(GenericTMPacket):
    """
    TM(239, 14) Service 239 – STIX testing and debugging

    Report ASIC ADC read
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 14


class TM_239_18(GenericTMPacket):
    """
    TM(239, 18) Service 239 – STIX testing and debugging

    Report ASIC register read
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 18


class TM_239_21(GenericTMPacket):
    """
    TM(239, 21) Service 239 – STIX testing and debugging

    Report stored attenuator data
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 239 and dh.service_subtype == 21

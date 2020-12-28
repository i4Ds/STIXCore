from stixcore.tmtc.packets import GenericTMPacket


class TM_237_12(GenericTMPacket):
    """
    TM(237, 12) Service 237 – Parameters management

    SuSW Parameter report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 237 and dh.service_subtype == 12


class TM_237_20(GenericTMPacket):
    """
    TM(237, 20) Service 237 – Parameters management

    ASW Parameter report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 237 and dh.service_subtype == 20

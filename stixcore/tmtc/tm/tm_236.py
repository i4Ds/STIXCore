from stixcore.tmtc.packets import GenericTMPacket


class TM_236_16(GenericTMPacket):
    """
    TM(236, 16) Service 236 – STIX configuration

    Telecommand acceptance report – success
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 16


class TM_236_19(GenericTMPacket):
    """
    TM(236, 19) Service 236 – STIX configuration

    FDIR parameter reported
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 236 and dh.service_subtype == 19

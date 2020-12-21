from stixcore.tmtc.packets import GenericTMPacket


class TM_1_1(GenericTMPacket):
    """
    TM(1, 1) Service 1 – Telecommand Verification.

    Telecommand acceptance report – success
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 1


class TM_1_2(GenericTMPacket):
    """
    TM(1, 2) Service 1 – Telecommand Verification.

    Telecommand acceptance report – failure
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 2


class TM_1_7(GenericTMPacket):
    """
    TM(1, 7) Service 1 – Telecommand Verification.

    Telecommand execution completed report – success
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 7


class TM_1_8(GenericTMPacket):
    """
    TM(1, 8) Service 1 – Telecommand Verification.

    Telecommand execution completed report – failure
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 1 and dh.service_subtype == 8

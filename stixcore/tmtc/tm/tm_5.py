from stixcore.tmtc.packets import GenericTMPacket


class TM_5_1(GenericTMPacket):
    """
    TM(5, 1) Service 5 – Event reporting.

    Normal/Progress Report (info)
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 1


class TM_5_2(GenericTMPacket):
    """
    TM(5, 2) Service 5 – Event reporting.

    Error/Anomaly Report – Low Severity (warning)
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 2


class TM_5_3(GenericTMPacket):
    """
    TM(5, 3) Service 5 – Event reporting.

    Error/Anomaly Report – Medium Severity (ground action requested)
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 3


class TM_5_4(GenericTMPacket):
    """
    TM(5, 4) Service 5 – Event reporting.

    Error/Anomaly Report – High Severity (on-board action requested)
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 5 and dh.service_subtype == 4

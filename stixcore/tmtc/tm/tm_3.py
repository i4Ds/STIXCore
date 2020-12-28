from stixcore.tmtc.packets import GenericTMPacket


class TM_3_25_1(GenericTMPacket):
    """
    TM(3, 25) Service 3 – housekeeping telemetry report

    SID 1: HK data “mini” report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and dh.pi1_val == 1


class TM_3_25_2(GenericTMPacket):
    """
    TM(3, 25) Service 3 – housekeeping telemetry report

    SID 2: HK data "maxi” report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and dh.pi1_val == 2


class TM_3_25_4(GenericTMPacket):
    """
    TM(3, 25) Service 3 – housekeeping telemetry report

    SID 4: Instrument heartbeat
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 3 and dh.service_subtype == 25 and dh.pi1_val == 4

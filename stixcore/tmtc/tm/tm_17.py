from stixcore.tmtc.packets import GenericTMPacket


class TM_17_2(GenericTMPacket):
    """
    TM(17, 2) Service 17 – Connection Test

    Connection Test Report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 17 and dh.service_subtype == 2

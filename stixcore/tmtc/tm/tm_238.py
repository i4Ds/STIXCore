from stixcore.tmtc.packets import GenericTMPacket


class TM_238_3(GenericTMPacket):
    """
    TM(238, 3) Service 238 – STIX Archive memory management

    User data selections report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 238 and dh.service_subtype == 3


class TM_238_7(GenericTMPacket):
    """
    TM(238, 7) Service 238 – STIX Archive memory management

    Filesystem check report
    """

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 238 and dh.service_subtype == 7

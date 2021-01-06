"""Service 22 – Context Saving."""
from stixcore.tmtc.packets import GenericTMPacket


class TM_22_2(GenericTMPacket):
    """TM(22, 2) Context Report from User."""

    def __init__(self, data, idb):
        super().__init__(data, idb)

    @classmethod
    def is_datasource_for(cls, tm_packet):
        dh = tm_packet.data_header
        return dh.service_type == 22 and dh.service_subtype == 2

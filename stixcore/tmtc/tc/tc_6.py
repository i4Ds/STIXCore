"""Service 6 – Memory management."""
from stixcore.tmtc.packets import GenericTCPacket

__all__ = ['TC_6_2']


class TC_6_2(GenericTCPacket):
    """TC(6, 2) Load Memory"""

    @classmethod
    def is_datasource_for(cls, packet):
        dh = packet.data_header
        return dh.service_type == 6 and dh.service_subtype == 2

    @property
    def unstacknix(self):
        return ['PIX00058']

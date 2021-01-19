from pathlib import Path

from stixcore.data import test
from stixcore.idb.manager import IDBManager
from stixcore.tmtc.packet_factory import TMTCPacketFactory
from stixcore.tmtc.packets import GenericPacket
from stixcore.tmtc.tm import *

__all__ = ['Packet']

idbm = IDBManager(Path(__file__).parent.parent / "idb" / "tests" / "data")

GenericPacket.idb_manager = idbm
Packet = TMTCPacketFactory(registry=GenericPacket._registry)

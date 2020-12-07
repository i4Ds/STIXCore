from pathlib import Path

import bitstring
import pytest

from stixcore.idb.manager import IDBManager
from stixcore.tmtc.packets import (
    SOURCE_PACKET_HEADER_STRUCTURE,
    TC_DATA_HEADER_STRUCTURE,
    TM_1_1,
    TM_DATA_HEADER_STRUCTURE,
    SourcePacketHeader,
    TCPacket,
    TMDataHeader,
    TMPacket,
)


@pytest.fixture
def idb():
    return IDBManager(Path(__file__).parent.parent.parent / "idb" / "tests" / "data") \
        .get_idb("2.26.34")

@pytest.mark.parametrize('class_header', [(SourcePacketHeader, SOURCE_PACKET_HEADER_STRUCTURE),
                                          (TMDataHeader, TM_DATA_HEADER_STRUCTURE)])
def test_tmtc_headers(class_header, idb):
    cls, header = class_header
    test_fmt = ', '.join(header.values())
    test_values = {n: 2**int(v.split(':')[-1])-1 for n, v in header.items()}
    test_binary = bitstring.pack(test_fmt, *test_values.values())
    sph = cls(test_binary, idb)
    assert all([getattr(sph, key) == test_values[key]
                for key in header.keys() if not key.startswith('spare')])


def test_tm_packet(idb):
    combind_structures = {**SOURCE_PACKET_HEADER_STRUCTURE, **TM_DATA_HEADER_STRUCTURE}
    test_fmt = ', '.join(combind_structures.values())
    test_values = {n: 2 ** int(v.split(':')[-1]) - 1 for n, v in
                   combind_structures.items()}
    test_binary = bitstring.pack(test_fmt, *test_values.values())
    tmtc_packet = TMPacket(test_binary, idb)
    assert all([getattr(tmtc_packet.source_packet_header, key) == test_values[key]
                for key in SOURCE_PACKET_HEADER_STRUCTURE.keys() if not key.startswith('spare')])
    assert all([getattr(tmtc_packet.data_header, key) == test_values[key]
                for key in TM_DATA_HEADER_STRUCTURE.keys() if not key.startswith('spare')])


def test_tc_packet(idb):
    combind_structures = {**SOURCE_PACKET_HEADER_STRUCTURE, **TC_DATA_HEADER_STRUCTURE}
    test_fmt = ', '.join(combind_structures.values())
    test_values = {n: 2 ** int(v.split(':')[-1]) - 1 for n, v in
                   combind_structures.items()}
    test_values['process_id'] = 90
    test_values['packet_category'] = 12
    test_binary = bitstring.pack(test_fmt, *test_values.values())
    tmtc_packet = TCPacket(test_binary, idb)
    assert all([getattr(tmtc_packet.source_packet_header, key) == test_values[key]
                for key in SOURCE_PACKET_HEADER_STRUCTURE.keys() if not key.startswith('spare')])
    assert all([getattr(tmtc_packet.data_header, key) == test_values[key]
                for key in TC_DATA_HEADER_STRUCTURE.keys() if not key.startswith('spare')])


def test_tm_1_1(idb):
    packet = TM_1_1('0x0da1c066000d100101782628a9c4e71e1dacc0a0', idb)
    assert packet.source_packet_header.process_id == 90
    assert packet.source_packet_header.packet_category == 1
    assert packet.data_header.service_type == 1
    assert packet.data_header.service_subtype == 1

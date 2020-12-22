import re
import binascii
from pathlib import Path

import bitstring
import pytest

import stixcore.tmtc.tm.tm_1 as tm_1
import stixcore.tmtc.tm.tm_3 as tm_3
import stixcore.tmtc.tm.tm_5 as tm_5
import stixcore.tmtc.tm.tm_21 as tm_21
import stixcore.tmtc.tm.tm_236 as tm_236
from stixcore.idb.manager import IDBManager
from stixcore.tmtc.packet_factory import BaseFactory, MultipleMatchError, NoMatchError, Packet
from stixcore.tmtc.packets import SOURCE_PACKET_HEADER_STRUCTURE, TM_DATA_HEADER_STRUCTURE


@pytest.fixture
def idbm():
    return IDBManager(Path(__file__).parent.parent.parent / "idb" / "tests" / "data")


@pytest.fixture
def idb():
    return IDBManager(Path(__file__).parent.parent.parent / "idb" / "tests" / "data") \
        .get_idb("2.26.34")


@pytest.fixture()
def data_dir():
    return Path(__file__).parent / 'data'


def _get_bin_from_file(data_dir, filename):
    with (data_dir / filename).open() as file:
        hex = file.read()
        hex_string = re.sub(r"\s+", "", hex)
        bin = binascii.unhexlify(hex_string)
        return bin


@pytest.mark.skip(reason="TODO: adapt later on")
def test_base_factory(idbm):
    class Dummy1(int):

        @classmethod
        def validate_function(cls, data):
            return data in [1, 2]

    class Dummy2(int):
        @classmethod
        def validate_function(cls, data):
            return data == 2

    factory = BaseFactory()

    with pytest.raises(AttributeError) as e:
        factory.register(Dummy1, 'test')
    assert str(e.value).startswith('Keyword argument')

    with pytest.raises(NoMatchError):
        factory(1, idbm)

    factory.register(Dummy1, Dummy1.validate_function)
    factory.register(Dummy2, Dummy2.validate_function)

    with pytest.raises(MultipleMatchError):
        factory(2, idbm)

    res = factory(1, idbm)
    assert res == 1
    assert isinstance(res, Dummy1)

    factory.unregister(Dummy1)
    res = factory(2, idbm)
    assert res == 2


@pytest.mark.skip(reason="TODO: Add back TM1")
def test_tm_packet(idbm):
    source_structure = {**SOURCE_PACKET_HEADER_STRUCTURE, **TM_DATA_HEADER_STRUCTURE}
    test_fmt = ', '.join([*source_structure.values(), 'uint:16', 'uint:16'])
    test_values = {n: 2 ** int(v.split(':')[-1]) - 1 for n, v in
                   source_structure.items()}
    test_values['service_type'] = 1
    test_values['service_subtype'] = 1
    test_values['v1'] = 1
    test_values['v2'] = 2
    test_binary = bitstring.pack(test_fmt, *test_values.values())
    packet = Packet(test_binary, idbm)
    assert isinstance(packet, tm_1.TM_1_1)
    assert all([getattr(packet.source_packet_header, key) == test_values[key]
                for key in SOURCE_PACKET_HEADER_STRUCTURE.keys()])
    assert all([getattr(packet.data_header, key) == test_values[key]
                for key in TM_DATA_HEADER_STRUCTURE.keys() if not key.startswith('spare')])
    assert packet.data.NIX00001 == 1
    assert packet.data.NIX00002 == 2


@pytest.mark.skip(reason="TODO: Add back TM1")
def test_tm_1_1(data_dir, idbm):
    hex = _get_bin_from_file(data_dir, '1_1.hex')
    packet = Packet(hex, idbm)
    assert isinstance(packet, tm_1.TM_1_1)
    assert packet.data_header.service_type == 1
    assert packet.data_header.service_subtype == 1
    assert packet.data_header.pi1_val is None

    assert packet.data.NIX00001 is not None
    assert packet.data.NIX00002 is not None


def test_tm_21_6_30(data_dir, idbm):
    hex = _get_bin_from_file(data_dir, '21_6_30.hex')
    packet = Packet(hex, idbm)
    assert isinstance(packet, tm_21.TM_21_6_30)
    assert packet.data_header.service_type == 21
    assert packet.data_header.service_subtype == 6
    assert packet.data_header.pi1_val == 30

    assert packet.data.NIX00120 is not None
    assert packet.data.NIX00405 is not None
    assert packet.data.NIXD0407 is not None


def test_tm_21_6_30_idb(data_dir, idb):
    hex = _get_bin_from_file(data_dir, '21_6_30.hex')
    packet = Packet(hex, idb)
    assert isinstance(packet, tm_21.TM_21_6_30)
    assert packet.data_header.service_type == 21
    assert packet.data_header.service_subtype == 6
    assert packet.data_header.pi1_val == 30

    assert packet.data.NIX00120 is not None
    assert packet.data.NIX00405 is not None
    assert packet.data.NIXD0407 is not None


@pytest.mark.parametrize('packtes', [
    (1,   1, None, tm_1.TM_1_1, True),
    (1,   2, 48000, tm_1.TM_1_2, True),
    (1,   7, None, tm_1.TM_1_7, True),
    (1,   8, 48452, tm_1.TM_1_8, True),

    (3,   25,   1, tm_3.TM_3_25_1, True),
    (3,   25,   2, tm_3.TM_3_25_2, True),

    (5,   1,   33, tm_5.TM_5_1, True),
    (5,   2,   21548, tm_5.TM_5_2, True),
    (5,   3,   32816, tm_5.TM_5_3, True),
    (5,   4,   54304, tm_5.TM_5_4, True),

    (21,   6,   20, tm_21.TM_21_6_20, True),
    (21,   6,   21, tm_21.TM_21_6_21, True),
    (21,   6,   22, tm_21.TM_21_6_22, True),
    (21,   6,   23, tm_21.TM_21_6_23, True),
    (21,   6,   24, tm_21.TM_21_6_24, True),
    (21,   6,   30, tm_21.TM_21_6_30, True),
    (21,   6,   31, tm_21.TM_21_6_31, True),
    (21,   6,   32, tm_21.TM_21_6_32, True),
    (21,   6,   33, tm_21.TM_21_6_33, True),
    (21,   6,   34, tm_21.TM_21_6_34, True),
    (21,   6,   41, tm_21.TM_21_6_41, True),
    (21,   6,   42, tm_21.TM_21_6_42, True),
    (21,   6,   43, tm_21.TM_21_6_43, False),

    (236,   16,   None, tm_236.TM_236_16, True),
    (236,   19,   None, tm_236.TM_236_19, True)
], ids=("TM_1_1",
        "TM_1_2",
        "TM_1_7",
        "TM_1_8",

        "TM_3_25_1",
        "TM_3_25_2",

        "TM_5_1",
        "TM_5_2",
        "TM_5_3",
        "TM_5_4",

        "TM_21_6_20",
        "TM_21_6_21",
        "TM_21_6_22",
        "TM_21_6_23",
        "TM_21_6_24",
        "TM_21_6_30",
        "TM_21_6_31",
        "TM_21_6_32",
        "TM_21_6_33",
        "TM_21_6_34",
        "TM_21_6_41",
        "TM_21_6_42",
        "TM_21_6_43",

        "TM_236_16",
        "TM_236_19"))
def test_all_tm(data_dir, idbm, packtes):
    t, st, pi1, cl, testpadding = packtes
    filename = f"{t}_{st}.hex" if pi1 is None else f"{t}_{st}_{pi1}.hex"
    hex = _get_bin_from_file(data_dir, filename)
    packet = Packet(hex, idbm)
    assert isinstance(packet, cl)
    assert packet.data_header.service_type == t
    assert packet.data_header.service_subtype == st
    assert packet.data_header.pi1_val == pi1
    # was all data consumed
    if testpadding:
        assert packet.source_packet_header.bitstream.pos == \
            len(packet.source_packet_header.bitstream)

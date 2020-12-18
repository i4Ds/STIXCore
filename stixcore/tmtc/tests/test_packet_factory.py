from pathlib import Path

import bitstring
import pytest

from stixcore.idb.manager import IDBManager
from stixcore.tmtc.packet_factory import BaseFactory, MultipleMatchError, NoMatchError, Packet
from stixcore.tmtc.packets import (
    SOURCE_PACKET_HEADER_STRUCTURE,
    TM_1_1,
    TM_21_6_30,
    TM_DATA_HEADER_STRUCTURE,
)


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
    assert isinstance(packet, TM_1_1)
    assert all([getattr(packet.source_packet_header, key) == test_values[key]
                for key in SOURCE_PACKET_HEADER_STRUCTURE.keys()])
    assert all([getattr(packet.data_header, key) == test_values[key]
                for key in TM_DATA_HEADER_STRUCTURE.keys() if not key.startswith('spare')])
    assert packet.data.NIX00001 == 1
    assert packet.data.NIX00002 == 2


def test_tm_1_1(data_dir, idbm):
    with (data_dir / 'tm_1_1.hex').open() as file:
        hex = file.read()
    packet = Packet(hex, idbm)
    assert isinstance(packet, TM_1_1)
    assert packet.service_type == 1
    assert packet.service_subtype == 1
    assert packet.data.NIX00001 == 7596
    assert packet.data.NIX00002 == 49312


def test_tm_21_6_30(data_dir, idbm):
    with (data_dir / 'tm_21_6_30.hex').open() as file:
        hex = file.read()
    packet = Packet(hex, idbm)
    assert isinstance(packet, TM_21_6_30)
    assert packet.service_type == 21
    assert packet.service_subtype == 6

    assert packet.data.NIX00120 == 30
    assert packet.data.NIX00405 == 39
    assert packet.data.NIXD0407 == 4095


def test_tm_21_6_30_idb(data_dir, idb):
    with (data_dir / 'tm_21_6_30.hex').open() as file:
        hex = file.read()
    packet = Packet(hex, idb)
    assert isinstance(packet, TM_21_6_30)
    assert packet.service_type == 21
    assert packet.service_subtype == 6

    assert packet.data.NIX00120 == 30
    assert packet.data.NIX00405 == 39
    assert packet.data.NIXD0407 == 4095

from pathlib import Path

import bitstring
import pytest

from stixcore.tmtc.packet_factory import BaseFactory, MultipleMatchError, NoMatchError, Packet
from stixcore.tmtc.packets import (
    SOURCE_PACKET_HEADER_STRUCTURE,
    TM_1_1,
    TM_21_6_30,
    TM_DATA_HEADER_STRUCTURE,
)


@pytest.fixture()
def data_dir():
    return Path(__file__).parent / 'data'


def test_base_factory():
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
        factory(1)

    factory.register(Dummy1, Dummy1.validate_function)
    factory.register(Dummy2, Dummy2.validate_function)

    with pytest.raises(MultipleMatchError):
        factory(2)

    res = factory(1)
    assert res == 1
    assert isinstance(res, Dummy1)

    factory.unregister(Dummy1)
    res = factory(2)
    assert res == 2


def test_tm_packet():
    source_structure = {**SOURCE_PACKET_HEADER_STRUCTURE, **TM_DATA_HEADER_STRUCTURE}
    test_fmt = ', '.join([*source_structure.values(), 'uint:16', 'uint:16'])
    test_values = {n: 2 ** int(v.split(':')[-1]) - 1 for n, v in
                   source_structure.items()}
    test_values['service_type'] = 1
    test_values['service_subtype'] = 1
    test_values['v1'] = 1
    test_values['v2'] = 2
    test_binary = bitstring.pack(test_fmt, *test_values.values())
    packet = Packet(test_binary)
    assert isinstance(packet, TM_1_1)
    assert all([getattr(packet.source_packet_header, key) == test_values[key]
                for key in SOURCE_PACKET_HEADER_STRUCTURE.keys()])
    assert all([getattr(packet.data_header, key) == test_values[key]
                for key in TM_DATA_HEADER_STRUCTURE.keys() if not key.startswith('spare')])
    assert packet.NIX00001 == 1
    assert packet.NIX00002 == 2


# def test_tc_packet():
#     source_structure = {**SOURCE_PACKET_HEADER_STRUCTURE, **TM_DATA_HEADER_STRUCTURE}
#     test_fmt = ', '.join(source_structure.values())
#     test_values = {n: 2 ** int(v.split(':')[-1]) - 1 for n, v in
#                    source_structure.items()}
#     test_values['packet_category'] = 12
#     test_binary = bitstring.pack(test_fmt, *test_values.values())
#     packet = Packet(test_binary)
#     assert isinstance(packet, TCPacket)
#     assert all([getattr(packet.source_packet_header, key) == test_values[key]
#                 for key in SOURCE_PACKET_HEADER_STRUCTURE.keys()])


def test_tm_1_1(data_dir):
    with (data_dir / 'tm_1_1.hex').open() as file:
        hex = file.read()
    packet = Packet(hex)
    assert isinstance(packet, TM_1_1)


def test_tm_21_6_30(data_dir):
    with (data_dir / 'tm_21_6_30.hex').open() as file:
        hex = file.read()
    packet = Packet(hex)
    assert isinstance(packet, TM_21_6_30)

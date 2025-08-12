import re
import glob
import binascii
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import bitstring
import pytest

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.processing.decompression import decompress
from stixcore.processing.engineering import raw_to_engineering
from stixcore.time.datetime import SCETime
from stixcore.tmtc import Packet
from stixcore.tmtc.packet_factory import (
    BaseFactory,
    MultipleMatchError,
    NoMatchError,
    TMTCPacketFactory,
)
from stixcore.tmtc.packets import (
    SOURCE_PACKET_HEADER_STRUCTURE,
    TM_DATA_HEADER_STRUCTURE,
    GenericPacket,
)
from stixcore.tmtc.parameter import CompressedParameter, EngineeringParameter
from stixcore.tmtc.tm import tm_1, tm_3, tm_5, tm_6, tm_17, tm_21, tm_236, tm_237, tm_238, tm_239


@pytest.fixture
def idbm():
    return IDBManager(test_data.idb.DIR)


@pytest.fixture
def idb():
    return IDBManager(test_data.idb.DIR).get_idb("2.26.34")


@pytest.fixture
def data_dir():
    return test_data.tmtc.TM_DIR


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
        factory.register(Dummy1, "test")
    assert str(e.value).startswith("Keyword argument")

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


@pytest.mark.skip(reason="TODO: Add back TM1")
def test_tm_packet(idbm):
    source_structure = {**SOURCE_PACKET_HEADER_STRUCTURE, **TM_DATA_HEADER_STRUCTURE}
    test_fmt = ", ".join([*source_structure.values(), "uint:16", "uint:16"])
    test_values = {n: 2 ** int(v.split(":")[-1]) - 1 for n, v in source_structure.items()}
    test_values["service_type"] = 1
    test_values["service_subtype"] = 1
    test_values["v1"] = 1
    test_values["v2"] = 2
    test_binary = bitstring.pack(test_fmt, *test_values.values())

    Packet = TMTCPacketFactory(registry=GenericPacket._registry, idbm=idbm)

    packet = Packet(test_binary)
    assert isinstance(packet, tm_1.TM_1_1)
    assert all(
        [getattr(packet.source_packet_header, key) == test_values[key] for key in SOURCE_PACKET_HEADER_STRUCTURE.keys()]
    )
    assert all(
        [
            getattr(packet.data_header, key) == test_values[key]
            for key in TM_DATA_HEADER_STRUCTURE.keys()
            if not key.startswith("spare")
        ]
    )
    assert packet.data.NIX00001 == 1
    assert packet.data.NIX00002 == 2


@pytest.mark.skip(reason="TODO: Add back TM1")
def test_tm_1_1(data_dir, idbm):
    hex = _get_bin_from_file(data_dir, "1_1.hex")
    Packet = TMTCPacketFactory(registry=GenericPacket._registry, idbm=idbm)
    packet = Packet(hex)
    assert isinstance(packet, tm_1.TM_1_1)
    assert packet.data_header.service_type == 1
    assert packet.data_header.service_subtype == 1
    assert packet.data_header.pi1_val is None
    assert packet.data.NIX00001 is not None
    assert packet.data.NIX00002 is not None


def test_tm_21_6_30(data_dir, idbm):
    hex = _get_bin_from_file(data_dir, "21_6_30.hex")
    IDBManager.instance = idbm
    Packet = TMTCPacketFactory(registry=GenericPacket._registry)
    packet = Packet(hex)
    assert isinstance(packet, tm_21.TM_21_6_30)
    assert packet.data_header.service_type == 21
    assert packet.data_header.service_subtype == 6
    assert packet.pi1_val == 30

    assert packet.data.NIX00120 is not None
    assert packet.data.NIX00405 is not None
    assert packet.data.NIXD0407 is not None


common_args = (
    "packets",
    [
        (1, 1, None, tm_1.TM_1_1, True, 1),
        (1, 2, 48000, tm_1.TM_1_2, True, 1),
        (1, 7, None, tm_1.TM_1_7, True, 1),
        (1, 8, 48452, tm_1.TM_1_8, True, 1),
        (3, 25, 1, tm_3.TM_3_25_1, True, 1),
        (3, 25, 2, tm_3.TM_3_25_2, True, 1),
        (5, 1, 33, tm_5.TM_5_1, True, 1),
        (5, 2, 21548, tm_5.TM_5_2, True, 1),
        (5, 3, 32816, tm_5.TM_5_3, True, 1),
        (5, 4, 54304, tm_5.TM_5_4, True, 1),
        (6, 6, 53250, tm_6.TM_6_6, True, 1),
        (6, 10, None, tm_6.TM_6_10, True, 1),
        (17, 2, None, tm_17.TM_17_2, True, 1),
        (21, 6, 20, tm_21.TM_21_6_20, True, 1),
        (21, 6, 20, tm_21.TM_21_6_20, True, 2),
        (21, 6, 21, tm_21.TM_21_6_21, True, 1),
        (21, 6, 21, tm_21.TM_21_6_21, True, 2),
        (21, 6, 22, tm_21.TM_21_6_22, True, 1),
        (21, 6, 22, tm_21.TM_21_6_22, True, 2),
        (21, 6, 23, tm_21.TM_21_6_23, True, 1),
        (21, 6, 23, tm_21.TM_21_6_23, True, 2),
        (21, 6, 24, tm_21.TM_21_6_24, True, 1),
        (21, 6, 24, tm_21.TM_21_6_24, True, 4),
        (21, 6, 30, tm_21.TM_21_6_30, True, 1),
        (21, 6, 31, tm_21.TM_21_6_31, True, 1),
        (21, 6, 32, tm_21.TM_21_6_32, True, 1),
        (21, 6, 33, tm_21.TM_21_6_33, True, 1),
        (21, 6, 34, tm_21.TM_21_6_34, True, 1),
        (21, 6, 41, tm_21.TM_21_6_41, True, 1),
        (21, 6, 42, tm_21.TM_21_6_42, True, 1),
        (21, 6, 43, tm_21.TM_21_6_43, False, 1),
        (236, 16, None, tm_236.TM_236_16, True, 1),
        (236, 19, None, tm_236.TM_236_19, True, 1),
        (237, 12, None, tm_237.TM_237_12, True, 1),
        (237, 20, None, tm_237.TM_237_20, True, 1),
        (238, 3, None, tm_238.TM_238_3, True, 1),
        (238, 7, None, tm_238.TM_238_7, True, 1),
        (239, 3, None, tm_239.TM_239_3, True, 1),
        (239, 6, None, tm_239.TM_239_6, True, 1),
        (239, 8, None, tm_239.TM_239_8, True, 1),
        (239, 10, None, tm_239.TM_239_10, True, 1),
        (239, 12, None, tm_239.TM_239_12, True, 1),
        (239, 14, None, tm_239.TM_239_14, True, 1),
        (239, 18, None, tm_239.TM_239_18, True, 1),
        (239, 21, None, tm_239.TM_239_21, False, 1),
        # TODO fix to full packet TM_239_21 read after fox of https://github.com/i4Ds/STIX-IDB/issues/16
    ],
)

packets_test_names = (
    "TM_1_1",
    "TM_1_2",
    "TM_1_7",
    "TM_1_8",
    "TM_3_25_1",
    "TM_3_25_2",
    "TM_5_1",
    "TM_5_2",
    "TM_5_3",
    "TM_5_4",
    "TM_6_6",
    "TM_6_10",
    "TM_17_2",
    "TM_21_6_20",
    "TM_21_6_20x2",
    "TM_21_6_21",
    "TM_21_6_21x2",
    "TM_21_6_22",
    "TM_21_6_22x2",
    "TM_21_6_23",
    "TM_21_6_23x2",
    "TM_21_6_24",
    "TM_21_6_24x4",
    "TM_21_6_30",
    "TM_21_6_31",
    "TM_21_6_32",
    "TM_21_6_33",
    "TM_21_6_34",
    "TM_21_6_41",
    "TM_21_6_42",
    "TM_21_6_43",
    "TM_236_16",
    "TM_236_19",
    "TM_237_12",
    "TM_237_20",
    "TM_238_3",
    "TM_238_7",
    "TM_239_3",
    "TM_239_6",
    "TM_239_8",
    "TM_239_10",
    "TM_239_12",
    "TM_239_14",
    "TM_239_18",
    "TM_239_21",
)


@pytest.mark.parametrize(*common_args, ids=packets_test_names)
def test_all_tm(data_dir, idbm, packets):
    t, st, pi1, cl, testpadding, nstr = packets
    n_str = "" if nstr <= 1 else f"_nstr_{nstr}"
    filename = f"{t}_{st}{n_str}.hex" if pi1 is None else f"{t}_{st}_{pi1}{n_str}.hex"

    hex = _get_bin_from_file(data_dir, filename)
    IDBManager.instance = idbm
    Packet = TMTCPacketFactory(registry=GenericPacket._registry)
    packet = Packet(hex)
    assert isinstance(packet, cl)
    assert packet.data_header.service_type == t
    assert packet.data_header.service_subtype == st
    assert packet.pi1_val == pi1
    # was all data consumed
    if testpadding:
        assert packet.source_packet_header.bitstream.pos == len(packet.source_packet_header.bitstream)


@pytest.mark.parametrize(*common_args, ids=packets_test_names)
def test_decompress(data_dir, idbm, packets):
    t, st, pi1, cl, test, nstr = packets

    # TODO enable test again after https://github.com/i4Ds/STIXCore/issues/40 resolved
    if cl == tm_21.TM_21_6_23:
        return

    n_str = "" if nstr <= 1 else f"_nstr_{nstr}"
    filename = f"{t}_{st}{n_str}.hex" if pi1 is None else f"{t}_{st}_{pi1}{n_str}.hex"
    hex = _get_bin_from_file(data_dir, filename)
    IDBManager.instance = idbm
    Packet = TMTCPacketFactory(registry=GenericPacket._registry)
    packet = Packet(hex)
    assert isinstance(packet, cl)
    c = decompress(packet)

    if nstr > 1:
        assert packet.data.NIX00403.value == nstr

    decompression_parameter = packet.get_decompression_parameter()
    if decompression_parameter is not None:
        assert c > 0
        for param_name, (sn, kn, mn) in decompression_parameter.items():
            params = packet.data.get(param_name)
            assert isinstance(params, CompressedParameter)
    else:
        assert c == 0


def test_decompress_l1_triggers(data_dir, idbm):
    hex = _get_bin_from_file(data_dir, "21_6_21_nstr_2.hex")
    IDBManager.instance = idbm
    Packet = TMTCPacketFactory(registry=GenericPacket._registry)
    packet = Packet(hex)

    packet.data_header.datetime = SCETime(640195650, 0)
    old_decompression_parameters = packet.get_decompression_parameter()
    assert list(set(old_decompression_parameters.values())) == [("NIXD0007", "NIXD0008", "NIXD0009")]

    packet.data_header.datetime = SCETime(677774251, 0)
    new_decompression_parameters = packet.get_decompression_parameter()
    assert old_decompression_parameters != new_decompression_parameters
    assert new_decompression_parameters["NIX00242"] == ("NIXD0010", "NIXD0011", "NIXD0012")


@pytest.mark.parametrize(*common_args, ids=packets_test_names)
def test_engineering(data_dir, idbm, packets):
    t, st, pi1, cl, test, nstr = packets

    n_str = "" if nstr <= 1 else f"_nstr_{nstr}"
    filename = f"{t}_{st}{n_str}.hex" if pi1 is None else f"{t}_{st}_{pi1}{n_str}.hex"

    hex = _get_bin_from_file(data_dir, filename)
    IDBManager.instance = idbm
    Packet = TMTCPacketFactory(registry=GenericPacket._registry)
    packet = Packet(hex)
    assert isinstance(packet, cl)

    c = raw_to_engineering(packet)

    if nstr > 1:
        assert packet.data.NIX00403.value == nstr

    _, e_parameter = packet.get_calibration_params()
    if len(e_parameter) > 0:
        assert c > 0
        for cparam in e_parameter:
            param = packet.data.get(cparam.PCF_NAME)
            if isinstance(param, list):
                for rep in param:
                    assert isinstance(rep, EngineeringParameter)
            else:
                isinstance(param, EngineeringParameter)
    else:
        assert c == 0


@pytest.mark.skip(reason="Broken on py3.6")
def test_parallel(data_dir, idbm):
    packet_data = []
    root = Path("")
    for filename in glob.glob(str(data_dir / "*.hex")):
        hex = _get_bin_from_file(root, filename)
        packet_data.append(hex)

    packets_count = 0
    with ProcessPoolExecutor() as exec:
        res = exec.map(Packet, packet_data)

    for r in res:
        packets_count += 1
        assert r.data_header.service_type > 0

    assert packets_count == len(packet_data)


# def test_parallel():
#     packet_data = []
#     tree = Et.parse('D:/CruisePhase_STP124_Part3_manual_BatchRequest.PktTmRaw.SOL.' +
#                     '0.2020.337.15.26.50.474.AuYP@2020.337.15.26.51.700.1.xml')
#     root = tree.getroot()
#     for i, node in enumerate(root.iter('Packet')):
#         packet_binary = unhexlify(node.text)
#         # Not sure why guess and extra moc header
#         packet_data.append(packet_binary[76:])
#
# #     start = time.time()
# #    for idx, pdhex in enumerate(packet_data):
# #        packet = Packet(pdhex)
# #    print(packet)
# #    end = time.time()
# #    print(end - start, i)
#
#     i = 0
#     start = time.time()
#     with ProcessPoolExecutor() as exec:
#         res = exec.map(Packet, packet_data)
#
#         for r in res:
#             i += 1
#     end = time.time()
#
#     print(end - start, i)
#     assert True
#
# def test_parallel():
#     packet_data = []
#     tree = Et.parse('D:/CruisePhase_STP124_Part3_manual_BatchRequest.PktTmRaw.SOL.' +
#                     '0.2020.337.15.26.50.474.AuYP@2020.337.15.26.51.700.1.xml')
#     root = tree.getroot()
#     for i, node in enumerate(root.iter('Packet')):
#         packet_binary = unhexlify(node.text)
#         # Not sure why guess and extra moc header
#         packet_data.append(packet_binary[76:])
#
# #     start = time.time()
# #    for idx, pdhex in enumerate(packet_data):
# #        packet = Packet(pdhex)
# #    print(packet)
# #    end = time.time()
# #    print(end - start, i)
#
#     i = 0
#     start = time.time()
#     with ProcessPoolExecutor() as exec:
#         res = exec.map(Packet, packet_data)
#
#         for r in res:
#             i += 1
#     end = time.time()
#
#     print(end - start, i)
#     assert True

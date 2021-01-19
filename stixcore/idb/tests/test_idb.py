import os
import sqlite3
from pathlib import Path

import pytest

from stixcore.idb.idb import IDB
from stixcore.idb.manager import IDBManager

VERSION = "2.26.34"


@pytest.fixture
def idb():
    return IDBManager(Path(os.path.abspath(__file__)).parent / 'data').get_idb(VERSION)


def test_idb_setup(idb):
    assert idb is not None
    assert idb.is_connected()
    assert idb.get_idb_version() == VERSION
    filename = idb.get_idb_filename()
    assert filename.endswith("idb.sqlite")
    idb.close()
    assert idb.is_connected() is False


def test_idb_setup_fails():
    with pytest.raises(sqlite3.Error) as e:
        _idb = IDB(Path(os.path.abspath(__file__)).parent / 'data')
        assert _idb.is_connected() is False
        _ = _idb.get_idb_version()
    assert len(str(e.value)) > 0


def test_get_spit(idb):
    spids = idb.get_all_spid()
    for spid, descr in spids:
        info = idb.get_spid_info(spid)
        telemetry_description = idb.get_telemetry_description(spid)
        assert len(info) == 1
        assert len(telemetry_description) <= 1

        PID_DESCR, PID_TYPE, PID_STYPE = info[0]
        assert PID_DESCR == descr


def test_get_scos_description(idb):
    info = idb.get_scos_description('NIX00354')
    assert info == "Quadrant identification (1..4)"
    # test twice for caching
    info = idb.get_scos_description('NIX00354')
    assert info != ""

    info = idb.get_scos_description('foobar')
    assert info == ""


def test_get_packet_pi1_val_position(idb):
    info = idb.get_packet_pi1_val_position(21, 6)
    assert info.PIC_PI1_OFF == 16
    assert info.PIC_PI1_WID == 8

    info = idb.get_packet_pi1_val_position('foo', 'bar')
    assert info is None


def test_pickle(idb):
    import pickle
    clone = pickle.loads(pickle.dumps(idb))
    assert idb is not clone
    assert idb.filename == clone.filename
    assert clone.is_connected()
    clone.close()


def test_get_parameter_description(idb):
    # a PCF param
    info = idb.get_parameter_description('NIX00354')
    assert info != ""
    # test twice for caching
    info = idb.get_parameter_description('NIX00354')
    assert info != ""

    # a CPC param
    info = idb.get_parameter_description('PIX00005')
    assert info != ""

    info = idb.get_parameter_description('foobar')
    assert info == ""


def test_get_parameter_unit(idb):
    # a PCF param
    info = idb.get_parameter_unit('NIX00102')
    assert info != ""
    # test twice for caching
    info = idb.get_parameter_unit('NIX00102')
    assert info != ""

    info = idb.get_parameter_unit('foobar')
    assert info == ""


def test_get_packet_type_info(idb):
    info = idb.get_packet_type_info(6, 10, None)
    assert info is not None

    info = idb.get_packet_type_info(6, 10, 0)
    assert info is not None

    info = idb.get_packet_type_info(6, 10, 1)
    assert info is None

    info = idb.get_packet_type_info(0, 0)
    assert info is None


def test_get_s2k_parameter_types(idb):
    info = idb.get_s2k_parameter_types(10, 13)
    assert info is not None
    # test twice for caching
    info = idb.get_s2k_parameter_types(10, 13)
    assert info is not None

    info = idb.get_s2k_parameter_types(11, 18)
    assert info is None


def test_convert_NIXG_NIXD(idb):
    info = idb.convert_NIXG_NIXD("foobar")
    assert len(info) == 0

    info = idb.convert_NIXG_NIXD("NIXG0011")
    assert len(info) >= 1


def test_get_fixed_packet_structure(idb):
    info = idb.get_fixed_packet_structure(123)
    assert len(info) == 0

    info = idb.get_fixed_packet_structure(54005)
    assert len(info) >= 1
    # test twice for caching
    info = idb.get_fixed_packet_structure(54005)
    assert len(info) >= 1


def test_get_telecommand_info(idb):
    info = idb.get_telecommand_info(6, 2)
    assert len(info) == 4

    info = idb.get_telecommand_info(6, 2, 1)
    assert len(info) == 4

    info = idb.get_telecommand_info(11, 11)
    assert info is None


def test_get_telecommand_structure(idb):
    info = idb.get_telecommand_structure("ZIX06009")
    assert len(info) >= 1

    info = idb.get_telecommand_structure("foobar")
    assert len(info) == 0


def test_is_variable_length_telecommand(idb):
    info = idb.is_variable_length_telecommand("ZIX06009")
    assert info is False

    info = idb.is_variable_length_telecommand("ZIX22003")
    assert info is True

    info = idb.is_variable_length_telecommand("foobar")
    assert info is False


def test_get_variable_packet_structure(idb):
    info = idb.get_variable_packet_structure(54118)
    assert len(info) >= 1

    # test twice for caching
    info = idb.get_variable_packet_structure(54118)
    assert len(info) >= 1

    info = idb.get_variable_packet_structure("foobar")
    assert len(info) == 0


def test_tcparam_interpret(idb):
    info = idb.tcparam_interpret('CAAT0005TC', 0)
    assert info != ''

    info = idb.tcparam_interpret('foobar', 0)
    assert info == ''


def test_get_calibration_curve(idb):
    info = idb.get_calibration_curve('CIXP0024TM')
    assert len(info) >= 1

    # test twice for caching
    info = idb.get_calibration_curve('CIXP0024TM')
    assert len(info) >= 1

    info = idb.get_calibration_curve('foobar')
    assert len(info) == 0


def test_get_textual_mapping(idb):
    info = idb.get_textual_mapping('NIX00013')
    assert len(info) == 2
    _idx = info[0]
    _str = info[1]
    assert len(_idx) == len(_str)
    assert isinstance(_idx[0], int)
    assert isinstance(_str[0], str)

    info = idb.get_textual_mapping('foobar')
    assert info is None


def test_textual_interpret(idb):
    info = idb.textual_interpret('CAAT0005TM', 0)
    assert len(info) >= 1

    # test twice for caching
    info = idb.textual_interpret('CAAT0005TM', 0)
    assert len(info) >= 1

    info = idb.textual_interpret('foobar', 1)
    assert len(info) == 0


def test_get_calibration_polynomial(idb):
    info = idb.get_calibration_polynomial('CIX00036TM')
    assert len(info) == 1
    assert len(info[0]) == 5

    # test twice for caching
    info = idb.get_calibration_polynomial('CIX00036TM')
    assert len(info) == 1
    assert len(info[0]) == 5

    info = idb.get_calibration_polynomial('foobar')
    assert len(info) == 0

import os
import sqlite3
from pathlib import Path

import pytest

from stixcore.data.test import test_data
from stixcore.idb.idb import (
    IDB,
    IDBCalibrationCurve,
    IDBCalibrationParameter,
    IDBPacketTree,
    IDBPolynomialCalibration,
    IDBTCInfo,
)
from stixcore.idb.manager import IDBManager

VERSION = "2.26.34"


@pytest.fixture
def idb():
    return IDBManager(test_data.idb.DIR).get_idb(VERSION)


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


def test_get_telecommand_info(idb):
    info = idb.get_telecommand_info(6, 2)

    assert isinstance(info, IDBTCInfo)

    info = idb.get_telecommand_info(11, 11)
    assert info is None


def test_get_telecommand_structure(idb):
    info = idb.get_telecommand_info(6, 9)

    assert info.is_variable() is False
    assert info.CCF_CNAME == 'ZIX06009'

    tree = idb.get_telecommand_structure(info.CCF_CNAME, isvar=info.is_variable())
    assert isinstance(tree, IDBPacketTree)
    assert len(tree._children) > 0

    info = idb.get_telecommand_structure("foobar", isvar=False)
    assert isinstance(tree, IDBPacketTree)
    assert len(tree._children) > 0


def test_is_variable_length_telecommand(idb):
    info = idb.is_variable_length_telecommand("ZIX06009")
    assert info is False

    info = idb.is_variable_length_telecommand("ZIX22003")
    assert info is True

    info = idb.is_variable_length_telecommand("foobar")
    assert info is False


def test_tcparam_interpret(idb):
    info = idb.tc_interpret('CAAT0005TC', 0)
    assert info == 'Disconnected'

    info = idb.tc_interpret('foobar', 0)
    assert info == 0


def test_get_calibration_curve(idb):
    dummy = {'PID_SPID': 'a', 'PID_DESCR': 'a', 'PID_TPSD': 'a', 'PCF_NAME': 'a', 'PCF_DESCR': 'a',
             'PCF_WIDTH': 'a', 'PCF_PFC': 'a', 'PCF_PTC': 'a', 'S2K_TYPE': 'a', 'PCF_CATEG': '',
             'PCF_UNIT': '', 'PCF_CURTX': 'CIXP0024TM'}

    p = IDBCalibrationParameter(**dummy)

    curve = idb.get_calibration_curve(p)
    assert isinstance(curve, IDBCalibrationCurve)
    for i, x in enumerate(curve.x):
        assert abs(curve(x) - curve.y[i]) < 0.001

    # test twice for caching
    curve = idb.get_calibration_curve(p)
    assert isinstance(curve, IDBCalibrationCurve)

    dummy['PCF_CURTX'] = 'f'
    dummy['PCF_CURTX'] = 'b'

    curve = idb.get_calibration_curve(IDBCalibrationParameter(**dummy))
    assert isinstance(curve, IDBCalibrationCurve)
    assert curve.valid is False


def test_textual_interpret(idb):
    info = idb.textual_interpret('CAAT0005TM', 0)
    assert info == 'Disconnected'

    # test twice for caching
    info = idb.textual_interpret('CAAT0005TM', 0)
    assert info == 'Disconnected'

    info = idb.textual_interpret('foobar', 1)
    assert (info is None) or (info == 1)


def test_get_calibration_polynomial(idb):
    poly = idb.get_calibration_polynomial('CIX00036TM')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly(1) == poly.A[1]
    assert poly.valid is True

    # test twice for caching
    poly = idb.get_calibration_polynomial('CIX00036TM')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly.valid is True

    poly = idb.get_calibration_polynomial('foobar')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly.valid is False

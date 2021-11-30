import re
import sys
from pathlib import Path
from binascii import unhexlify
from unittest.mock import patch

import numpy as np
import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.data.test import test_data
from stixcore.idb.idb import IDBPolynomialCalibration
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCManager
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products.product import Product
from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


@pytest.fixture
def soc_manager():
    return SOCManager(Path(__file__).parent.parent.parent / "data" / "test" / "io" / 'soc')


@pytest.fixture
def idb():
    return IDBManager(test_data.idb.DIR).get_idb("2.26.34")


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


@pytest.mark.xfail(sys.platform == "win32", reason="numpy defaults to int32 on windows")
def test_level_b(soc_manager, out_dir):
    files_to_process = list(soc_manager.get_files(TMTC.TM))
    res = process_tmtc_to_levelbinary(files_to_process=files_to_process[0:1], archive_path=out_dir)
    assert len(res) == 1
    fits = res.pop()
    diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                    ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
    if not diff.identical:
        print(diff.report())
    assert diff.identical


@pytest.mark.xfail(sys.platform == "win32", reason="numpy defaults to int32 on windows")
def test_level_0(out_dir):
    lb = test_data.products.LB_21_6_30_fits
    l0 = Level0(out_dir / 'LB', out_dir)
    res = l0.process_fits_files(files=[lb])
    assert len(res) == 2
    for fits in res:
        diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
        if not diff.identical:
            print(diff.report())
        assert diff.identical


@pytest.mark.xfail(sys.platform == "win32", reason="numpy defaults to int32 on windows")
def test_level_1(out_dir):
    l0 = test_data.products.L0_LightCurve_fits
    l1 = Level1(out_dir / 'LB', out_dir)
    res = sorted(l1.process_fits_files(files=l0))
    assert len(res) == 2

    # test for https://github.com/i4Ds/STIXCore/issues/180
    # TODO remove when solved
    lc1 = Product(res[0])
    lc2 = Product(res[1])
    t = np.hstack((np.array(lc1.data['time']), (np.array(lc2.data['time']))))
    td = np.hstack((np.array(lc1.data['timedel']), (np.array(lc2.data['timedel']))))
    range(len(lc1.data['time'])-3, len(lc1.data['time'])+3)
    assert np.all((t[1:] - t[0:-1]) == td[0:-1])
    # end test for https://github.com/i4Ds/STIXCore/issues/180

    for fits in res:
        diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
        if not diff.identical:
            print(diff.report())
        assert diff.identical


def test_get_calibration_polynomial(idb):
    poly = idb.get_calibration_polynomial('CIX00036TM')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly(1) == poly.A[1]
    assert poly.valid is True

    assert (poly(np.array([1, 2, 3])) == np.array([poly.A[1], poly.A[1] * 2, poly.A[1] * 3])).all()
    assert poly([1, 2, 3]) == [poly.A[1], poly.A[1] * 2, poly.A[1] * 3]


@patch('stixcore.io.soc.manager.SOCPacketFile')
def test_pipeline(socpacketfile, out_dir):

    l0_proc = Level0(out_dir / 'LB', out_dir)
    l1_proc = Level1(out_dir / 'LB', out_dir)

    all = True
    report = dict()

    exclude = ['TM_21_6_20', '__doc__', 'TM_DIR', 'TM_1_2_48000']

    for pid, fkey in enumerate([k for k in test_data.tmtc.__dict__.keys()
                                if k not in exclude]):
        # for pid, fkey in enumerate([k for k in test_data.tmtc.__dict__.keys()
        #                             if k in singletest]):
        hex_file = test_data.tmtc.__dict__[fkey]

        try:
            with hex_file.open('r') as file:
                hex = file.readlines()

            socpacketfile.get_packet_binaries.return_value = list(
                [(pid*1000 + i, unhexlify(re.sub(r"\s+", "", h))) for i, h in enumerate(hex)])

            lb_files = process_tmtc_to_levelbinary([socpacketfile], archive_path=out_dir)
            assert len(lb_files) > 0

            l0_files = l0_proc.process_fits_files(files=lb_files)
            assert len(l0_files) > 0

            l1_files = l1_proc.process_fits_files(files=l0_files)
            assert len(l1_files) > 0

            print(f"OK {fkey}: {l1_files}")
        except Exception as e:
            report[fkey] = e
            all = False

    if not all:
        for key, error in report.items():
            logger.error(f"Error while processing TM file: {key}")
            logger.error(error)
        raise ValueError("Pipline Test went wrong")

from pathlib import Path

import numpy as np
import pytest

from stixcore.data.test import test_data
from stixcore.idb.idb import IDBPolynomialCalibration
from stixcore.idb.manager import IDBManager
from stixcore.io.fits.processors import FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.products.levelb.binary import LevelB
from stixcore.tmtc.packets import TMTC


@pytest.fixture
def soc_manager():
    return SOCManager(Path(__file__).parent.parent.parent / "data" / "test" / "io" / 'soc')


@pytest.fixture
def idb():
    return IDBManager(test_data.idb.DIR).get_idb("2.26.34")


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


def test_level_b(soc_manager, out_dir):
    fits_processor = FitsLBProcessor(out_dir)

    files_to_process = soc_manager.get_files(TMTC.TM)
    for tmtc_file in files_to_process:
        for prod in LevelB.from_tm(tmtc_file):
            fits_processor.write_fits(prod)
            # do again for __add__
            fits_processor.write_fits(prod)


def test_get_calibration_polynomial(idb):
    poly = idb.get_calibration_polynomial('CIX00036TM')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly(1) == poly.A[1]
    assert poly.valid is True

    assert (poly(np.array([1, 2, 3])) == np.array([poly.A[1], poly.A[1] * 2, poly.A[1] * 3])).all()
    assert poly([1, 2, 3]) == [poly.A[1], poly.A[1] * 2, poly.A[1] * 3]

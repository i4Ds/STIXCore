import os
import re
import shutil
from time import perf_counter
from pathlib import Path
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.io.fits.processors import FitsL0Processor, FitsL1Processor
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.products.level0.housekeeping import MaxiReport as MaxiReportL0
from stixcore.products.level0.housekeeping import MiniReport as MiniReportL0
from stixcore.products.level1.housekeeping import MaxiReport as MaxiReportL1
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

testpackets = [(test_data.tmtc.TM_3_25_1, MiniReportL0, 'mini',
                '0660010031:51423', '0660010031:51423', 1),
               (test_data.tmtc.TM_3_25_2, MaxiReportL0, 'maxi',
                '0660258881:33104', '0660258881:33104', 1)]


@pytest.fixture
def out_dir():
    out_dir = Path(__file__).parent.parent.parent / "io" / "tests" / "data" / "out"
    if not out_dir.exists():
        os.makedirs(out_dir)
    return out_dir


def teardown_function():
    out_dir = Path(__file__).parent.parent.parent / "io" / "tests" / "data" / "out"
    if out_dir.exists():
        shutil.rmtree(str(out_dir))


@pytest.fixture
def idbm():
    return IDBManager(test_data.idb.DIR)


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].stem for f in testpackets])
def test_housekeeping(levelb, packets):
    hex_file, cl, name, beg, end, size = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    hk = cl.from_levelb(levelb)

    assert hk.level == 'L0'
    assert hk.name == name
    assert str(hk.obs_beg) == beg
    assert str(hk.obs_end) == end
    assert len(hk.data) == size


@patch('stixcore.products.levelb.binary.LevelB')
def test_calibration_hk(levelb, idbm, out_dir):

    with test_data.tmtc.TM_3_25_2.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    hkl0 = MaxiReportL0.from_levelb(levelb)
    hkl1 = MaxiReportL1.from_level0(hkl0)

    fits_procl1 = FitsL1Processor(out_dir)
    fits_procl1.write_fits(hkl1)[0]

    assert True


def test_calibration_hk_many(idbm, out_dir):

    idbm.download_version("2.26.35", force=True)
    idbm.download_version("2.26.34", force=True)

    tstart = perf_counter()

    # read the HKM data from an entire day (part 1)
    prod_lb_p1 = next(LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P1)))
    # create a levelb product
    hkl0_p1 = MaxiReportL0.from_levelb(prod_lb_p1)

    fits_procl0 = FitsL0Processor(out_dir)
    # write the level0 to fits
    filename = fits_procl0.write_fits(hkl0_p1)[0]

    # read it in again
    hkl0_p1_io = Product(filename)

    # read the secont part of HKM data (same day)
    prod_lb_p2 = next(LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P2)))
    # create a levelb product
    hkl0_p2 = MaxiReportL0.from_levelb(prod_lb_p2)

    # fake a idb change on the same day
    hkl0_p2.idb_versions["2.26.35"] = hkl0_p2.idb_versions["2.26.34"]
    del hkl0_p2.idb_versions["2.26.34"]

    # combine both parts (incl idb_version change) still level 0
    hkl0 = hkl0_p1_io + hkl0_p2

    # convert to level 1
    hkl1 = MaxiReportL1.from_level0(hkl0, idbm=idbm)

    fits_procl1 = FitsL1Processor(out_dir)
    # write out the level 1 product
    filename = fits_procl1.write_fits(hkl1)[0]

    # read in again
    hkl1_io = Product(filename)

    # TODO add a lot of time checks and comparison between all products and levels
    assert hkl1_io.obs_end is not None

    tend = perf_counter()

    logger.info('Time taken %f', tend - tstart)


if __name__ == '__main__':
    test_calibration_hk_many(IDBManager(test_data.idb.DIR))

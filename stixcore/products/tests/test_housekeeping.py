import re
import tempfile
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
                '0660010031:51424', '0660010031:51424', 1),
               (test_data.tmtc.TM_3_25_2, MaxiReportL0, 'maxi',
                '0660258881:33104', '0660258881:33104', 1)]


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
    assert hk.scet_timerange.start.to_string() == beg
    assert hk.scet_timerange.end.to_string() == end
    assert len(hk.data) == size


@pytest.mark.xfail
@patch('stixcore.products.levelb.binary.LevelB')
def test_calibration_hk(levelb, idbm):

    with test_data.tmtc.TM_3_25_2.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    hkl0 = MaxiReportL0.from_levelb(levelb)
    hkl1 = MaxiReportL1.from_level0(hkl0)

    fits_procl1 = FitsL1Processor(Path(tempfile.gettempdir()))
    fits_procl1.write_fits(hkl1)[0]

    assert True


@pytest.mark.xfail
def test_calibration_hk_many(idbm):

    idbm.download_version("2.26.35", force=True)

    tstart = perf_counter()

    prod_lb_p1 = LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P1))
    hk_p1 = MaxiReportL0.from_levelb(list(prod_lb_p1)[0])

    fits_procl0 = FitsL0Processor(Path(tempfile.gettempdir()))
    filename = fits_procl0.write_fits(hk_p1)[0]

    hk_p1_io = Product(filename)

    prod_lb_p2 = LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P2))
    hk_p2 = MaxiReportL0.from_levelb(list(prod_lb_p2)[0])

    # fake a idb change on the same day
    hk_p2.idb_versions["2.26.35"] = hk_p2.idb_versions["2.26.34"]
    del hk_p2.idb_versions["2.26.34"]

    hkl0 = hk_p1_io + hk_p2

    hkl1 = MaxiReportL1.from_level0(hkl0, idbm=idbm)

    fits_procl1 = FitsL1Processor(Path(tempfile.gettempdir()))
    filename = fits_procl1.write_fits(hkl1)[0]

    tend = perf_counter()

    logger.info('Time taken %f', tend - tstart)


if __name__ == '__main__':
    test_calibration_hk_many(IDBManager(test_data.idb.DIR))

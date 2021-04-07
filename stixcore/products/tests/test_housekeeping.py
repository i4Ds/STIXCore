import re
from time import perf_counter
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.processing.engineering import raw_to_engineering_product
from stixcore.products.level0.housekeeping import MaxiReport, MiniReport
from stixcore.products.levelb.binary import LevelB

testpackets = [(test_data.tmtc.TM_3_25_1, MiniReport, 'mini',
                '0660010031:51423', '0660010031:51423', 1),
               (test_data.tmtc.TM_3_25_2, MaxiReport, 'maxi',
                '0660258881:33104', '0660258881:33104', 1)]


@pytest.fixture
def idbm():
    return IDBManager(test_data.idb.DIR)


@pytest.fixture
def soc_hk():
    return test_data.io.HK_MAXI


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
def test_calibration_hk(levelb, idbm):

    with test_data.tmtc.TM_3_25_2.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    hk = MaxiReport.from_levelb(levelb)

    setattr(hk, "idb", {"2.26.34": (660258881.0, 660258882.0)})
    # setattr(hk.data['hk_dpu_pcb_t'], "meta", {"NIXS": "NIXD0025", "PCF_CURTX": "CIXP0024TM"})
    # setattr(hk.data['hk_dpu_2v5_c'], "meta", {"NIXS": "NIXD0028", "PCF_CURTX": "CIXP0026TM"})
    raw_to_engineering_product(hk, idbm)

    print(1)


def test_calibration_hk_many(soc_hk, idbm):

    idbm.download_version("2.26.35", force=True)

    tstart = perf_counter()

    prod_lb = LevelB.from_tm(SOCPacketFile(soc_hk))
    hk = MaxiReport.from_levelb(prod_lb)

    setattr(hk, "idb", {"2.26.34": (653981065.0, 653991065.0),
                        "2.26.35": (653991065.0, 654073673.0)})
    raw_to_engineering_product(hk, idbm)

    tend = perf_counter()

    print('Time taken %f', tend - tstart)

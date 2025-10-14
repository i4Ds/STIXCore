import re
from time import perf_counter
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.io.product_processors.fits.processors import FitsL0Processor, FitsL1Processor
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.processing.LBtoL0 import Level0
from stixcore.products.level0.housekeepingL0 import MaxiReport as MaxiReportL0
from stixcore.products.level0.housekeepingL0 import MiniReport as MiniReportL0
from stixcore.products.level1.housekeepingL1 import MaxiReport as MaxiReportL1
from stixcore.products.level1.housekeepingL1 import MiniReport as MiniReportL1
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

testpackets = [
    (test_data.tmtc.TM_3_25_1, MiniReportL0, MiniReportL1, "mini", "0660010031:51424", "0660010031:51424", 1),
    (test_data.tmtc.TM_3_25_2, MaxiReportL0, MaxiReportL1, "maxi", "0660258881:33104", "0660258881:33104", 1),
]


@pytest.fixture(scope="module")
def soop_manager():
    SOOPManager.instance = SOOPManager(test_data.soop.DIR)
    return SOOPManager.instance


@pytest.fixture(scope="module")
def idbm():
    return IDBManager(test_data.idb.DIR)


@patch("stixcore.products.levelb.binary.LevelB")
@pytest.mark.parametrize("packets", testpackets, ids=[f[0].stem for f in testpackets])
def test_housekeeping(levelb, packets, soop_manager):
    hex_file, cl_l0, cl_l1, name, beg, end, size = packets
    with hex_file.open("r") as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = {"raw_file": "raw.xml", "packet": 0}

    hk_l0 = cl_l0.from_levelb(levelb)

    assert hk_l0.level == "L0"
    assert hk_l0.name == name
    assert hk_l0.scet_timerange.start.to_string() == beg
    assert hk_l0.scet_timerange.end.to_string() == end
    assert len(hk_l0.data) == size

    hk_l1 = cl_l1.from_level0(hk_l0)
    assert hk_l1.level == "L1"


@patch("stixcore.products.levelb.binary.LevelB")
def test_calibration_hk(levelb, idbm, tmp_path, soop_manager):
    with test_data.tmtc.TM_3_25_2.open("r") as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = {"raw_file": "raw.xml", "packet": 0}

    hkl0 = MaxiReportL0.from_levelb(levelb)
    hkl0.control["parent"] = ["parent.fits"]
    hkl0.control["raw_file"] = ["raw.xml"]
    hkl1 = MaxiReportL1.from_level0(hkl0)

    fits_procl1 = FitsL1Processor(tmp_path)
    fits_procl1.write_fits(hkl1)[0]

    assert True


def test_calibration_hk_many(idbm, tmp_path, soop_manager):
    idbm.download_version("2.26.35", force=True)
    IDBManager.instance = idbm

    tstart = perf_counter()

    prod_lb_p1 = list(LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P1)))[0]
    prod_lb_p1.control["raw_file"] = ["raw.xml"]
    hk_p1 = MaxiReportL0.from_levelb(prod_lb_p1)
    hk_p1.control["raw_file"] = ["raw.xml"]
    hk_p1.control["parent"] = ["parent.fits"]
    fits_procl0 = FitsL0Processor(tmp_path)

    filename = fits_procl0.write_fits(hk_p1)[0]

    hk_p1_io = Product(filename)

    prod_lb_p2 = LevelB.from_tm(SOCPacketFile(test_data.io.HK_MAXI_P2))
    hk_p2 = MaxiReportL0.from_levelb(list(prod_lb_p2)[0])

    # fake a idb change on the same day
    hk_p2.idb_versions["2.26.35"] = hk_p2.idb_versions["2.26.34"]
    del hk_p2.idb_versions["2.26.34"]

    hkl0 = hk_p1_io + hk_p2
    hkl0.control["raw_file"] = ["raw.xml"]
    hkl0.control["parent"] = ["parent.fits"]

    hkl1 = MaxiReportL1.from_level0(hkl0)
    hkl1.control["raw_file"] = ["raw.xml"]
    hkl1.control["parent"] = ["parent.fits"]

    fits_procl1 = FitsL1Processor(tmp_path)
    filename = fits_procl1.write_fits(hkl1)[0]

    tend = perf_counter()

    logger.info("Time taken %f", tend - tstart)


if __name__ == "__main__":
    # TODO to be removed
    # test_calibration_hk_many(IDBManager(test_data.idb.DIR))
    from pathlib import Path

    l1_a = Product(
        Path(
            "/home/shane/fits_20220321/L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T095000-20211013T095000_V01_2110130064.fits"
        )
    )  # noqa
    l0_a = l1_a.find_parent_products("/home/shane/fits_20220321/")[0]
    lb_a = l0_a.find_parent_products("/home/shane/fits_20220321/")[0]

    # gaps_a = lb_a.control['data_length'] < 4000
    # print(len(lb_a.control['data_length'][gaps_a]))

    # print(len(l1_a.data))

    # l1_b = Product(Path("/home/shane/fits_20220321/L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T034959-20211013T035842_V01_2110130059.fits"))  # noqa
    # l0_b = l1_b.find_parent_products("/home/shane/fits_20220321/")[0]
    # lb_b = l0_b.find_parent_products("/home/shane/fits_20220321/")[0]

    # gaps_b = lb_b.control['data_length'] < 4000
    # print(len(lb_b.control['data_length'][gaps_b]))
    # print(len(l1_b.data))
    # print(l1_b.data['timedel'].as_float())

    # #l1_f = Product(Path("/home/shane/fits_20220321/L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T035000-20211013T035121_V01_2110130054.fits"))  # noqa
    # l1_f = Product(Path("/home/shane/fits_20220321/L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T095000-20211013T095000_V01_2110130064.fits"))  # noqa
    # # l1_f = Product(Path("/home/shane/fits_20220321/L1/2021/06/28/SCI/solo_L1_stix-sci-xray-rpd_20210628T092301-20210628T092501_V01_2106280010-54759.fits"))  # noqa

    # l0_f = l1_f.find_parent_products("/home/shane/fits_20220321/")[0]
    # lb_f = l0_f.find_parent_products("/home/shane/fits_20220321/")[0]

    # _spm = SpiceKernelManager(test_data.ephemeris.KERNELS_DIR)
    # Spice.instance = Spice(_spm.get_latest_mk())
    # print(f"Spice kernel @: {Spice.instance.meta_kernel_path}")

    SOOPManager.instance = SOOPManager(test_data.soop.DIR)
    tmp_path = Path("/home/nicky/fitstest/")
    l0_proc = Level0(tmp_path, tmp_path)
    # l0_files = l0_proc.process_fits_files(files=l0_f.find_parent_files(
    # "/home/shane/fits_20220321/"))

    l0_files = l0_proc.process_fits_files(
        files=[Path("/home/shane/fits_20220321/LB/21/6/42/solo_LB_stix-21-6-42_0700358400_V01.fits")]
    )
    p = [Product(f) for f in l0_files]

    print("done")

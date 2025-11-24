import zipfile
import warnings
import urllib.request
from pprint import pformat
from pathlib import Path

import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.manager import IDBManager
from stixcore.io.RidLutManager import RidLutManager
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.pipeline import PipelineStatus
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.scripts.end2end_testing import end2end_pipeline

logger = get_logger(__name__)


@pytest.fixture(scope="session")
def out_dir(tmpdir_factory):
    return Path(str(tmpdir_factory.getbasetemp()))


@pytest.fixture(scope="session")
def orig_data(out_dir):
    orig_dir = out_dir / "origdata"
    orig_dir.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve("https://pub099.cs.technik.fhnw.ch/data/end2end/data/head.zip", orig_dir / "orig.zip")

    with zipfile.ZipFile(orig_dir / "orig.zip", "r") as zip_ref:
        zip_ref.extractall(orig_dir)
    return orig_dir


@pytest.fixture(scope="session")
def orig_fits(orig_data):
    return list(orig_data.rglob("*.fits"))


@pytest.fixture(scope="session")
def current_fits(orig_data, out_dir):
    return end2end_pipeline(orig_data, out_dir)


@pytest.mark.remote_data
@pytest.mark.end2end
def test_find_parents(current_fits, out_dir):
    for fits in current_fits:
        p = Product(fits)
        if hasattr(p, "find_parent_files"):
            parents = p.find_parent_files(out_dir)
            assert len(parents) > 0


@pytest.mark.remote_data
@pytest.mark.end2end
def test_complete(orig_fits, current_fits):
    error_c = 0
    for ofits in orig_fits:
        try:
            next(cfits for cfits in current_fits if ofits.name == cfits.name)
        except StopIteration:
            error_c += 1
            warnings.warn(f"no corresponding file found for {ofits} in the current fits files")
    if error_c > 0:
        raise ValueError(f"{error_c} errors out of {len(orig_fits)}\nnumber of fits files differ")


@pytest.mark.remote_data
@pytest.mark.end2end
def test_identical(orig_fits, current_fits):
    error_c = 0
    error_files = list()

    for cfits in current_fits:
        # find corresponding original file
        try:
            ofits = next(ofits for ofits in orig_fits if ofits.name == cfits.name)
        except StopIteration:
            error_c += 1
            warnings.warn(f"no corresponding file found for {cfits} in the original fits files")
            error_files.append((cfits, ofits))
            continue
        diff = FITSDiff(
            ofits,
            cfits,
            atol=0.00001,
            rtol=0.00001,
            ignore_keywords=["CHECKSUM", "DATASUM", "DATE", "VERS_SW", "VERS_CFG", "HISTORY", "COMMENT"],
        )
        if not diff.identical:
            error_c += 1
            warnings.warn(diff.report())
            error_files.append((cfits, ofits))

    if error_c > 0:
        raise ValueError(
            f"{error_c} errors out of {len(current_fits)}\nthere are differences in FITS files\n {pformat(error_files)}"
        )


@pytest.mark.skip(reason="used as a local test at the moment")
def test_e2e_21_6_32(out_dir):
    _spm = SpiceKernelManager(test_data.ephemeris.KERNELS_DIR)
    Spice.instance = Spice(_spm.get_latest_mk())

    # pinpoint the api files location
    CONFIG.set("SOOP", "soop_files_download", str(test_data.soop.DIR))

    SOOPManager.instance = SOOPManager(test_data.soop.DIR, mock_api=True)

    idbpath = Path(__file__).parent.parent.parent / "data" / "idb"
    IDBManager.instance = IDBManager(idbpath)  # force_version="2.26.35")

    RidLutManager.instance = RidLutManager(Path(CONFIG.get("Publish", "rid_lut_file")), update=True)

    PipelineStatus.log_setup()

    f1 = Path("/data/stix/out/test/e2e_21_6_32/solo_LB_stix-21-6-32_0678153600_V02.fits")
    f2 = Path("/data/stix/out/test/e2e_21_6_32/solo_LB_stix-21-6-32_0678240000_V02.fits")

    out_dir_o1 = out_dir / "o1"
    out_dir_o2 = out_dir / "o2"

    l0_proc_o1 = Level0(out_dir_o1, out_dir_o1)
    l0_files_o1 = l0_proc_o1.process_fits_files(files=[f2, f1])

    l0_proc_o2 = Level0(out_dir_o2, out_dir_o2)
    l0_files_o2 = l0_proc_o2.process_fits_files(files=[f1, f2])

    n_errors = 0

    for fits_1 in l0_files_o1:
        # find corresponding original file
        fits_2 = next(ofits for ofits in l0_files_o2 if ofits.name == fits_1.name)
        diff = FITSDiff(
            fits_1,
            fits_2,
            atol=0.00001,
            rtol=0.00001,
            ignore_keywords=["CHECKSUM", "DATASUM", "DATE", "VERS_SW", "VERS_CFG", "HISTORY"],
        )
        if not diff.identical:
            print(diff.report())
            n_errors += 1

    assert n_errors == 0

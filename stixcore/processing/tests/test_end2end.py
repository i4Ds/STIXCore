import zipfile
import warnings
import urllib.request
from pprint import pformat
from pathlib import Path

import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.products.product import Product
from stixcore.util.logging import get_logger
from stixcore.util.scripts.end2end_testing import end2end_pipeline

logger = get_logger(__name__)


@pytest.fixture(scope="session")
def out_dir(tmpdir_factory):
    return Path(str(tmpdir_factory.getbasetemp()))


@pytest.mark.remote_data
@pytest.fixture(scope="session")
def orig_data(out_dir):
    orig_dir = out_dir / "origdata"
    orig_dir.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve("https://pub099.cs.technik.fhnw.ch/data/end2end/data/head.zip",
                               orig_dir / "orig.zip")

    with zipfile.ZipFile(orig_dir / "orig.zip", 'r') as zip_ref:
        zip_ref.extractall(orig_dir)
    return orig_dir


@pytest.fixture(scope="session")
def orig_fits(orig_data):
    return list(orig_data.rglob("*.fits"))


@pytest.fixture(scope="session")
def current_fits(orig_data, out_dir):
    return end2end_pipeline(orig_data, out_dir)


def test_find_parents(current_fits, out_dir):
    for fits in current_fits:
        p = Product(fits)
        if hasattr(p, 'find_parent_files'):
            parents = p.find_parent_files(out_dir)
            assert len(parents) > 0


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
            continue
        diff = FITSDiff(ofits, cfits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW', 'HISTORY'])
        if not diff.identical:
            error_c += 1
            warnings.warn(diff.report())
            error_files.append((cfits, ofits))

    if error_c > 0:
        raise ValueError(f"{error_c} errors out of {len(current_fits)}\n"
                         f"there are differences in FITS files\n {pformat(error_files)}")

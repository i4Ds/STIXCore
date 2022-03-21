import zipfile
import warnings
import urllib.request
from pathlib import Path

import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.util.logging import get_logger
from stixcore.util.scripts.end2end_testing import en2end_pipeline

logger = get_logger(__name__)


@pytest.fixture(scope="session")
def out_dir(tmpdir_factory):
    return Path(str(tmpdir_factory.getbasetemp()))


@pytest.mark.remote_data
@pytest.fixture(scope="session")
def orig_data(out_dir):
    orig_dir = out_dir / "origdata"
    orig_dir.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve("http://pub099.cs.technik.fhnw.ch/data/end2end/data/head.zip",
                               orig_dir / "orig.zip")

    with zipfile.ZipFile(orig_dir / "orig.zip", 'r') as zip_ref:
        zip_ref.extractall(orig_dir)
    return orig_dir


@pytest.fixture(scope="session")
def orig_fits(orig_data):
    return list(orig_data.rglob("*.fits"))


@pytest.fixture(scope="session")
def current_fits(orig_data, out_dir):
    return en2end_pipeline(orig_data, out_dir)


@pytest.mark.end2end
def test_complete(orig_fits, current_fits):
    error = False
    for ofits in orig_fits:
        try:
            next(cfits for cfits in current_fits if ofits.name == cfits.name)
        except StopIteration:
            error = True
            warnings.warn(f"no corresponding file found for {ofits} in the current fits files")
    if error:
        raise ValueError("one or many errors\nnumber of fits files differ")


@pytest.mark.end2end
def test_identical(orig_fits, current_fits):
    error = False
    for cfits in current_fits:
        # find corresponding original file
        try:
            ofits = next(ofits for ofits in orig_fits if ofits.name == cfits.name)
        except StopIteration:
            error = True
            warnings.warn(f"no corresponding file found for {cfits} in the original fits files")
            continue
        diff = FITSDiff(ofits, cfits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
        if not diff.identical:
            error = True
            warnings.warn(diff.report())

    if error:
        raise ValueError("one or many errors\nthere are differentses in FITS files")

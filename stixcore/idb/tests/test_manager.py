import os
import shutil
from pathlib import Path

import pytest

from stixcore.data.test import test_data
from stixcore.datetime.datetime import SCETime as StixDateTime
from stixcore.idb.manager import IDBManager


@pytest.fixture
def idb_manager():
    return IDBManager(test_data.idb.DIR)


def teardown_function():
    downloadtest = Path(os.path.abspath(__file__)).parent / 'data/v2.26.33'
    if downloadtest.exists():
        shutil.rmtree(str(downloadtest))


def test_idb_manager(idb_manager):
    assert str(idb_manager.data_root) ==\
           str(Path(__file__).parent.parent.parent / "data" / "test" / "idb")


def test_root_not_found_error():
    with pytest.raises(ValueError) as e:
        _ = IDBManager(".foo/")
    assert str(e.value).startswith('path not found')


@pytest.mark.parametrize('versions', [("2.26.1", False), ("2.26.2", False), ((2, 26, 3), False),
                                      ("2.26.34", True), ("1.2.3", False)])
def test_has_version(versions, idb_manager):
    versionlabel, should = versions
    has_ver = idb_manager.has_version(versionlabel)
    assert should == has_ver


@pytest.mark.remote_data
def test_download_version(idb_manager):
    assert idb_manager.download_version("2.26.34", force=True)

    with pytest.raises(ValueError) as e:
        idb_manager.download_version("2.26.34")
    assert len(str(e.value)) > 1

    assert idb_manager.download_version("2.26.34", force=True)


def test_find_version(idb_manager):
    idb = idb_manager.get_idb(obt=StixDateTime(coarse=631155005, fine=0))
    assert idb.get_idb_version() == "2.26.34"
    idb.close()

    # fall back to the default
    idb = idb_manager.get_idb(obt=StixDateTime(coarse=9631155005, fine=0))
    assert idb.get_idb_version() == "2.26.34"

    v = idb_manager.find_version(obt=None)
    assert v == "2.26.3"


def test_get_versions(idb_manager):
    versions = idb_manager.get_versions()
    assert isinstance(versions, list)
    # zjust 3 not 4 as 2.26.2 contains no file
    assert len(versions) == 4


def test_get_idb_not_found_error(idb_manager):
    with pytest.raises(ValueError) as e:
        _ = idb_manager.get_idb("a.b.c")
    assert str(e.value).startswith('Version')


def test_get_idb(idb_manager):
    idb = idb_manager.get_idb("2.26.34")
    assert idb.get_idb_version() == "2.26.34"
    assert idb.is_connected() is True
    idb.close()
    assert idb.is_connected() is False


def test_get_idb_cached(idb_manager):
    idb = idb_manager.get_idb("2.26.34")
    idbc = idb_manager.get_idb("2.26.34")
    assert idb is idbc

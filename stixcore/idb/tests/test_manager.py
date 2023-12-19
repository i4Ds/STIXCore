import os
import shutil
from pathlib import Path

import pytest

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.time import SCETime


@pytest.fixture
@pytest.mark.remote_data
def idb_manager():
    return IDBManager(test_data.idb.DIR)


def teardown_function():
    downloadtest = Path(os.path.abspath(__file__)).parent / 'data/v2.26.33'
    if downloadtest.exists():
        shutil.rmtree(str(downloadtest))


@pytest.mark.remote_data
def test_idb_manager(idb_manager):
    assert str(idb_manager.data_root) ==\
           str(Path(__file__).parent.parent.parent / "data" / "test" / "idb")


@pytest.mark.parametrize('versions', [("2.26.1", False), ("2.26.2", False), ((2, 26, 3), False),
                                      ("2.26.34", True), ("1.2.3", False)])
def test_has_version(versions, idb_manager):
    versionlabel, should = versions
    has_ver = idb_manager.has_version(versionlabel)
    assert should == has_ver


@pytest.mark.remote_data
def test_force_version_str(idb_manager):
    idb_manager.download_version("2.26.35", force=True)
    idb_m = IDBManager(test_data.idb.DIR, force_version='2.26.35')
    idb_f = idb_m.get_idb("any")
    assert idb_f.get_idb_version() == '2.26.35'
    assert idb_f.filename == test_data.idb.DIR / 'v2.26.35' / 'idb.sqlite'

    idb = idb_m.get_idb(obt=SCETime.min_time())
    assert idb_f == idb

    idb = idb_m.get_idb(obt=SCETime.max_time())
    assert idb_f == idb


def test_force_version_path():
    p = test_data.idb.DIR.parent / 'idb_force' / 'idb.sqlite'
    idb_m = IDBManager(test_data.idb.DIR, force_version=p)
    idb = idb_m.get_idb("any")
    assert idb.get_idb_version() == '2.26.35'
    assert idb.filename == p


@pytest.mark.remote_data
def test_download_version(idb_manager):
    assert idb_manager.download_version("2.26.34", force=True)

    assert idb_manager.download_version("2.26.34")

    assert idb_manager.download_version("2.26.34", force=True)


@pytest.mark.remote_data
def test_find_version(idb_manager):
    idb = idb_manager.get_idb(obt=SCETime(coarse=631155005, fine=0))
    assert idb.get_idb_version() == "2.26.32"
    idb.close()

    # fall back to the default
    idb = idb_manager.get_idb(obt=SCETime(coarse=2 ** 31 - 1, fine=0))
    assert idb.get_idb_version() == "2.26.37"

    assert idb_manager.find_version(obt=None) == "2.26.32"


@pytest.mark.remote_data
def test_get_versions(idb_manager):
    versions = idb_manager.get_versions()
    assert isinstance(versions, list)
    assert len(versions) > 4


@pytest.mark.remote_data
def test_get_idb_not_found_error(idb_manager):
    with pytest.raises(ValueError) as e:
        _ = idb_manager.get_idb("a.b.c")
    assert str(e.value).startswith('Version')


@pytest.mark.remote_data
def test_get_idb(idb_manager):
    idb = idb_manager.get_idb("2.26.34")
    assert idb.get_idb_version() == "2.26.34"
    assert idb.is_connected() is True
    idb.close()
    assert idb.is_connected() is False


@pytest.mark.remote_data
def test_get_idb_cached(idb_manager):
    idb = idb_manager.get_idb("2.26.34")
    idbc = idb_manager.get_idb("2.26.34")
    assert idb is idbc

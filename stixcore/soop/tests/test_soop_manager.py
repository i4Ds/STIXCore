import os
import sys
import time
import shutil
import platform

import dateutil.parser
import pytest
from watchdog.observers import Observer

from stixcore.data.test import test_data
from stixcore.processing.pipeline import GFTSFileHandler
from stixcore.soop.manager import HeaderKeyword, KeywordSet, SOOPManager, SoopObservationType

MOVE_FILE = 'SSTX_observation_timeline_export_M04_V02.json'


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


@pytest.fixture
def soop_manager():
    return SOOPManager(test_data.soop.DIR, mock_api=True)


def teardown_function():
    movedfile = test_data.soop.DIR / MOVE_FILE
    if movedfile.exists():
        os.remove(movedfile)

    movedfile = test_data.soop.DIR / (MOVE_FILE+".tmp")
    if movedfile.exists():
        os.remove(movedfile)


def test_soop_manager(soop_manager):
    assert str(soop_manager.data_root) ==\
           str(test_data.soop.DIR)


@pytest.mark.skipif(sys.platform.startswith('win'), reason="does not run on windows")
def test_soop_manager_watchdog(soop_manager):
    observer = Observer()
    soop_handler = GFTSFileHandler(soop_manager.add_soop_file_to_index,
                                   SOOPManager.SOOP_FILE_REGEX)
    observer.schedule(soop_handler, soop_manager.data_root,  recursive=False)
    observer.start()

    # 3 files are in the base dir
    assert soop_manager.filecounter == 3
    assert len(soop_manager.soops) == 129
    assert len(soop_manager.observations) == 132

    time.sleep(1)

    # emulate a new file approaches via rsync
    shutil.copy(test_data.soop.DIR / "wd" / MOVE_FILE, test_data.soop.DIR / (MOVE_FILE+".tmp"))
    shutil.move(test_data.soop.DIR / (MOVE_FILE+".tmp"), test_data.soop.DIR / MOVE_FILE)

    time.sleep(3)

    # currently not triggered on mac see: https://github.com/i4Ds/STIXCore/issues/149
    if platform.system() != "Darwin":
        # the new data should be integrated now
        assert soop_manager.filecounter == 4
        assert len(soop_manager.soops) == 129
        assert len(soop_manager.observations) == 132

    observer.stop()


def test_soop_download_all_soop(soop_manager, out_dir):
    version = 2
    lpt = 'LTP09_Sep2022-Dec2022'
    f = out_dir / f"{lpt}.{version}.all.json"
    assert not f.exists()
    soop_manager.download_all_soops_from_api(lpt, version, f)
    assert f.exists()


def test_soop_manager_find_point(soop_manager):
    start = dateutil.parser.parse("2021-10-04T12:00:00Z")
    obslist = soop_manager.find_observations(start=start)
    assert len(obslist) == 2
    for obs in obslist:
        assert obs.startDate <= start <= obs.endDate


def test_soop_manager_find_range(soop_manager):
    start = dateutil.parser.parse("2021-10-04T12:00:00Z")
    end = dateutil.parser.parse("2021-10-18T00:00:00Z")
    obslist = soop_manager.find_observations(start=start, end=end)
    assert len(obslist) == 16
    for obs in obslist:
        assert (obs.startDate >= start or obs.endDate >= start)
        assert (obs.endDate <= end or obs.startDate <= end)


def test_soop_manager_find_filter(soop_manager):
    start = dateutil.parser.parse("2021-10-04T12:00:00Z")
    end = dateutil.parser.parse("2021-10-18T00:00:00Z")
    obslist = soop_manager.find_observations(start=start, end=end,
                                             otype=SoopObservationType.STIX_BASIC)
    assert len(obslist) == 2


def test_soop_manager_get_keywords(soop_manager):
    start = dateutil.parser.parse("2021-12-24T12:00:00Z")
    end = dateutil.parser.parse("2021-12-25T00:00:00Z")
    keylist = soop_manager.get_keywords(start=start, end=end, otype=SoopObservationType.ALL)
    assert len(keylist) == 3
    keyset = KeywordSet(keylist)
    assert keyset.get(HeaderKeyword(name='OBS_ID')).value\
        == "SSTX_050A_LF5_11B_5Md2_11X;SSTX_050A_LF5_11B_vFLg_16S"
    assert keyset.get(HeaderKeyword(name='OBS_TYPE')).value\
        == "5Md2;vFLg"
    assert keyset.get(HeaderKeyword(name='OBS_MODE')).value\
        == "STIX_ANALYSIS;STIX_BASIC"

    start = dateutil.parser.parse("2022-11-05T12:00:00Z")
    end = dateutil.parser.parse("2022-11-05T20:00:00Z")
    keylist = soop_manager.get_keywords(start=start, end=end, otype=SoopObservationType.ALL)
    assert len(keylist) == 3
    keyset = KeywordSet(keylist)
    assert keyset.get(HeaderKeyword(name='TARGET')).value\
        == "none"
    assert keyset.get(HeaderKeyword(name='SOOPTYPE')).value\
        == "LF2"
    assert keyset.get(HeaderKeyword(name='SOOPNAME')).value\
        == "L_FULL_HRES_HCAD_Eruption-Watch"


def test_soop_manager_get_keywords_time_not_found(soop_manager):
    start = dateutil.parser.parse("2016-10-04T12:00:00Z")
    end = dateutil.parser.parse("2016-10-18T00:00:00Z")
    keylist = soop_manager.get_keywords(start=start, end=end, otype=SoopObservationType.ALL)
    assert len(keylist) == 0


def test_soop_manager_keywordset():
    a = HeaderKeyword(name="a", value="v1", comment="ca")
    a2 = HeaderKeyword(name="a", value="v2", comment="ca")
    a3 = HeaderKeyword(name="a", value="v2", comment="ca")

    b = HeaderKeyword(name="b", value="v1", comment="cb")
    b2 = HeaderKeyword(name="B", value="v1", comment="cb2")

    f_set = KeywordSet([a, a2, b, b2, a3])

    f_list = f_set.to_list()
    assert len(f_list) == 2

    ma = f_set.get(a)
    assert ma.comment == "ca"
    assert ma.name == "A"
    assert ma.value == "v1;v2"

    mb = f_set.get(b)
    assert mb.comment == "cb;cb2"
    assert mb.name == "B"
    assert mb.value == "v1"

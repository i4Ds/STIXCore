import pytest

from stixcore.processing.publish import PublishConflicts, PublishHistoryStorage
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


def test_publish_history_create_empty(out_dir):
    h = PublishHistoryStorage(out_dir / "test.sqlite")
    assert h.is_connected()
    assert h.count() == 0


def test_publish_history_add_file_not_found(out_dir):
    h = PublishHistoryStorage(out_dir / "test.sqlite")
    assert h.is_connected()
    with pytest.raises(FileNotFoundError):
        h.add(out_dir / "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-5.fits")
    assert h.count() == 0


def test_publish_history_add_file_wrong_name_structure(out_dir):
    h = PublishHistoryStorage(out_dir / "test.sqlite")
    assert h.is_connected()
    with pytest.raises(IndexError):
        h.add(out_dir / "test_best.fits")
    assert h.count() == 0


def test_publish_history_add_file(out_dir):
    h = PublishHistoryStorage(out_dir / "test.sqlite")
    f1 = out_dir / "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-59362.fits"
    f1.touch()
    f2 = out_dir / "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-60000.fits"
    f2.touch()
    assert h.is_connected()

    status, items_a1 = h.add(f1)
    assert h.count() == 1
    assert status == PublishConflicts.ADDED
    assert items_a1[0]['name'] ==\
        'solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-59362.fits'
    assert items_a1[0]['path'] == str(out_dir)
    assert items_a1[0]['version'] == 1
    assert items_a1[0]['esaname'] == 'solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01'

    status, items_a2 = h.add(f1)
    assert h.count() == 1
    assert status == PublishConflicts.SAME_EXISTS
    assert items_a2[0] == items_a1[0]

    status, items_a3 = h.add(f2)
    assert h.count() == 2
    assert status == PublishConflicts.SAME_ESA_NAME
    assert items_a3[0] == items_a1[0]
    assert items_a3[1]['name'] ==\
        'solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-60000.fits'
    assert items_a3[1]['path'] == str(out_dir)
    assert items_a3[1]['version'] == 1
    assert items_a3[1]['esaname'] == 'solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01'

    status, items_a4 = h.add(f2)
    assert h.count() == 2
    assert status == PublishConflicts.SAME_EXISTS
    assert items_a4[0] == items_a3[1]

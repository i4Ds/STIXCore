
from unittest.mock import patch

import numpy as np
import pytest

from astropy.table import QTable

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.processing.publish import (
    PublishConflicts,
    PublishHistoryStorage,
    PublishResult,
    publish_fits_to_esa,
)
from stixcore.time.datetime import SCETime, SCETimeRange
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


@patch('stixcore.products.level1.scienceL1.Spectrogram')
def test_publish_fits_to_esa(product, out_dir):
    target_dir = out_dir / "esa"
    same_dir = out_dir / "same"
    fits_dir = out_dir / "fits"
    same_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    fits_dir.mkdir(parents=True, exist_ok=True)

    processor = FitsL1Processor(fits_dir)
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)
    product.scet_timerange = SCETimeRange(start=beg, end=end)
    product.utc_timerange = product.scet_timerange.to_timerange()
    product.idb_versions = {'1.2': product.scet_timerange}
    product.control = QTable({"scet_coarse": [[beg.coarse, end.coarse]],
                              "scet_fine": [[beg.fine, end.fine]],
                              "index": [1],
                              "request_id": [123]})

    t = SCETime(coarse=[beg.coarse, end.coarse])
    product.data = QTable({"time": t,
                           "timedel": t-beg,
                           "fcounts": np.array([1, 2]),
                           "control_index": [1, 1]})
    product.raw = ['packet1.xml', 'packet2.xml']
    product.parent = ['packet1.xml', 'packet2.xml']
    product.level = 'L1'
    product.service_type = 21
    product.service_subtype = 6
    product.ssid = 24
    product.type = 'sci'
    product.name = 'xray-spec'
    product.obt_beg = beg
    product.obt_end = end
    product.date_obs = beg
    product.date_beg = beg
    product.date_end = end
    product.split_to_files.return_value = [product]
    product.get_energies = False

    data = product.data[:]  # make a clone
    files = []
    product.control['request_id'] = 123
    files.extend(processor.write_fits(product))  # orig

    product.control['request_id'] = 124
    product.data = data[:]
    files.extend(processor.write_fits(product))  # same ignore

    product.control['request_id'] = 125
    product.data = data[:]
    files.extend(processor.write_fits(product))  # same ignore

    product.control['request_id'] = 126
    product.data = data[:]
    product.data['fcounts'][0] = 100
    files.extend(processor.write_fits(product))  # sub1

    product.control['request_id'] = 127
    product.data = data[:]
    product.data['fcounts'][0] = 200
    files.extend(processor.write_fits(product))  # sub2

    product.control['request_id'] = 128
    product.data = data[:]
    product.data['fcounts'][0] = 300
    files.extend(processor.write_fits(product))  # sub3 -> ERROR

    product.control['request_id'] = 129
    product.data = data[:]
    product.data['fcounts'][0] = 400
    files.extend(processor.write_fits(product))  # sub4 -> ERROR

    res = publish_fits_to_esa(['--target_dir', str(target_dir),
                               '--same_esa_name_dir', str(same_dir),
                               '--include_levels', 'l1',
                               '--sort_files',
                               '--update_rid_lut',
                               '--waiting_period', '0s',
                               '--db_file', str(out_dir / "test.sqlite"),
                               '--fits_dir', str(fits_dir)])

    assert res
    # the first one was added
    assert len(res[PublishResult.PUBLISHED]) == 1
    # 2 where published as supplement
    assert len(res[PublishResult.MODIFIED]) == 2
    # 2 where ignored as the data is the same
    assert len(res[PublishResult.IGNORED]) == 2
    # 2 errors as it would be a third/more supplement
    assert len(res[PublishResult.ERROR]) == 2
    assert res[PublishResult.ERROR][0][1] == 'max supplement error'

import re
import sys
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

import astropy.units as u
from astropy.io import fits
from astropy.table import QTable

from stixcore.io.product_processors.fits.processors import FitsL1Processor
from stixcore.processing.publish import (
    PublishConflicts,
    PublishHistoryStorage,
    PublishResult,
    publish_fits_to_esa,
)
from stixcore.products.product import Product
from stixcore.time.datetime import SCETime, SCETimeRange
from stixcore.util.logging import get_logger
from stixcore.util.scripts.incomplete_twins_fits import incomplete_twins
from stixcore.util.util import (
    get_complete_file_name,
    get_complete_file_name_and_path,
    get_incomplete_file_name,
)

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
    assert items_a1[0]["name"] == "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-59362.fits"
    assert items_a1[0]["path"] == str(out_dir)
    assert items_a1[0]["version"] == 1
    assert items_a1[0]["esaname"] == "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01"

    status, items_a2 = h.add(f1)
    assert h.count() == 1
    assert status == PublishConflicts.SAME_EXISTS
    assert items_a2[0] == items_a1[0]

    status, items_a3 = h.add(f2)
    assert h.count() == 2
    assert status == PublishConflicts.SAME_ESA_NAME
    assert items_a3[0] == items_a1[0]
    assert items_a3[1]["name"] == "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01_2202230003-60000.fits"
    assert items_a3[1]["path"] == str(out_dir)
    assert items_a3[1]["version"] == 1
    assert items_a3[1]["esaname"] == "solo_L1_stix-sci-xray-spec_20220223T1-20220223T2_V01"

    status, items_a4 = h.add(f2)
    assert h.count() == 2
    assert status == PublishConflicts.SAME_EXISTS
    assert items_a4[0] == items_a3[1]


@patch("stixcore.products.level1.quicklookL1.Background")
def test_publish_fits_to_esa_incomplete(product, out_dir):
    PublishHistoryStorage(out_dir / "test.sqlite")

    target_dir = out_dir / "esa"
    same_dir = out_dir / "same"
    fits_dir = out_dir / "fits"
    same_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    fits_dir.mkdir(parents=True, exist_ok=True)

    files = []
    processor = FitsL1Processor(fits_dir)
    for stime in [707875074, 708739079, 709739079]:
        beg = SCETime(coarse=stime, fine=0)
        end = SCETime(coarse=stime + 10, fine=2**15)
        product.scet_timerange = SCETimeRange(start=beg, end=end)
        product.utc_timerange = product.scet_timerange.to_timerange()
        product.idb_versions = {"1.2": product.scet_timerange}
        product.control = QTable(
            {
                "scet_coarse": [[beg.coarse, end.coarse]],
                "scet_fine": [[beg.fine, end.fine]],
                "index": [1],
            }
        )

        t = SCETime(coarse=[beg.coarse, end.coarse])
        product.data = QTable({"time": t, "timedel": t - beg, "fcounts": np.array([1, 2]), "control_index": [1, 1]})
        product.raw = ["packet1.xml", "packet2.xml"]
        product.parent = ["packet1.xml", "packet2.xml"]
        product.level = "L1"
        product.LEVEL = "L1"
        product.service_type = 21
        product.service_subtype = 6
        product.ssid = 31
        product.fits_daily_file = True
        product.type = "ql"
        product.name = "background"
        product.NAME = "background"
        product.obt_beg = beg
        product.obt_end = end
        product.date_obs = beg
        product.date_beg = beg
        product.date_end = end
        product.exposure = 2
        product.max_exposure = 3
        product.dmin = 2
        product.dmax = 3
        product.bunit = "count"
        product.split_to_files.return_value = [product]
        product.get_energies = False
        product.get_processing_version.return_value = 2

        files.extend(processor.write_fits(product))

    assert len(files) == 3
    # this was processed with predicted and flown
    assert (
        fits.getval(files[0], "SPICE_MK")
        == "solo_ANC_soc-pred-mk_V106_20201116_001.tm, solo_ANC_soc-flown-mk_V105_20200515_001.tm"
    )
    # the filename should be marked as incomplete
    assert get_complete_file_name(files[0].name) != files[0].name
    assert get_incomplete_file_name(files[0].name) == files[0].name

    res = publish_fits_to_esa(
        [
            "--target_dir",
            str(target_dir),
            "--same_esa_name_dir",
            str(same_dir),
            "--include_levels",
            "l1",
            "--waiting_period",
            "0s",
            "--db_file",
            str(out_dir / "test.sqlite"),
            "--fits_dir",
            str(fits_dir),
        ]
    )

    assert res
    published_files = res[PublishResult.PUBLISHED]

    assert len(published_files) == 3
    # no conflicting esa names expected
    assert len(published_files[0]) == 1
    # incomplete files should be updated with just flown spice mk
    assert (
        fits.getval(Path(published_files[0][0]["path"]) / published_files[0][0]["name"], "SPICE_MK")
        == "solo_ANC_soc-flown-mk_V105_20200515_001.tm"
    )
    # should be marked as complete afterwards
    assert get_complete_file_name(published_files[0][0]["name"]) == published_files[0][0]["name"]
    assert get_incomplete_file_name(published_files[0][0]["name"]) != published_files[0][0]["name"]


def test_fits_incomplete_switch_over(out_dir):
    with patch("stixcore.products.level1.quicklookL1.Background") as product:
        PublishHistoryStorage(out_dir / "test.sqlite")

        target_dir = out_dir / "esa"
        same_dir = out_dir / "same"
        fits_dir = out_dir / "fits"
        same_dir.mkdir(parents=True, exist_ok=True)
        target_dir.mkdir(parents=True, exist_ok=True)
        fits_dir.mkdir(parents=True, exist_ok=True)

        files_first = []
        processor = FitsL1Processor(fits_dir)
        for stime in [707875174, 708739179, 709739179, 1999999999]:
            beg = SCETime(coarse=stime, fine=0)
            end = SCETime(coarse=stime + 10, fine=2**15)
            product.scet_timerange = SCETimeRange(start=beg, end=end)
            product.utc_timerange = product.scet_timerange.to_timerange()
            product.idb_versions = {"1.2": product.scet_timerange}
            product.control = QTable(
                {
                    "scet_coarse": [beg.coarse],
                    "scet_fine": [beg.fine],
                    "raw_file": ["test.xml"],
                    "parent": ["parent.fits"],
                    "index": [1],
                }
            )

            t = SCETime(coarse=[beg.coarse, end.coarse])
            product.data = QTable(
                {
                    "time": t,
                    "timedel": t - beg,
                    "fcounts": np.array([1, 2]),
                    "counts": np.array([1, 2]) * u.deg_C,
                    "control_index": [1, 1],
                }
            )
            product.raw = ["packet1.xml", "packet2.xml"]
            product.parent = ["packet1.xml", "packet2.xml"]
            product.level = "L1"
            product.LEVEL = "L1"
            product.NAME = "background"
            product.service_type = 21
            product.service_subtype = 6
            product.ssid = 31
            product.fits_daily_file = True
            product.type = "ql"
            product.name = "background"
            product.obt_beg = beg
            product.obt_end = end
            product.date_obs = beg
            product.date_beg = beg
            product.date_end = end
            product.exposure = 2
            product.max_exposure = 3
            product.dmin = 2
            product.dmax = 3
            product.bunit = "counts"
            product.split_to_files.return_value = [product]
            product.get_energies = False
            product.get_processing_version.return_value = 2

            files_first.extend(processor.write_fits(product))

        assert len(files_first) == 4
        assert get_complete_file_name(files_first[0].name) != files_first[0].name
        assert get_incomplete_file_name(files_first[0].name) == files_first[0].name

        res = publish_fits_to_esa(
            [
                "--target_dir",
                str(target_dir),
                "--same_esa_name_dir",
                str(same_dir),
                "--include_levels",
                "l1",
                "--waiting_period",
                "0s",
                "--db_file",
                str(out_dir / "test.sqlite"),
                "--fits_dir",
                str(fits_dir),
            ]
        )

        assert res
        published_files = res[PublishResult.PUBLISHED]

        # the one file from the future should be ignored
        assert len(res[PublishResult.IGNORED]) == 1

        # after the publishing all files will have the complete name
        assert len(published_files) == 3
        # no conflicting esa names expected
        assert len(published_files[0]) == 1

        # should be marked as complete afterwards
        assert get_complete_file_name(published_files[0][0]["name"]) == published_files[0][0]["name"]
        assert get_incomplete_file_name(published_files[0][0]["name"]) != published_files[0][0]["name"]

        files_last = []
        for f in files_first[:-1]:  # not the last one from future
            # read all FITS files and modify the data slightly
            p = Product(get_complete_file_name_and_path(f))
            # old data.fcounts = [t1: 1, t2: 2]
            p.data["fcounts"][0] = 3
            p.data["counts"] = 3 * u.deg_C
            # remove the second time stamp
            p.data.remove_row(1)
            # new data.fcounts = [t1: 3]
            # write the data again
            # the complete file(name) should be found and the data should be merged into that
            files_last.extend(processor.write_fits(p))

        for f in files_last:
            p = Product(f)
            assert len(p.data) == 2
            # the combined data should contain the second time bin of the old data
            # (complete file name)
            # the first data point should be from the latest data
            # resulting data.fcounts = [t1: 3, t2: 2]
            assert (p.data["fcounts"] == [3, 2]).all()


@pytest.mark.skipif(sys.platform.startswith("win"), reason="does not run on windows")
@pytest.mark.skipif(sys.platform == "Darwin", reason="does not run on mac")
def test_fits_incomplete_switch_over_remove_dup_files(out_dir):
    test_fits_incomplete_switch_over(out_dir)

    fits_dir = out_dir / "fits"
    target_dir = out_dir / "move"

    ufiles = sorted(list(fits_dir.rglob("*_V*U.fits")))
    p = re.compile(r".*_V[0-9]+\.fits")
    cfiles = sorted([f for f in fits_dir.rglob("*.fits") if p.match(f.name)])

    assert len(ufiles) == 3
    assert len(cfiles) == 4

    ufiles[0].unlink()
    ufiles[1].unlink()
    cfiles[1].unlink()

    res = incomplete_twins(["--fits_dir", str(fits_dir), "--twin_target", str(target_dir), "--move_twin"])

    assert res
    assert len(res) == 1
    moved = list(target_dir.rglob("*.fits"))
    assert len(moved) == 1
    assert moved[0].name == cfiles[3].name


@patch("stixcore.products.level1.scienceL1.Spectrogram")
def test_publish_fits_to_esa(product, out_dir):
    target_dir = out_dir / "esa"
    same_dir = out_dir / "same"
    fits_dir = out_dir / "fits"
    same_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    fits_dir.mkdir(parents=True, exist_ok=True)

    processor = FitsL1Processor(fits_dir)
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2**15)
    product.scet_timerange = SCETimeRange(start=beg, end=end)
    product.utc_timerange = product.scet_timerange.to_timerange()
    product.idb_versions = {"1.2": product.scet_timerange}
    product.control = QTable(
        {
            "scet_coarse": [[beg.coarse, end.coarse]],
            "scet_fine": [[beg.fine, end.fine]],
            "index": [1],
            "request_id": [123],
        }
    )

    t = SCETime(coarse=[beg.coarse, end.coarse])
    product.data = QTable({"time": t, "timedel": t - beg, "fcounts": np.array([1, 2]), "control_index": [1, 1]})
    product.raw = ["packet1.xml", "packet2.xml"]
    product.parent = ["packet1.xml", "packet2.xml"]
    product.level = "L1"
    product.service_type = 21
    product.service_subtype = 6
    product.ssid = 24
    product.type = "sci"
    product.name = "xray-spec"
    product.obt_beg = beg
    product.obt_end = end
    product.date_obs = beg
    product.date_beg = beg
    product.date_end = end
    product.exposure = 2
    product.max_exposure = 3
    product.dmin = 2
    product.dmax = 3
    product.bunit = "count"
    product.split_to_files.return_value = [product]
    product.get_processing_version.return_value = 2
    product.get_energies = False

    data = product.data[:]  # make a clone
    files = []
    product.control["request_id"] = 123
    files.extend(processor.write_fits(product))  # orig

    product.control["request_id"] = 124
    product.data = data[:]
    files.extend(processor.write_fits(product))  # same ignore

    product.control["request_id"] = 125
    product.data = data[:]
    files.extend(processor.write_fits(product))  # same ignore

    product.control["request_id"] = 126
    product.data = data[:]
    product.data["fcounts"][0] = 100
    files.extend(processor.write_fits(product))  # sub1

    product.control["request_id"] = 127
    product.data = data[:]
    product.data["fcounts"][0] = 200
    files.extend(processor.write_fits(product))  # sub2

    product.control["request_id"] = 128
    product.data = data[:]
    product.data["fcounts"][0] = 300
    files.extend(processor.write_fits(product))  # sub3 -> ERROR

    product.control["request_id"] = 129
    product.data = data[:]
    product.data["fcounts"][0] = 400
    files.extend(processor.write_fits(product))  # sub4 -> ERROR

    product.control["request_id"] = 130
    product.data = data[:]
    product.data["fcounts"][0] = 400
    product.ssid = 23
    product.type = "sci"
    product.name = "xray-vis"
    files.extend(processor.write_fits(product))  # blacklist

    with open(out_dir / "blacklist.txt", "w") as blacklist:
        blacklist.write(files[-1].name + "\n")

    supplement_report = out_dir / "supplement_report.csv"

    res = publish_fits_to_esa(
        [
            "--target_dir",
            str(target_dir),
            "--same_esa_name_dir",
            str(same_dir),
            "--include_levels",
            "l1",
            "--sort_files",
            # '--update_rid_lut',
            "--supplement_report",
            str(supplement_report),
            "--blacklist_files",
            str(out_dir / "blacklist.txt"),
            "--waiting_period",
            "0s",
            "--db_file",
            str(out_dir / "test.sqlite"),
            "--fits_dir",
            str(fits_dir),
        ]
    )

    assert res
    # the first one was added
    assert len(res[PublishResult.PUBLISHED]) == 1
    # 2 where published as supplement
    assert len(res[PublishResult.MODIFIED]) == 2
    # 2 where ignored as the data is the same
    assert len(res[PublishResult.IGNORED]) == 2
    # 1 where added to blacklist
    assert len(res[PublishResult.BLACKLISTED]) == 1
    # 2 errors as it would be a third/more supplement
    assert len(res[PublishResult.ERROR]) == 2
    assert res[PublishResult.ERROR][0][1] == "max supplement error"

    # check if the FILENAME keyword was replaced
    for f in target_dir.glob("*.fits"):
        fncw = fits.getval(f, "FILENAME")
        assert fncw == f.name

    assert supplement_report.exists()

    # publish again this time without blacklist
    # res2 = publish_fits_to_esa(['--target_dir', str(target_dir),
    #                             '--same_esa_name_dir', str(same_dir),
    #                             '--include_levels', 'l1',
    #                             '--sort_files',
    #                             '--supplement_report', str(supplement_report),
    #                             '--waiting_period', '0s',
    #                             '--db_file', str(out_dir / "test.sqlite"),
    #                             '--fits_dir', str(fits_dir)])

    # nothing new after rerun
    # assert len(res2) == 0

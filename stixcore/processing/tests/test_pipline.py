import re
import time
import shutil

import pytest
from watchdog.observers import Observer

from stixcore.processing.pipeline import GFTSFileHandler
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


def processing_tm_file(path, **args):
    logger.info(f"start processing tm file: {path}")
    time.sleep(2)
    logger.info(f"stop processing tm file: {path}")


@pytest.fixture
def gfts_manager():
    return GFTSFileHandler(processing_tm_file, re.compile(r".*test_[0-9]*.tm$"), name="w-dog-test")


def test_gfts_manager_watchdog(out_dir, gfts_manager):
    (out_dir / "test_1.tm").touch()
    (out_dir / "test_2.tm").touch()
    (out_dir / "test_3.tm").touch()
    (out_dir / "test_4.tmp").touch()
    (out_dir / "test_5.tmp").touch()
    (out_dir / "test_6.tmp").touch()

    observer = Observer()
    observer.schedule(gfts_manager, out_dir, recursive=False)
    observer.start()

    assert gfts_manager.queue.qsize() == 0

    # 3 files are in the base dir
    # simulate start with unprocessed
    gfts_manager.add_to_queue(out_dir.glob("*.tm"))
    assert gfts_manager.queue.qsize() == 3

    # now some tm in coming in
    time.sleep(1)
    shutil.move((out_dir / "test_4.tmp"), (out_dir / "test_4.tm"))
    time.sleep(1)
    shutil.move((out_dir / "test_5.tmp"), (out_dir / "test_5.tm"))
    time.sleep(1)
    shutil.move((out_dir / "test_6.tmp"), (out_dir / "test_6.tm"))
    time.sleep(1)

    openfiles = gfts_manager.queue.qsize()
    assert openfiles > 2
    time.sleep(20)
    assert gfts_manager.queue.qsize() < openfiles
    observer.stop()

import os
import shutil
from pathlib import Path

import pytest

from stixcore.io.fits.processors import FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.products.levelb.binary import LevelB
from stixcore.tmtc.packets import TMTC


@pytest.fixture
def soc_manager():
    return SOCManager(Path(os.path.abspath(__file__)).parent.parent.parent /
                      "io" / "tests" / "data")


@pytest.fixture
def out_dir():
    out_dir = Path(os.path.abspath(__file__)).parent.parent.parent / "io" / "tests" / "data" / "out"
    if not out_dir.exists():
        os.makedirs(out_dir)
    return out_dir


def teardown_function():
    out_dir = Path(os.path.abspath(__file__)).parent.parent.parent / "io" / "tests" / "data" / "out"
    if out_dir.exists():
        shutil.rmtree(str(out_dir))


def test_level_b(soc_manager, out_dir):
    fits_processor = FitsLBProcessor(out_dir)

    files_to_process = soc_manager.get_files(TMTC.TM)
    for tmtc_file in files_to_process:
        LevelB.from_tm(tmtc_file, fits_processor)
        # do again for __add__
        LevelB.from_tm(tmtc_file, fits_processor)

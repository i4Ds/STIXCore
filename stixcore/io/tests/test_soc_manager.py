import os
from pathlib import Path

import pytest

from stixcore.io.soc.manager import SOCManager, SOCPacketFile
from stixcore.tmtc.packets import TMTC


@pytest.fixture
def soc_manager():
    return SOCManager(Path(os.path.abspath(__file__)).parent / 'data')


@pytest.fixture
def base_dir():
    return Path(os.path.abspath(__file__)).parent / 'data'


def test_soc_manager(soc_manager):
    assert str(soc_manager.data_root) == str((Path(os.path.abspath(__file__)).parent / 'data'))


def test_root_not_found_error():
    with pytest.raises(ValueError) as e:
        _ = SOCManager(".foo/")
    assert str(e.value).startswith('path not found')


def test_get_files(soc_manager):
    files = soc_manager.get_files(TMTC.TM)
    for file in files:
        i = 0
        for binary in file.get_packet_binaries():
            i += 1
        assert i > 0


def test_soc_file(base_dir):
    sf = SOCPacketFile(Path(base_dir / "PktTmRaw.xml"))
    assert sf.tmtc == TMTC.TM

    sf = SOCPacketFile(Path(base_dir / "PktTcReport.xml"))
    assert sf.tmtc == TMTC.TC

    with pytest.raises(ValueError) as e:
        _ = SOCPacketFile(".foo/")
        assert str(e.value).startswith('path not found')

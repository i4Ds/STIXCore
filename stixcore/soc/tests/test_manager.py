import os
from pathlib import Path

import pytest

from stixcore.soc.manager import SOCManager


@pytest.fixture
def soc_manager():
    return SOCManager(Path(os.path.abspath(__file__)).parent / 'data')


def test_soc_manager(soc_manager):
    assert str(soc_manager.data_root) == str((Path(os.path.abspath(__file__)).parent / 'data'))


def test_root_not_found_error():
    with pytest.raises(ValueError) as e:
        _ = SOCManager(".foo/")
    assert str(e.value).startswith('path not found')

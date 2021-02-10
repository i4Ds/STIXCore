from pathlib import Path

import pytest

from stixcore.config.data_types import EnergyChannel
from stixcore.config.reader import read_energy_channels


@pytest.fixture
def path():
    return Path(__file__).parent.parent / "data" / "common"


def test_module_available(path):
    assert path.exists()
    assert (path / "README.md").exists()


def test_read_energy_channels(path):
    ec = read_energy_channels(path / "detector" / "ScienceEnergyChannels_1000.csv")
    assert len(ec) == 32
    assert isinstance(ec[1], EnergyChannel)

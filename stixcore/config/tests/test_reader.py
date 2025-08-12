from pathlib import Path
from datetime import datetime

import pytest
from numpy.testing import assert_equal

import astropy.units as u
from astropy.table import QTable

from stixcore.config.reader import get_sci_channels, read_sci_energy_channels


def test_read_sci_energy_channels():
    path = Path(__file__).parent.parent / "data" / "common" / "detector" / "ScienceEnergyChannels_1000.csv"
    sci_channels = read_sci_energy_channels(path)
    assert len(sci_channels) == 33
    assert_equal(sci_channels["Elower"][1], 4.0 * u.keV)
    assert_equal(sci_channels["Eupper"][1], 5.0 * u.keV)
    assert_equal(sci_channels["dE/E"][1], 0.222)


def test_get_sci_channels():
    sci_channels = get_sci_channels(datetime.now())
    assert isinstance(sci_channels, QTable)
    assert len(sci_channels) == 33
    assert_equal(sci_channels["Elower"][1], 4.0 * u.keV)
    assert_equal(sci_channels["Eupper"][1], 5.0 * u.keV)

    sci_channels = get_sci_channels(datetime(2023, 1, 30, 12))
    assert isinstance(sci_channels, QTable)
    assert len(sci_channels) == 33
    assert_equal(sci_channels["Elower"][1], 4.0 * u.keV)
    assert_equal(sci_channels["Eupper"][1], 4.45 * u.keV)

    with pytest.raises(ValueError, match=r"No Science Energy.*"):
        get_sci_channels(datetime(2018, 1, 1))

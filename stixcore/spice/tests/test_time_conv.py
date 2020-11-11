import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from stixcore.spice.manager import SpiceManager

T0_UTC_ISO = '2000-01-01T00:00:00.000+00:00'
T0_SCET = '1/0000000000:00000'
T0_DATETIME = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

orig_directory = ''


# For meta kernel to work have to be in the same directory as the kernels or set the PATH variable
# in the MK to the correct value as we don't know where this during testing the setup and teardown
# function will change to and form this directory
def setup_function():
    global orig_directory
    orig_directory = os.getcwd()
    file_dir = Path(os.path.abspath(__file__))
    os.chdir(file_dir.parent / 'data')


def teardown_function():
    os.chdir(orig_directory)


@pytest.fixture
def spicemanager():
    return SpiceManager(mk_path='test_20201001_V01.mk')


def test_spicemanager(spicemanager):
    assert spicemanager.kernel_date == datetime(2020, 10, 1)


# Compare to know fixed SCET T0 value
def test_utc_to_scet_t0(spicemanager):
    assert spicemanager.utc_to_scet(T0_UTC_ISO[:-6]) == T0_SCET


# Compare to know fixed UTC ISO T0 value
def test_scet_to_utc_t0(spicemanager):
    assert spicemanager.scet_to_utc(T0_SCET) == T0_UTC_ISO[:-6]


def test_scet_to_utc_round_trips(spicemanager):
    atime = datetime(year=2020, month=10, day=15, hour=13, minute=33, microsecond=123456)
    atime_str = atime.isoformat(timespec='milliseconds')
    assert spicemanager.utc_to_scet(spicemanager.scet_to_utc(T0_SCET)) == T0_SCET
    assert spicemanager.scet_to_utc(spicemanager.utc_to_scet(T0_UTC_ISO[:-6])) == T0_UTC_ISO[:-6]
    assert spicemanager.scet_to_utc(spicemanager.utc_to_scet(atime_str)) == atime_str


def test_scet_to_datetime_round_trips(spicemanager):
    atime = datetime(year=2020, month=10, day=15, hour=13, minute=33, microsecond=123456,
                     tzinfo=timezone.utc)

    assert spicemanager.scet_to_datetime(spicemanager.datetime_to_scet(T0_DATETIME)) == T0_DATETIME
    assert spicemanager.datetime_to_scet(spicemanager.scet_to_datetime(T0_SCET)) == T0_SCET
    # Only have 3 significant figures in milliseconds precision
    dt = spicemanager.scet_to_datetime(spicemanager.datetime_to_scet(atime)) - atime
    assert dt.total_seconds() < 1e-3

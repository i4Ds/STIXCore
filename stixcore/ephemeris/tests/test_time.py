from datetime import datetime, timezone

import pytest

from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Time

T0_UTC_ISO = '2000-01-01T00:00:00.000+00:00'
T0_SCET = '1/0000000000:00000'
T0_DATETIME = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def spicemanager():
    return Time(meta_kernel_path=test_data.ephemeris.META_KERNEL_POS)


# def test_spicemanager(spicemanager):
#     assert spicemanager.kernel_date == datetime(2020, 10, 1)


# Compare to know fixed SCET T0 value
def test_utc_to_scet_t0(spicemanager):
    with spicemanager as spice:
        assert spice.utc_to_scet(T0_UTC_ISO[:-6]) == T0_SCET


# Compare to know fixed UTC ISO T0 value
def test_scet_to_utc_t0(spicemanager):
    with spicemanager as spice:
        res_string = spice.scet_to_utc(T0_SCET)
        res_number = spice.scet_to_utc(0.0)
    assert res_number == res_string
    assert res_string == T0_UTC_ISO[:-6]


def test_scet_to_datetime_t0(spicemanager):
    with spicemanager as spice:
        res_string = spice.scet_to_datetime(T0_SCET)
        res_number = spice.scet_to_datetime(0.0)
    assert res_number == res_string
    assert res_number == T0_DATETIME


def test_scet_to_utc_round_trips(spicemanager):

    atime = datetime(year=2020, month=10, day=15, hour=13, minute=33, microsecond=123456)
    atime_str = atime.isoformat(timespec='milliseconds')
    with spicemanager as spice:
        assert spice.utc_to_scet(spicemanager.scet_to_utc(T0_SCET)) == T0_SCET
        assert spice.scet_to_utc(spicemanager.utc_to_scet(T0_UTC_ISO[:-6])) == T0_UTC_ISO[:-6]
        assert spice.scet_to_utc(spicemanager.utc_to_scet(atime_str)) == atime_str


def test_scet_to_datetime_round_trips(spicemanager):
    atime = datetime(year=2020, month=10, day=15, hour=13, minute=33, microsecond=123456,
                     tzinfo=timezone.utc)

    with spicemanager as spice:
        assert spice.scet_to_datetime(spicemanager.datetime_to_scet(T0_DATETIME)) == T0_DATETIME
        assert spice.datetime_to_scet(spicemanager.scet_to_datetime(T0_SCET)) == T0_SCET
    # Only have 3 significant figures in milliseconds precision
        dt = spice.scet_to_datetime(spicemanager.datetime_to_scet(atime)) - atime
        assert dt.total_seconds() < 1e-3

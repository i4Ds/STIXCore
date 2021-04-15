from datetime import datetime, timezone

import astropy.units as u

from stixcore.datetime.datetime import SCETime, SCETimeRange


def test_init():
    dt = SCETime(coarse=0, fine=0)
    assert dt.to_datetime() == datetime(2000, 1, 1, 0, tzinfo=timezone.utc)


def test_as_float():
    dt = SCETime(coarse=1, fine=0)
    assert dt.as_float() == 1.0 * u.s


def test_from_float():
    dt = SCETime.from_float(1.0 * u.s)
    assert dt == SCETime(coarse=1, fine=0)


def test_to_str():
    dt = SCETime(coarse=123, fine=45)
    assert dt == SCETime.from_string(str(dt))


def test_lt():
    dt = SCETime(coarse=123, fine=45)
    dt2 = SCETime(coarse=124, fine=45)
    assert dt < dt2
    assert dt <= dt2
    assert dt2 > dt
    assert dt2 >= dt
    assert dt2 is not dt
    assert dt2 == SCETime.from_string(str(dt2))


def test_minmax_time():
    assert SCETime.min_time() == SCETime(coarse=0, fine=0)
    assert SCETime.max_time() == SCETime(coarse=(2**32)-1, fine=(2**16)-1)
    # TODO enable after https://github.com/i4Ds/STIXCore/issues/102
    # assert SCETime.min_time() - SCETime(coarse=0, fine=1) == SCETime.min_time()
    assert SCETime.max_time() + SCETime(coarse=0, fine=1) == SCETime(coarse=0, fine=0)


def test_time_range():
    tr = SCETimeRange(start=SCETime(coarse=100, fine=0), end=SCETime(coarse=200, fine=0))
    tp_in = SCETime(coarse=150, fine=0)
    tr_in = SCETimeRange(start=SCETime(coarse=150, fine=0), end=SCETime(coarse=160, fine=0))
    tr_out = SCETimeRange(start=SCETime(coarse=150, fine=0), end=SCETime(coarse=250, fine=0))
    tp_out = SCETime(coarse=250, fine=0)
    assert tp_in in tr
    assert tp_out not in tr
    assert tr_in in tr
    assert tr_out not in tr

    tr.expand(tp_out)
    tr.expand(tr_out)

    assert tp_out in tr
    assert tr_out in tr

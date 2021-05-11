from datetime import datetime, timezone

import numpy as np
import pytest

import astropy.units as u

from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.time.datetime import MAX_COARSE, MAX_FINE


def test_time_init():
    t1 = SCETime(0, 0)
    t2 = SCETime.from_float(0*u.s)
    t3 = SCETime(t1)
    assert t1 == t2
    assert t2 == t3
    assert t1 == t3

    with pytest.raises(ValueError, match=r'Coarse time must be in range.*'):
        SCETime(-1, 0)

    with pytest.raises(ValueError, match=r'Fine time must be in range.*'):
        SCETime(0, -1)

    with pytest.raises(ValueError):
        _ = SCETime(2 ** 44-1, 0)

    with pytest.raises(ValueError):
        SCETime(0, 2**16+1)

    with pytest.raises(ValueError):
        SCETime(0.0, 0)


def test_time_to_datetime():
    dt = SCETime(coarse=0, fine=0)
    assert dt.to_datetime() == datetime(2000, 1, 1, 0, tzinfo=timezone.utc)


def test_time_as_float():
    dt = SCETime(coarse=1, fine=0)
    assert dt.as_float() == 1.0 * u.s


def test_time_from_float():
    dt = SCETime.from_float(1 * u.s)
    assert dt == SCETime(coarse=1, fine=0)


def test_time_to_str():
    dt = SCETime(coarse=123, fine=45)
    assert dt == SCETime.from_string(str(dt))


def test_time_add():
    t1 = SCETime(123, 456)
    with pytest.raises(TypeError, match=r'Only Quantities and SCETimeDelta.*'):
        _ = t1 + SCETime(0, 1)
    with pytest.raises(ValueError, match=r'.*are not convertible'):
        _ = t1 + (1*u.m)

    # test right add
    t2 = t1 + (1 + 1/MAX_FINE) * u.s
    # test left add
    t3 = (1 + 1/MAX_FINE) * u.s + t1
    assert t2 == t3
    assert t2.coarse == 124
    assert t2.fine == 457


def test_time_sub():
    t1 = SCETime(123, 456)
    with pytest.raises(TypeError, match=r'Only quantities, SCETime and SCETimeDelt.*'):
        _ = t1 - 1
    with pytest.raises(ValueError, match=r'.*are not convertible'):
        _ = t1 + (1*u.m)

    # test sub
    t2 = t1 - (1 + 1/MAX_FINE) * u.s
    assert t2.coarse == 122
    assert t2.fine == 455
    # test rsub
    with pytest.raises(TypeError, match=r'unsupported operand.*'):
        t2 = (1 + 1/MAX_FINE) * u.s - t1

    # Test subtract to times
    dt = t1 - t2
    assert isinstance(dt, SCETimeDelta)
    assert dt.coarse == 1
    assert dt.fine == 1

    dt = t2 - t1
    assert isinstance(dt, SCETimeDelta)
    assert dt.coarse == -1
    assert dt.fine == -1

    # Test subtract deltatime
    t3 = t2 - dt
    assert isinstance(t3, SCETime)
    assert t3.coarse == 123
    assert t3.fine == 456

    # Can't subtract time from a delta time
    with pytest.raises(TypeError, match=r'Unsupported operation for '
                                        r'types SCETimeDelta and SCETime'):
        _ = dt - t1


def test_time_eq():
    t1 = SCETime(123, 456)
    t2 = SCETime.from_float((123 + 456/MAX_FINE)*u.s)
    t3 = SCETime(765, 432)
    assert t1 == t2
    assert t1 != t3


def test_time_broadcast():
    t = SCETime(0, 0)
    t1 = t + np.arange(100) * u.s
    t2 = SCETime(np.arange(100, dtype=np.int), 0)
    t3 = SCETime(0, np.arange(100, dtype=np.int))
    assert t1.shape == (100,)
    assert t2.shape == (100,)
    assert t3.shape == (100,)


def test_time_lt():
    dt = SCETime(coarse=123, fine=45)
    dt2 = SCETime(coarse=124, fine=45)
    assert dt < dt2
    assert dt <= dt2
    assert dt2 > dt
    assert dt2 >= dt
    assert dt2 is not dt
    assert dt2 == SCETime.from_string(str(dt2))


def test_time_minmax():
    assert SCETime.min_time() == SCETime(coarse=0, fine=0)
    assert SCETime.max_time() == SCETime(coarse=MAX_COARSE, fine=MAX_FINE)
    # TODO enable after https://github.com/i4Ds/STIXCore/issues/102
    # assert SCETime.min_time() - SCETime(coarse=0, fine=1) == SCETime.min_time()
    with pytest.raises(ValueError, match=r'Coarse time must be in range.*'):
        m = SCETime.max_time()
        dt = SCETimeDelta(0, 1)
        nm = m + dt
        print(nm)


def test_timedelta_init():
    dt1 = SCETimeDelta(0, 0)
    dt2 = SCETimeDelta.from_float(0*u.s)
    dt3 = SCETimeDelta(dt1)
    assert dt1 == dt2
    assert dt2 == dt3
    assert dt1 == dt3

    with pytest.raises(ValueError):
        _ = SCETimeDelta(2 ** 32 + 1, 0)

    with pytest.raises(ValueError):
        SCETime(0, 2**16+1)

    with pytest.raises(ValueError):
        SCETime(0.0, 0)


def test_timedelta_as_float():
    dt = SCETimeDelta(coarse=-1, fine=0)
    assert dt.as_float() == -1.0 * u.s


def test_timedelta_from_float():
    dt = SCETimeDelta.from_float(-1 * u.s)
    assert dt == SCETimeDelta(coarse=-1, fine=0)


def test_timedelta_add():
    t1 = SCETime(1, 1)
    dt1 = SCETimeDelta(100, 1)
    dt2 = SCETimeDelta(200, 2)

    # test time plus timedelta
    t1_dt1 = dt1 + t1
    dt1_t1 = t1 + dt1
    assert t1_dt1 == dt1_t1
    assert t1_dt1.coarse == 101
    assert t1_dt1.fine == 2

    with pytest.raises(ValueError, match=f'.*are not convertible'):
        _ = dt1 + (1*u.m)

    # test timedelta plus timedelta/quantity
    dt1_dt2 = dt1 + dt2
    dt1_float = dt1 + (200+2/MAX_FINE)*u.s
    dt2_dt1 = dt2 + dt1
    float_dt2 = (100 + 1/MAX_FINE) * u.s + dt2
    assert dt1_dt2 == dt2_dt1
    assert dt1_float == dt1_dt2
    assert float_dt2 == dt1_dt2
    assert dt1_dt2.coarse == 300
    assert dt1_dt2.fine == 3


def test_deltatime_sub():
    t1 = SCETime(100, 2)
    dt1 = SCETimeDelta(100, 1)
    dt2 = SCETimeDelta(200, 2)

    with pytest.raises(TypeError, match=r'Unsupported operation for types SCETimeDelta and int'):
        _ = dt1 - 1
    with pytest.raises(TypeError, match=r'Quantity could not be converted to SCETimeDelta'):
        _ = dt1 - (1*u.m)

    # test sub deltatimes and quantities
    dt1_dt2 = dt1 - dt2
    dt1_float = dt1 - (200 + 2 / MAX_FINE) * u.s
    assert dt1_dt2 == dt1_float
    assert dt1_dt2.coarse == -100
    assert dt1_dt2.fine == -1

    dt2_dt1 = dt2 - dt1
    float_dt1 = (200 + 2/MAX_FINE) * u.s - dt1
    assert dt2_dt1 == float_dt1
    assert dt2_dt1.coarse == 100
    assert dt2_dt1.fine == 1

    # test sub times
    with pytest.raises(TypeError, match=f'Unsupported operation for types.*'):
        dt1 - t1

    t2 = t1 - dt1
    assert t2.coarse == 0
    assert t2.fine == 1
    with pytest.raises(ValueError, match=r'Coarse time must be in range.*'):
        t1 - dt2


def test_timedelta_eq():
    dt1 = SCETimeDelta(123, 456)
    dt2 = SCETimeDelta((123 + 456/MAX_FINE)*u.s)
    dt3 = SCETimeDelta(-1, -1)
    assert dt1 == dt2
    assert dt1 != dt3
    assert dt1 == (123 + 456/MAX_FINE)*u.s


def test_timerange():
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

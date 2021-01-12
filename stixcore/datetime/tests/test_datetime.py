from datetime import datetime, timezone

from stixcore.datetime.datetime import DateTime


def test_init():
    dt = DateTime(coarse=0, fine=0)
    assert dt.to_datetime() == datetime(2000, 1, 1, 0, tzinfo=timezone.utc)

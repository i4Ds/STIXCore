from datetime import datetime

import pytest

from astropy.time import Time
from astropy.units import Quantity

from stixcore.data.test import test_data
from stixcore.products.level0.quicklookL0 import LightCurve as LCL0
from stixcore.products.level1.quicklookL1 import LightCurve as LCL1
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.time import SCETime
from stixcore.time.datetime import SCETimeDelta


def test_ql_lb():
    lb_fits = test_data.products.LB_21_6_30_fits
    lb_prod = Product(lb_fits)
    assert isinstance(lb_prod, LevelB)
    assert lb_prod.level == "LB"
    assert lb_prod.service_type == 21
    assert lb_prod.service_subtype == 6
    assert lb_prod.ssid == 30
    # TODO not really a test just from output
    assert lb_prod.obt_beg == SCETime(coarse=664148503, fine=10710)


def test_read_timeformat():
    lq_scet = Product(test_data.products.L1_LightCurve_fits[0])

    assert type(lq_scet.data["time"][0]) is SCETime
    assert type(lq_scet.data["timedel"][0]) is SCETimeDelta

    lq_utc = Product(test_data.products.L1_LightCurve_fits[0], get_timeformat_from_TIMESYS=True)
    assert type(lq_utc.data["time"][0]) is Time
    assert type(lq_utc.data["timedel"][0]) is Quantity

    assert abs((lq_scet.scet_timerange.start - lq_utc.scet_timerange.start).coarse) < 1
    assert abs((lq_scet.scet_timerange.end - lq_utc.scet_timerange.end).coarse) < 1
    assert abs((lq_scet.utc_timerange.start - lq_utc.utc_timerange.start).to("s").value) < 0.2
    assert abs((lq_scet.utc_timerange.end - lq_utc.utc_timerange.end).to("s").value) < 0.2


# The fits file times maybe off by onescet time bin need to regenerate and test
@pytest.mark.xfail
def test_ql_l0():
    l0_fits = test_data.products.L0_LightCurve_fits
    l0_prod = Product(l0_fits)
    assert isinstance(l0_prod, LCL0)
    assert l0_prod.level == "L0"
    assert l0_prod.service_type == 21
    assert l0_prod.service_subtype == 6
    assert l0_prod.ssid == 30
    # TODO not really a test just from output
    assert l0_prod.obs_beg == SCETime(coarse=664146182, fine=58989)


@pytest.mark.xfail
def test_ql_l1():
    l1_fits = test_data.products.L1_LightCurve_fits
    l1_prod = Product(l1_fits)
    assert isinstance(l1_prod, LCL1)
    assert l1_prod.level == "L1"
    assert l1_prod.service_type == 21
    assert l1_prod.service_subtype == 6
    assert l1_prod.ssid == 30
    # TODO not really a test just from output
    assert l1_prod.obs_beg.datetime == datetime(2021, 1, 16, 23, 59, 59, 362000)

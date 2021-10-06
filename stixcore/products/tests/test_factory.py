from datetime import datetime

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0.quicklookL0 import LightCurve as LCL0
from stixcore.products.level1.quicklookL1 import LightCurve as LCL1
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.time import SCETime


def test_ql_lb():
    lb_fits = test_data.products.LB_21_6_30_fits
    lb_prod = Product(lb_fits)
    assert isinstance(lb_prod, LevelB)
    assert lb_prod.level == 'LB'
    assert lb_prod.service_type == 21
    assert lb_prod.service_subtype == 6
    assert lb_prod.ssid == 30
    # TODO not really a test just from output
    assert lb_prod.obt_beg == SCETime(coarse=664148503, fine=10710)


# The fits file times maybe off by onescet time bin need to regenerate and test
@pytest.mark.xfail
def test_ql_l0():
    l0_fits = test_data.products.L0_LightCurve_fits
    l0_prod = Product(l0_fits)
    assert isinstance(l0_prod, LCL0)
    assert l0_prod.level == 'L0'
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
    assert l1_prod.level == 'L1'
    assert l1_prod.service_type == 21
    assert l1_prod.service_subtype == 6
    assert l1_prod.ssid == 30
    # TODO not really a test just from output
    assert l1_prod.obs_beg.datetime == datetime(2021, 1, 16, 23, 59, 59, 362000)

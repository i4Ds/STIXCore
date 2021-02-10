from pathlib import Path
from datetime import datetime

from stixcore.datetime.datetime import DateTime
from stixcore.products.level0.quicklook import LightCurve as LCL0
from stixcore.products.level1.quicklook import LightCurve as LCL1
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product


def test_ql_lb():
    lb_fits = Path(__file__).parent / 'data' / 'solo_LB_stix-21-6-30_0664156800_V01.fits'
    lb_prod = Product(lb_fits)
    assert isinstance(lb_prod, LevelB)
    assert lb_prod.level == 'LB'
    assert lb_prod.service_type == 21
    assert lb_prod.service_subtype == 6
    assert lb_prod.ssid == 30
    # TODO not really a test just from output
    assert lb_prod.obt_beg == DateTime(coarse=664148503, fine=10710)


def test_ql_l0():
    l0_fits = Path(__file__).parent / 'data' / 'solo_L0_stix-ql-LightCurve_0664070400_V01.fits'
    l0_prod = Product(l0_fits)
    assert isinstance(l0_prod, LCL0)
    assert l0_prod.level == 'L0'
    assert l0_prod.service_type == 21
    assert l0_prod.service_subtype == 6
    assert l0_prod.ssid == 30
    # TODO not really a test just from output
    assert l0_prod.obs_beg == DateTime(coarse=664146182, fine=58989)


def test_ql_l1():
    l1_fits = Path(__file__).parent / 'data' / 'solo_L1_stix-ql-LightCurve_20210117_V01.fits'
    l1_prod = Product(l1_fits)
    assert isinstance(l1_prod, LCL1)
    assert l1_prod.level == 'L1'
    assert l1_prod.service_type == 21
    assert l1_prod.service_subtype == 6
    assert l1_prod.ssid == 30
    # TODO not really a test just from output
    assert l1_prod.obs_beg.datetime == datetime(2021, 1, 16, 23, 59, 59, 362000)

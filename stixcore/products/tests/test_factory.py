from stixcore.products.product import Product


def test_ql_l0():
    path = '/Users/shane/Projects/STIX/dataview/data/test_meta/L0/21/6/30/' \
           'solo_L0_stix-ql-LightCurve_0663984000_V01.fits'
    Product(path)
    print('a')


def test_ql_l1():
    path = '/Users/shane/Projects/STIX/dataview/data/test_new/L1/2021/01/17/QL/' \
           'solo_L1_stix-ql-LightCurve_20210117_V01.fits'
    Product(path)
    print('a')

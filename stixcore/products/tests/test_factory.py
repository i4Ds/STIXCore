from stixcore.products.product import Product


def test_ql_l0():
    path = '/Users/shane/Projects/STIX/dataview/data/asdfadsf/' \
           'LB/21/6/30/solo_LB_stix-21-6-30_640396800_V01.fits'
    Product(path)
    print('a')


def test_ql_l1():
    path = '/Users/shane/Projects/STIX/dataview/data/asdfadsf/' \
           'L1/2020/04/17/QL/solo_L1_stix-ql-LightCurve_20200417_V01.fits'
    Product(path)
    print('a')

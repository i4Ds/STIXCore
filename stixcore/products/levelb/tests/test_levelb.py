
import numpy as np

from stixcore.data.test import test_data
from stixcore.products import Product
from stixcore.products.levelb.binary import LevelB


def test_slice():
    lb_fits = test_data.products.LB_21_6_30_fits
    lb_prod = Product(lb_fits)
    assert isinstance(lb_prod, LevelB)

    slice = lb_prod[0]
    assert np.all(slice.data == lb_prod.data[0]) and np.all(slice.data == lb_prod.data[0])
    slice = lb_prod[-1]
    assert np.all(slice.data == lb_prod.data[-1]) and np.all(slice.data == lb_prod.data[-1])
    slice = lb_prod[0:2]
    assert np.all(slice.data == lb_prod.data[0:2]) and np.all(slice.data == lb_prod.data[0:2])
    slice = lb_prod[[1, 2, 3]]
    assert np.all(slice.data == lb_prod.data[[1, 2, 3]]) \
        and np.all(slice.data == lb_prod.data[[1, 2, 3]])


def test_add():
    lb_fits = test_data.products.LB_21_6_30_fits
    prod1 = Product(lb_fits)
    prod2 = Product(lb_fits)
    # Make sure equal to start
    assert np.all(prod1.control == prod2.control)
    assert np.all(prod1.data == prod2.data)
    res = prod1 + prod2
    # Make sure original data isn't changed
    assert np.all(prod1.control == prod2.control)
    assert np.all(prod1.data == prod2.data)

    # res, prob1 and prod2 should all be equal
    assert np.all(prod1.data == res.data)
    assert np.all(prod1.control == res.control)
    assert np.all(prod2.data == res.data)
    assert np.all(prod2.control == res.control)

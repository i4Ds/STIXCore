import re
import pickle
from pathlib import Path
from unittest.mock import patch

import numpy as np

from stixcore.data.test import test_data
from stixcore.products.level0.quicklook import LightCurve as L0LightCurve
from stixcore.products.level1.quicklook import LightCurve as L1LightCurve


@patch('stixcore.products.levelb.binary.LevelB')
def test_lightcurve(levelb):
    hex_file = test_data.tmtc.TM_21_6_30
    with hex_file.open('r') as file:
        hex = file.read()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", hex)]

    ql_lc_product_l0 = L0LightCurve.from_levelb(levelb)

    # pickle.dump(ql_lc_product, open(Path("d:/ql.pickle"), "wb"))
    comp = pickle.load(open(Path("d:/ql.pickle"), "rb"))

    dc = ql_lc_product_l0.data['time'] == comp.data['time']
    pc = ql_lc_product_l0.control == comp.control

    assert np.all(dc)
    assert np.all(pc)

    assert ql_lc_product_l0.level == 'L0'
    assert ql_lc_product_l0.name == 'LightCurve'
    assert len(ql_lc_product_l0.control) == 1
    assert len(ql_lc_product_l0.data) == ql_lc_product_l0.control['num_samples']

    ql_lc_product_l1 = L1LightCurve.from_level0(ql_lc_product_l0)
    assert ql_lc_product_l1.level == 'L1'
    assert ql_lc_product_l1.name == 'LightCurve'
    assert len(ql_lc_product_l1.control) == 1
    assert len(ql_lc_product_l1.data) == ql_lc_product_l1.control['num_samples']

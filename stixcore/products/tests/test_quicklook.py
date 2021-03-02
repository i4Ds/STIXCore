import re
from unittest.mock import patch

from stixcore.data.test import test_data
from stixcore.products.level0.quicklook import LightCurve


@patch('stixcore.products.levelb.binary.LevelB')
def test_lightcurve(levelb):
    hex_file = test_data.tmtc.TM_21_6_30
    with hex_file.open('r') as file:
        hex = file.read()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", hex)]

    ql_lc_product = LightCurve.from_levelb(levelb)
    assert ql_lc_product.level == 'L0'
    assert ql_lc_product.name == 'LightCurve'
    assert len(ql_lc_product.control) == 1
    assert len(ql_lc_product.data) == ql_lc_product.control['num_samples']

import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0 import quicklook as qll0
from stixcore.products.level1 import quicklook as qll1

testpackets = [(test_data.tmtc.TM_21_6_30, qll0.LightCurve, 'ql-lightcurve',
                '0659402030:00007', '0659402958:00007', 232),
               (test_data.tmtc.TM_21_6_31, qll0.Background, 'ql-background',
                '0659399870:00253', '0659402958:00253', 386),
               (test_data.tmtc.TM_21_6_32, qll0.Spectra, 'ql-spectra',
                '0659399434:00007', '0659402538:00007', 4),
               (test_data.tmtc.TM_21_6_33, qll0.Variance, 'ql-variance',
                '0659399970:00007', '0659402958:00007', 747),
               (test_data.tmtc.TM_21_6_34, qll0.FlareFlag, 'ql-flareflag',
                '0659400170:00007', '0659402958:00007', 697),
               (test_data.tmtc.TM_21_6_41_complete, qll0.EnergyCalibration, 'ql-energycalibration',
                '0659318520:00000', '0659326920:00000', 1)
               ]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].stem for f in testpackets])
def test_quicklook(levelb, packets):
    hex_file, cl, name, beg, end, size = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    ql = cl.from_levelb(levelb)

    assert ql.level == 'L0'
    assert ql.name == name
    assert str(ql.obs_beg) == beg
    # TODO enable time tests again
    # assert str(ql.obs_end) == end
    assert len(ql.data) == size


@patch('stixcore.products.levelb.binary.LevelB')
def test_lightcurve(levelb):
    hex_file = test_data.tmtc.TM_21_6_30
    with hex_file.open('r') as file:
        hex = file.read()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", hex)]

    ql_lc_product_l0 = qll0.LightCurve.from_levelb(levelb)

    assert ql_lc_product_l0.level == 'L0'
    assert ql_lc_product_l0.name == 'ql-lightcurve'
    assert len(ql_lc_product_l0.control) == 1
    assert len(ql_lc_product_l0.data) == ql_lc_product_l0.control['num_samples']

    ql_lc_product_l1 = qll1.LightCurve.from_level0(ql_lc_product_l0)
    assert ql_lc_product_l1.level == 'L1'
    assert ql_lc_product_l1.name == 'ql-lightcurve'
    assert len(ql_lc_product_l1.control) == 1
    assert len(ql_lc_product_l1.data) == ql_lc_product_l1.control['num_samples']

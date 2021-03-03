import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0.science import CompressedPixelData, SummedPixelData

testpackets = [(test_data.tmtc.TM_21_6_21, CompressedPixelData, 'xray-cpd'),
               (test_data.tmtc.TM_21_6_21_nstr_2, CompressedPixelData, 'xray-cpd'),
               (test_data.tmtc.TM_21_6_22, SummedPixelData, 'xray-spd')
               ]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].name for f in testpackets])
def test_xray(levelb, packets):
    hex_file, cl, name = packets
    with hex_file.open('r') as file:
        hex = file.read()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", hex)]

    xray_L0 = cl.from_levelb(levelb)

    assert xray_L0.level == 'L1'
    assert xray_L0.name == name

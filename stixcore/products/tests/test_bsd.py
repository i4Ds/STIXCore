import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0.science import CompressedPixelData

testpackets = [(test_data.tmtc.TM_21_6_21_complete, CompressedPixelData, 'xray-cpd')]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].name for f in testpackets])
def test_xray(levelb, packets):
    hex_file, cl, name = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    xray_L0 = cl.from_levelb(levelb)

    assert xray_L0.level == 'L1'
    assert xray_L0.name == name
    assert str(xray_L0.obs_beg) == '41961022488579:06655'
    assert str(xray_L0.obs_end) == '41961022488659:12799'
    assert len(xray_L0.data) == 5

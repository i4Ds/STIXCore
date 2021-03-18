import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0.science import (
    Aspect,
    CompressedPixelData,
    RawPixelData,
    Spectrogram,
    Visibility,
)

testpackets = [(test_data.tmtc.TM_21_6_20_complete, RawPixelData, 'xray-rpd',
                '42006729850898:00000', '42006729851000:00000', 6),
               (test_data.tmtc.TM_21_6_21, CompressedPixelData, 'xray-cpd',
                '43180392185945:52223', '43180392185945:58367', 1),
               (test_data.tmtc.TM_21_6_21_complete, CompressedPixelData, 'xray-cpd',
                '41961022488579:06655', '41961022488659:12799', 5),
               (test_data.tmtc.TM_21_6_24, Spectrogram, 'xray-spectrogram',
                '43214571372558:38911', '43214571373473:32767', 54),
               (test_data.tmtc.TM_21_6_23_complete, Visibility, 'xray-visibility',
                '42076627730432:06655', '42076627730448:33279', 5),
               (test_data.tmtc.TM_21_6_42_complete, Aspect, 'burst-aspect',
                '0645932472:05485', '0645933120:33750', 2105)]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].name.split('.')[0] for f in testpackets])
def test_xray(levelb, packets):
    hex_file, cl, name, beg, end, size = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    xray_L0 = cl.from_levelb(levelb)

    assert xray_L0.level == 'L0'
    assert xray_L0.name == name
    assert str(xray_L0.obs_beg) == beg
    assert str(xray_L0.obs_end) == end
    assert len(xray_L0.data) == size

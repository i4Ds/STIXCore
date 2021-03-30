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
                '0640971848:00000', '0640971950:00000', 6),
               (test_data.tmtc.TM_21_6_21, CompressedPixelData, 'xray-cpd',
                '0658880585:52427', '0658880585:58981', 1),
               (test_data.tmtc.TM_21_6_21_complete, CompressedPixelData, 'xray-cpd',
                '0640274394:06553', '0640274474:13106', 5),
               (test_data.tmtc.TM_21_6_24, Spectrogram, 'xray-spectrogram',
                '0659402043:39320', '0659402958:32767', 54),
               (test_data.tmtc.TM_21_6_23_complete, Visibility, 'xray-visibility',
                '0642038387:06553', '0642038403:32767', 5),
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

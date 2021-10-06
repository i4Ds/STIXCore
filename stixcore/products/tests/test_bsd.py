import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0 import scienceL0 as sl0
from stixcore.products.level1 import scienceL1 as sl1

testpackets = [(test_data.tmtc.TM_21_6_20_complete, sl0.RawPixelData, sl1.RawPixelData,
                'xray-rpd', '0640971848f00000', '0640971950f00000', 6),
               (test_data.tmtc.TM_21_6_21, sl0.CompressedPixelData, sl1.CompressedPixelData,
                'xray-cpd', '0658880585f52427', '0658880585f58981', 1),
               (test_data.tmtc.TM_21_6_21_complete, sl0.CompressedPixelData,
                sl1.CompressedPixelData, 'xray-cpd', '0640274394f06553', '0640274476f06553', 5),
               (test_data.tmtc.TM_21_6_24, sl0.Spectrogram, sl1.Spectrogram,
                'xray-spec', '0659402043f39320', '0659402958f32767', 54),
               (test_data.tmtc.TM_21_6_23_complete, sl0.Visibility, sl1.Visibility,
                'xray-vis', '0642038387f06553', '0642038403f32767', 5),
               (test_data.tmtc.TM_21_6_42_complete, sl0.Aspect, sl1.Aspect,
                'aspect-burst', '0645932472f05485', '0645933132f52624', 2105)]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].name.split('.')[0] for f in testpackets])
def test_xray(levelb, packets):
    hex_file, cl_l0, cl_l1, name, beg, end, size = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = {'raw_file': 'raw.xml', 'packet': 0}
    xray_L0 = cl_l0.from_levelb(levelb, parent='parent.fits')

    assert xray_L0.level == 'L0'
    assert xray_L0.name == name
    # TODO enable time tests again
    # assert str(xray_L0.obs_beg) == beg
    # assert str(xray_L0.obs_end) == end
    assert len(xray_L0.data) == size

    xray_L1 = cl_l1.from_level0(xray_L0, parent='parent.l0.fits')
    assert xray_L1.level == 'L1'
    assert xray_L1.name == name
    assert len(xray_L1.data) == size

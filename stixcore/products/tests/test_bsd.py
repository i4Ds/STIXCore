import re
from unittest.mock import patch

import numpy as np
import pytest

from stixcore.data.test import test_data
from stixcore.products.level0 import scienceL0 as sl0
from stixcore.products.level1 import scienceL1 as sl1
from stixcore.time import SCETime


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


testpackets = [
    (
        test_data.tmtc.TM_21_6_20_complete,
        sl0.RawPixelData,
        sl1.RawPixelData,
        "xray-rpd",
        "640971848:0",
        "640971968:0",
        6,
    ),
    (
        test_data.tmtc.TM_21_6_21,
        sl0.CompressedPixelData,
        sl1.CompressedPixelData,
        "xray-cpd",
        "658880585:52428",
        "658880586:52428",
        1,
    ),
    (
        test_data.tmtc.TM_21_6_21_complete,
        sl0.CompressedPixelData,
        sl1.CompressedPixelData,
        "xray-cpd",
        "640274394:6554",
        "640274494:6554",
        5,
    ),
    (
        test_data.tmtc.TM_21_6_24,
        sl0.Spectrogram,
        sl1.Spectrogram,
        "xray-spec",
        "659402043:39322",
        "659402958:32768",
        54,
    ),
    (
        test_data.tmtc.TM_21_6_23_complete,
        sl0.Visibility,
        sl1.Visibility,
        "xray-vis",
        "642038387:6554",
        "642038407:6554",
        5,
    ),
    (
        test_data.tmtc.TM_21_6_42_complete,
        sl0.Aspect,
        sl1.Aspect,
        "aspect-burst",
        "645932471:62633",
        "645933124:48429",
        2105,
    ),
]


@patch("stixcore.products.levelb.binary.LevelB")
@pytest.mark.parametrize("packets", testpackets, ids=[f[0].name.split(".")[0] for f in testpackets])
def test_xray(levelb, out_dir, packets):
    hex_file, cl_l0, cl_l1, name, beg, end, size = packets
    with hex_file.open("r") as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = {"raw_file": "raw.xml", "packet": np.array([[0, 1, 2, 3]])}
    xray_L0 = cl_l0.from_levelb(levelb, parent="parent.fits")

    assert xray_L0.level == "L0"
    assert xray_L0.name == name
    assert xray_L0.scet_timerange.start == SCETime.from_string(beg)
    assert xray_L0.scet_timerange.end == SCETime.from_string(end)
    assert len(xray_L0.data) == size

    xray_L1 = cl_l1.from_level0(xray_L0, parent="parent.l0.fits")
    assert xray_L1.level == "L1"
    assert xray_L1.name == name
    assert len(xray_L1.data) == size

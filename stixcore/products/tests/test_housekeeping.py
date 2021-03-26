import re
from unittest.mock import patch

import pytest

from stixcore.data.test import test_data
from stixcore.products.level0.housekeeping import MaxiReport, MiniReport

testpackets = [(test_data.tmtc.TM_3_25_1, MiniReport, 'mini',
                '0660010031:51423', '0660010031:51423', 1),
               (test_data.tmtc.TM_3_25_2, MaxiReport, 'maxi',
                '0660258881:33104', '0660258881:33104', 1)]


@patch('stixcore.products.levelb.binary.LevelB')
@pytest.mark.parametrize('packets', testpackets, ids=[f[0].stem for f in testpackets])
def test_housekeeping(levelb, packets):
    hex_file, cl, name, beg, end, size = packets
    with hex_file.open('r') as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]

    hk = cl.from_levelb(levelb)

    assert hk.level == 'L0'
    assert hk.name == name
    assert str(hk.obs_beg) == beg
    assert str(hk.obs_end) == end
    assert len(hk.data) == size

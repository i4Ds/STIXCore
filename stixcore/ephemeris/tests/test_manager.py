from datetime import datetime
from unittest import mock

import pytest

from stixcore.ephemeris.manager import NotInSpiceContext, SpiceManager


@pytest.fixture
def TestManager():
    class TestSpiceManger(SpiceManager):
        @SpiceManager.spice_context
        def test_function(self, arg):
            return arg

    return TestSpiceManger


def test_manager_nokernel(TestManager):
    with pytest.raises(ValueError) as e:
        TestManager(meta_kernel_path='notreal.mk')
    assert str(e.value).startswith('Meta kernel not found')


@mock.patch('spiceypy.furnsh')
@mock.patch('spiceypy.unload')
def test_manager_context(mock_furnsh, mock_unload, TestManager, tmpdir):
    tmp_file = tmpdir.join('test_20200101_V01.mk')
    tmp_file.write('')
    tm = TestManager(meta_kernel_path=str(tmp_file))
    assert tm.kernel_date == datetime(2020, 1, 1)
    with pytest.raises(NotInSpiceContext):
        tm.test_function()

    with tm as context:
        res = context.test_function(1)

    assert res == 1
    assert mock_furnsh.called_once_with(str(tmp_file))
    assert mock_unload.called_once_with(str(tmp_file))

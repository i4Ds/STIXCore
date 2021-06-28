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
    # assert tm.kernel_date == datetime(2020, 1, 1)
    with pytest.raises(NotInSpiceContext):
        tm.test_function()

    with tm as context:
        res = context.test_function(1)

    assert res == 1
    assert mock_furnsh.called_once_with(str(tmp_file))
    assert mock_unload.called_once_with(str(tmp_file))


def test_wrap_value_field(TestManager):
    wrapped = TestManager._wrap_value_field(''.join([str(i) for i in range(100)]))
    assert wrapped == """'012345678910111213141516171819202122232425262728293031323334353637383940414243+'
'444546474849505152535455565758596061626364656667686970717273747576777879808182+'
'8384858687888990919293949596979899'"""

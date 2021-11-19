import os
from unittest import mock

import pytest

from stixcore.data.test import test_data
from stixcore.ephemeris.manager import (
    NotInSpiceContext,
    SpiceKernelLoader,
    SpiceKernelManager,
    SpiceKernelType,
)


@pytest.fixture
def spicekernelmanager():
    return SpiceKernelManager.instance


@pytest.fixture
def TestLoader():
    class TestSpiceKernelLoader(SpiceKernelLoader):
        @SpiceKernelLoader.spice_context
        def test_function(self, arg):
            return arg

    return TestSpiceKernelLoader


def test_loader_nokernel(TestLoader):
    with pytest.raises(ValueError) as e:
        TestLoader(meta_kernel_path='notreal.mk')
    assert str(e.value).startswith('Meta kernel not found')


@mock.patch('spiceypy.furnsh')
@mock.patch('spiceypy.unload')
def test_loader_context(mock_furnsh, mock_unload, TestLoader, tmpdir):
    tmp_file = tmpdir.join('test_20200101_V01.mk')
    tmp_file.write('')
    tm = TestLoader(meta_kernel_path=str(tmp_file))
    # assert tm.kernel_date == datetime(2020, 1, 1)
    with pytest.raises(NotInSpiceContext):
        tm.test_function()

    with tm as context:
        res = context.test_function(1)

    assert res == 1
    assert mock_furnsh.called_once_with(str(tmp_file))
    assert mock_unload.called_once_with(str(tmp_file))


def test_wrap_value_field(TestLoader):
    wrapped = TestLoader._wrap_value_field(''.join([str(i) for i in range(100)]))
    assert wrapped == """'012345678910111213141516171819202122232425262728293031323334353637383940414243+'
'444546474849505152535455565758596061626364656667686970717273747576777879808182+'
'8384858687888990919293949596979899'"""


def test_manager_fail_create():
    with pytest.raises(ValueError) as e:
        SpiceKernelManager("nodir")
    assert str(e.value).startswith('path not found')


def test_manager_create(spicekernelmanager):
    assert spicekernelmanager.path == test_data.ephemeris.KERNELS_DIR


def test_manager_get_latest(spicekernelmanager):
    assert (spicekernelmanager.get_latest(SpiceKernelType.MK).name ==
            "solo_ANC_soc-flown-mk_V105_20200515_001.tm")
    assert (spicekernelmanager.get_latest(SpiceKernelType.SCLK).name ==
            "solo_ANC_soc-sclk_20200904_V01.tsc")
    assert (spicekernelmanager.get_latest(SpiceKernelType.LSK).name ==
            "naif0012.tls")

    with pytest.raises(ValueError) as e:
        spicekernelmanager.get_latest(SpiceKernelType.FK)
    assert str(e.value).startswith('No current kernel found')


def test_manager_environment(spicekernelmanager):
    assert str(spicekernelmanager.path) == os.environ.get("STIX_PROCESSING_SPICE_DIR", "")
    latest_mk = spicekernelmanager.get_latest(SpiceKernelType.MK)
    assert os.environ.get("STIX_PROCESSING_SPICE_MK", "") == ""

    latest_mk = spicekernelmanager.get_latest(SpiceKernelType.MK, setenvironment=True)
    assert str(latest_mk) == os.environ.get("STIX_PROCESSING_SPICE_MK", "")

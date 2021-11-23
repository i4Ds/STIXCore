import os

import pytest

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager, SpiceKernelType


@pytest.fixture
def spicekernelmanager():
    return SpiceKernelManager(CONFIG.get("Paths", "spice_kernels"))


def test_loader_nokernel():
    with pytest.raises(ValueError) as e:
        Spice(meta_kernel_path='notreal.mk')
    assert str(e.value).startswith('Meta kernel not found')


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

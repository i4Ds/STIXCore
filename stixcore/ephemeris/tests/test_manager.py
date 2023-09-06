import pytest

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager, SpiceKernelType


@pytest.fixture
def spicekernelmanager():
    return SpiceKernelManager(CONFIG.get("Paths", "spice_kernels"))


def test_loader_nokernel():
    with pytest.raises(ValueError) as e:
        Spice(meta_kernel_pathes='notreal.mk')
    assert str(e.value).startswith("Failed to load any NEW META KERNEL")


def test_manager_fail_create():
    with pytest.raises(ValueError) as e:
        SpiceKernelManager("nodir")
    assert str(e.value).startswith('path not found')


def test_manager_create(spicekernelmanager):
    assert spicekernelmanager.path == test_data.ephemeris.KERNELS_DIR


def test_manager_get_latest(spicekernelmanager):
    assert (spicekernelmanager.get_latest(SpiceKernelType.MK)[0].name ==
            "solo_ANC_soc-flown-mk_V105_20200515_001.tm")
    assert (spicekernelmanager.get_latest(SpiceKernelType.SCLK)[0].name ==
            "solo_ANC_soc-sclk_20200904_V01.tsc")
    assert (spicekernelmanager.get_latest(SpiceKernelType.LSK)[0].name ==
            "naif0012.tls")

    assert (spicekernelmanager.get_latest(SpiceKernelType.MK_PRED)[0].name ==
            "solo_ANC_soc-pred-mk_V106_20201116_001.tm")

    with pytest.raises(ValueError) as e:
        spicekernelmanager.get_latest(SpiceKernelType.FK)
    assert str(e.value).startswith('No current kernel found')

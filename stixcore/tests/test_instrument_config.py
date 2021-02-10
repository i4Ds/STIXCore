from pathlib import Path

import pytest


@pytest.fixture
def path():
    return Path(__file__).parent.parent.parent / "STIX-CONF"


def test_module_available(path):
    assert path.exists()
    assert (path / "README.md").exists()

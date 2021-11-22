from stixcore.config.config import CONFIG


def test_parse():
    assert CONFIG.get("Paths", "tm_archive", fallback="test") == ''
    assert CONFIG.get("Paths", "tm_archive2", fallback="test") == 'test'

    assert not CONFIG.getboolean("IDLBridge", "enabled", fallback=False)
    assert CONFIG.getboolean("IDLBridge", "enabled2", fallback=True)

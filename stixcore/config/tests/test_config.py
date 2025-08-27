from stixcore.config.config import CONFIG


def test_parse():
    assert "tm_archive" in CONFIG["Paths"]
    assert CONFIG.get("Paths", "tm_archive2", fallback="test") == "test"

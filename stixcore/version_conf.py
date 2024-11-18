from pathlib import Path


def get_conf_version():
    try:
        with open(Path(__file__).parent / "config" / "data" / "common" / "VERSION.TXT") as f:
            return f.readline().strip()
    except Exception:
        return 'v0.1.5code'


__version_conf__ = get_conf_version()

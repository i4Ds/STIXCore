import os
from pathlib import Path
from configparser import ConfigParser


def _get_config():
    config_path = Path(os.path.expanduser('~')) / 'stixcore.ini'
    config = ConfigParser()
    config.read(config_path)
    return config


CONFIG = _get_config()

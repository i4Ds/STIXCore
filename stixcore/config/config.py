import os
from pathlib import Path
from configparser import ConfigParser


def _get_config():
    """
    Parse and set config info expects a file with the following entries

    [Paths]
    tm_archive = /home/shane/tm
    fits_archive = /home/shane/fits
    spice_kernels = /home/shane/spice

    Returns
    -------
    The parsed configuration as nested dictionaries
    """
    config_file = Path(os.path.expanduser('~')) / 'stixcore.ini'
    with config_file.open('r') as file:
        config = ConfigParser()
        config.read_file(file)
        return config


CONFIG = _get_config()

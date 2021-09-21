import os
from pathlib import Path
from configparser import ConfigParser

import stixcore
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


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
    module_dir = Path(stixcore.__file__).parent
    default = module_dir / 'data' / 'stixcore.ini'
    user_file = Path(os.path.expanduser('~')) / 'stixcore.ini'
    config_files = [default, user_file]
    config = ConfigParser()
    for file in config_files:
        try:
            with file.open('r') as buffer:
                config.read_file(buffer)
        except FileNotFoundError:
            logger.info('Config file %s not found', file)
    return config


CONFIG = _get_config()

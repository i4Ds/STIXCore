import os
import sys
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
    if 'pytest' in sys.modules:
        default = module_dir / 'data' / 'test' / 'stixcore.ini'
        config_files = [default]
    else:
        default = module_dir / 'data' / 'stixcore.ini'
        # merge with local user ini
        user_file = Path(os.path.expanduser('~')) / 'stixcore.ini'
        config_files = [default, user_file]

    config = ConfigParser()
    for file in config_files:
        try:
            with file.open('r') as buffer:
                config.read_file(buffer)
        except FileNotFoundError:
            logger.info('Config file %s not found', file)

    # override the spice kernel dir in case of testing
    if 'pytest' in sys.modules:
        from stixcore.data.test import test_data
        config.set('Paths', 'spice_kernels', str(test_data.ephemeris.KERNELS_DIR))

    return config


CONFIG = _get_config()

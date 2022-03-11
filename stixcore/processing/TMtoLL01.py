"""
Low Latency Date Pipeline processing
"""

import time
import shutil
import logging
from os import getenv
from pathlib import Path
from datetime import datetime

from stixcore.config.config import CONFIG
from stixcore.io.fits.processors import FitsLL01Processor
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

Y_M_D_H_M = "%Y%m%d%H%M"
DIR_ENVNAMES = [('requests', 'instr_input_requests'), ('output', 'instr_output')]
REQUEST_GLOB = 'request_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]*'
LLDP_VERSION = "00.07.00"

logger = get_logger(__name__)


def get_paths():
    """
    Return paths for lldp processing.
    Returns
    -------
    dict
        Dictionary of paths
    """
    paths = {}
    for (name, envname) in DIR_ENVNAMES:
        path = getenv(envname)
        if path is None:
            raise ValueError('Environment variable with name "%s" was not found.' % (envname,))
        paths[name] = Path(path)

    paths['products'] = paths['output'].joinpath('products')
    paths['failed'] = paths['output'].joinpath('failed')
    paths['temporary'] = paths['output'].joinpath('temporary')
    paths['logs'] = paths['output'].joinpath('logs')

    return paths


class RequestException(Exception):
    """
    Exception during processing of request.
    """


def process_request(request, outputdir):
    """
    Process at LLDP request.
    Parameters
    ----------
    request : `pathlib.Path`
        Path to directory containing request
    outputdir :
        Path to directory to store outputs
    Raises
    ------
    RequestException
        There was an error processing the request
    """
    logger.info('Processing %s', request.name)

    tmtc_files = list(request.joinpath('telemetry').glob('*.xml'))
    if len(tmtc_files) != 1:
        raise RequestException('Expected one tmtc file found %s.', len(tmtc_files))
    soc_file = SOCPacketFile(tmtc_files[0])
    lb = LevelB.from_tm(soc_file)
    prods = []
    for prod in lb:
        # Only process light curve (30) and flare flag and location (34)
        if prod.ssid in (30, 34):
            tmp = Product._check_registered_widget(
                level='L0', service_type=prod.service_type,
                service_subtype=prod.service_subtype, ssid=prod.ssid,
                data=None, control=None)
            try:
                l0 = tmp.from_levelb(prod, parent='')
                prods.append(l0)
            except Exception as e:
                logger.error('Error processing file %s for %s, %s, %s', soc_file,
                             lb.service_type, lb.service_subtype, lb.ssid)
                logger.error('%s', e)
                if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                    raise e

    processor = FitsLL01Processor()
    curtime = datetime.now()
    for prod in prods:
        processor.write_fits(prod, outputdir, curtime)


if __name__ == "__main__":
    PATHS = get_paths()

    logger.propagate = False
    file_handler = logging.FileHandler(PATHS['logs'] / 'lldp_processing.log')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s - %(name)s',
                                  datefmt='%Y-%m-%dT%H:%M:%SZ')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.root.setLevel(logging.DEBUG)
    logger.info('LLDP pipeline starting')
    while True:
        requests = list(PATHS['requests'].glob(REQUEST_GLOB))
        logger.info('Found %s requests to process.', len(requests))
        for request in PATHS['requests'].glob(REQUEST_GLOB):
            if PATHS['failed'].joinpath(request.name).exists():
                logger.info('%s found in failed (failed).', request.name)
            elif PATHS['temporary'].joinpath(request.name).exists():
                logger.info('%s found in temporary (processing or failed)', request.name)
            elif PATHS['products'].joinpath(request.name).exists():
                logger.info('%s found in products (completed)', request.name)
            else:
                logger.info('%s to be praocessed', request.name)
                temp_path = PATHS['temporary'] / request.name
                temp_path.mkdir(exist_ok=True, parents=True)
                try:
                    process_request(request, temp_path)
                except Exception as e:
                    logger.error('%s error while processing', request.name)
                    logger.error(e, exc_info=True)
                    failed_path = PATHS['failed']
                    shutil.move(temp_path.as_posix(), failed_path.as_posix())
                    continue

                shutil.move(temp_path.as_posix(), PATHS['products'].as_posix())
                logger.info('Finished Processing %s', request.name)

        time.sleep(5)

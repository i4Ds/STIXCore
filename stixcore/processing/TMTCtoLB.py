import logging
import warnings
from time import perf_counter
from pathlib import Path
from itertools import chain
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.io.fits.processors import FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.products.levelb.binary import LevelB
from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.WARNING)


def tmtc_to_l0(tmtc_path, archive_path):
    socm = SOCManager(tmtc_path)
    out_dir = archive_path
    out_dir.mkdir(parents=True, exist_ok=True)
    files_to_process = list(socm.get_files(TMTC.TM))
    return process_tmtc_to_levelbinary(files_to_process, archive_path)


def process_tmtc_to_levelbinary(files_to_process, archive_path=None):
    if archive_path is None:
        archive_path = Path(CONFIG.get('Paths', 'fits_archive'))
    fits_processor = FitsLBProcessor(archive_path)
    jobs = []
    with Manager() as manager:
        open_files = manager.list()
        with ThreadPoolExecutor() as exec:
            for tmtc_file in files_to_process:
                logger.info(f'Started processing of file: {tmtc_file}')
                # TODO sorting filter etc
                for prod in LevelB.from_tm(tmtc_file):
                    if prod:
                        jobs.append(exec.submit(fits_processor.write_fits, prod, open_files))
                logger.info(f'Finished processing of file: {tmtc_file}')

    unique_files = set()
    files = [r.result() for r in chain(jobs) if r is not None]
    [unique_files.update(set(f)) for f in files if f is not None]
    return unique_files


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('LevelB run')
    warnings.filterwarnings('ignore', module='astropy.io.fits.card')

    tm_path = Path('/home/shane/tm')
    archive_path = Path('/home/shane/fits_test')

    lb_files = tmtc_to_l0(tmtc_path=tm_path, archive_path=archive_path)
    logger.info(lb_files)
    tend = perf_counter()
    logger.info('Time taken %f', tend - tstart)

import logging
from time import perf_counter
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.io.fits.processors import FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.products.levelb.binary import LevelB
from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


def tmtc_to_l0(tmtc_path, archive_path):
    socm = SOCManager(tmtc_path)
    out_dir = archive_path
    out_dir.mkdir(parents=True, exist_ok=True)
    files_to_process = socm.get_files(TMTC.TM)
    return process_tmtc_to_levelbinary(files_to_process, archive_path)


def process_tmtc_to_levelbinary(files_to_process, archive_path=None):
    if archive_path is None:
        archive_path = Path(CONFIG.get('Paths', 'fits_archive'))
    fits_processor = FitsLBProcessor(archive_path)
    all_files = set()
    for tmtc_file in files_to_process:
        logger.info(f'Processing file: {tmtc_file}')
        jobs = []
        with ProcessPoolExecutor() as executor:
            for prod in LevelB.from_tm(tmtc_file):
                if prod:
                    jobs.append(executor.submit(fits_processor.write_fits, prod))

        for job in jobs:
            try:
                new_files = job.result()
                all_files.update(new_files)
            except Exception:
                logger.error('Error processing', exc_info=True)

    return all_files


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('LevelB run')

    tm_path = Path('/Users/shane/Projects/STIX/tm')
    archive_path = Path('/Users/shane/Projects/STIX/fits_test')

    lb_files = tmtc_to_l0(tmtc_path=tm_path, archive_path=archive_path)

    tend = perf_counter()
    logger.info(lb_files)
    logger.info('Time taken %f', tend - tstart)

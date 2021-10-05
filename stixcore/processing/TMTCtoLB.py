import logging
from time import perf_counter
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

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
    files_to_process = list(socm.get_files(TMTC.TM))[:3]
    return process_tmtc_to_levelbinary(files_to_process, archive_path)


def process_tmtc_to_levelbinary(files_to_process, archive_path=None):
    if archive_path is None:
        archive_path = Path(CONFIG.get('Paths', 'fits_archive'))
    fits_processor = FitsLBProcessor(archive_path)
    files = []
    with ThreadPoolExecutor() as exec:
        for tmtc_file in files_to_process:
            logger.info(f'Processing file: {tmtc_file}')
            # TODO sorting filter etc
            for prod in LevelB.from_tm(tmtc_file):
                if prod:
                    files.append(exec.submit(fits_processor.write_fits, prod))

    return files


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('LevelB run')

    tm_path = Path('/Users/shane/Projects/STIX/tm')
    archive_path = Path('/Users/shane/Projects/STIX/fits_new')

    lb_files = tmtc_to_l0(tmtc_path=tm_path, archive_path=archive_path)

    tend = perf_counter()
    logger.info('Time taken %f', tend - tstart)

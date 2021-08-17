import logging
from time import perf_counter
from pathlib import Path

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
    files_to_process = socm.get_files(TMTC.TM)
    return process_tmtc_to_levelbinary(files_to_process, archive_path)


def process_tmtc_to_levelbinary(files_to_process, archive_path=None):
    if archive_path is None:
        archive_path = Path(CONFIG.get('Paths', 'fits_archive'))
    fits_processor = FitsLBProcessor(archive_path)
    all_files = []
    for tmtc_file in files_to_process:
        logger.info(f'Processing file: {tmtc_file}')
        # TODO sorting filter etc
        for prod in LevelB.from_tm(tmtc_file):
            if prod:
                files = fits_processor.write_fits(prod)
                all_files.extend(files)
    return all_files


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('LevelB run')

    tm_path = Path('/home/shane/tm181')
    archive_path = Path('/home/shane/fits181')

    lb_files = tmtc_to_l0(tmtc_path=tm_path, archive_path=archive_path)

    tend = perf_counter()
    logger.info('Time taken %f', tend - tstart)

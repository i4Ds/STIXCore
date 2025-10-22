import warnings
from time import perf_counter
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.io.product_processors.fits.processors import FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.products.levelb.binary import LevelB
from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


def tmtc_to_l0(tmtc_path, archive_path):
    socm = SOCManager(tmtc_path)
    out_dir = archive_path
    out_dir.mkdir(parents=True, exist_ok=True)
    files_to_process = list(socm.get_files(TMTC.TM))
    return process_tmtc_to_levelbinary(files_to_process, archive_path)


def process_tmtc_to_levelbinary(files_to_process, archive_path=None):
    if archive_path is None:
        archive_path = Path(CONFIG.get("Paths", "fits_archive"))
    fits_processor = FitsLBProcessor(archive_path)
    all_files = set()
    for tmtc_file in files_to_process:
        logger.info(f"Processing file: {tmtc_file}")
        jobs = []
        with ProcessPoolExecutor() as executor:
            for prod in LevelB.from_tm(tmtc_file):
                if prod:
                    jobs.append(executor.submit(fits_processor.write_fits, prod))

        for job in jobs:
            try:
                new_files = job.result()
                all_files.update(new_files)
            except Exception as e:
                logger.error("Error processing", exc_info=True)
                if CONFIG.getboolean("Logging", "stop_on_error", fallback=False):
                    raise e

    return all_files


if __name__ == "__main__":
    tstart = perf_counter()
    logger.info("LevelB run")
    warnings.filterwarnings("ignore", module="astropy.io.fits.card")

    tm_path = Path("/home/shane/tm")
    archive_path = Path("/home/shane/fits_test_latest")

    lb_files = tmtc_to_l0(tmtc_path=tm_path, archive_path=archive_path)
    logger.info(len(lb_files))
    tend = perf_counter()
    logger.info("Time taken %f", tend - tstart)

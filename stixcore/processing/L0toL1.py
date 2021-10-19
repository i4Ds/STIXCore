import logging
import warnings
from time import sleep, perf_counter
from pathlib import Path
from itertools import chain
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor

from sunpy.util.datatype_factory_base import NoMatchError

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.INFO)


class Level1:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL1Processor(self.output_dir)

    def process_fits_files(self, files=None):
        if files is None:
            files = self.level0_files

        with Manager() as manager:
            open_files = manager.list()
            with ProcessPoolExecutor() as executor:
                jobs = [executor.submit(process_file, file, self.processor, open_files)
                        for file in files]

        unique_files = set()
        files = [r.result() for r in chain(jobs) if r is not None]
        [unique_files.update(set(f)) for f in files if f is not None]
        return unique_files


def process_file(file, processor, open_files):
    if file.name in open_files:
        for i in range(100):
            logger.debug('Waiting file %s in open files', file.name)
            sleep(1)
            if file.name not in open_files:
                break
        else:
            logger.debug('File was never free %s', file.name)

    try:
        l0 = Product(file)
    except Exception:
        logger.error('Loading file %s', file, exc_info=True)
        return
    try:
        tmp = Product._check_registered_widget(level='L1', service_type=l0.service_type,
                                               service_subtype=l0.service_subtype,
                                               ssid=l0.ssid, data=None, control=None)
        l1 = tmp.from_level0(l0, parent=file.name)
        return processor.write_fits(l1, open_files)
    except NoMatchError:
        logger.debug('No match for product %s', l0)
    except Exception:
        logger.error('Error processing file %s', file, exc_info=True)


if __name__ == '__main__':
    tstart = perf_counter()
    warnings.filterwarnings('ignore', module='astropy.io.fits.card')

    fits_path = Path('/home/shane/fits_test/L0')
    bd = Path('/home/shane/fits_test/')

    l1processor = Level1(fits_path, bd)
    all_files = l1processor.process_fits_files()
    logger.info(all_files)
    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

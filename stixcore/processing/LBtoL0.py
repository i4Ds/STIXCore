import logging
import warnings
from time import sleep, perf_counter
from pathlib import Path
from itertools import chain
from collections import defaultdict
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor

from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.WARNING)


class Level0:
    """

    """
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.levelb_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL0Processor(self.output_dir)

    def process_fits_files(self, files=None):
        tm = defaultdict(list)
        if files is None:
            files = self.levelb_files
        # Create list of file by type
        for file in files:
            mission, level, identifier, *_ = file.name.split('_')
            tm_type = tuple(map(int, identifier.split('-')[1:]))
            # if tm_type[-1] not in {20, 21, 22, 23, 24} and tm_type[0] == 21:  # TODO Fix 43
            tm[tm_type].append(file)

        # Need to fix not standard time axis
        try:
            del tm[(21, 6, 43)]
        except Exception:
            pass

        # For each type
        with Manager() as manager:
            open_files = manager.list()
            with ProcessPoolExecutor() as executor:
                res = []
                for tm_type, files in tm.items():
                    # Stand alone packet data
                    if (tm_type[0] == 21 and tm_type[-1] not in {20, 21, 22, 23, 24}) or \
                            tm_type[0] != 21:
                        for file in files:
                            logger.debug('Processing stand alone packets from file %s', file.name)
                            res.append(self.process_standalone(file, executor, open_files))
                    else:
                        for file in files:
                            logger.debug('Processing packet sequence from file %s', file.name)
                            res.extend(self.process_sequence(file, executor, open_files))

            unique_files = set()
            files = [r.result() for r in chain(res) if r is not None]
            [unique_files.update(set(f)) for f in files if f is not None]
            return unique_files

    def process_standalone(self, file, executor, open_files):
        if file.name in open_files:
            for i in range(100):
                logger.debug('Waiting file %s in open files', file.name)
                sleep(1)
                if file.name not in open_files:
                    break
            else:
                logger.debug('File was never free %s', file.name)

        try:
            levelb = Product(file)
            return executor.submit(self.process_product, levelb, file, open_files)
        except Exception:
            logger.error('Error processing file %s', file)

    def process_sequence(self, file, executor, open_files):
        if file.name in open_files:
            for i in range(100):
                logger.debug('Waiting file %s in open files', file.name)
                sleep(1)
                if file.name not in open_files:
                    break
            else:
                logger.error('File was never free %s', file.name)

        jobs = []
        last_incomplete = []
        levelb = Product(file)
        complete, incomplete = levelb.extract_sequences()

        if incomplete and last_incomplete:
            combined_complete, combined_incomplete \
                = (incomplete[0] + last_incomplete[0]).extract_sequences()
            complete.extend(combined_complete)
            last_incomplete = combined_incomplete

        if complete:
            for comp in complete:
                # TODO need to carry better information for logging like index from
                #  original files and file names
                jobs.append(executor.submit(self.process_product, comp, file, open_files))
            complete = []
        try:
            last_incomplete = last_incomplete[0] + incomplete[0]
        except IndexError:
            last_incomplete = []

        if last_incomplete:
            for inc in last_incomplete:
                jobs.append(executor.submit(self.process_product,  inc, file, open_files))

        return jobs

    def process_product(self, prod, file, open_files):
        tmp = Product._check_registered_widget(level='L0',
                                               service_type=prod.service_type,
                                               service_subtype=prod.service_subtype,
                                               ssid=prod.ssid, data=None,
                                               control=None)
        try:
            level0 = tmp.from_levelb(prod, parent=file.name)
            return self.processor.write_fits(level0, open_files)
        except Exception as e:
            logger.error(f'Error processing {file.name}, {prod.control["packet"]}')
            logger.error('%s', e)


if __name__ == '__main__':
    tstart = perf_counter()

    warnings.filterwarnings('ignore', module='astropy.io.fits.card')

    fits_path = Path('/Users/shane/Projects/STIX/fits_test/LB/')
    bd = Path('/Users/shane/Projects/STIX/fits_test')

    l0processor = Level0(fits_path, bd)
    l0_files = l0processor.process_fits_files()
    logger.info(l0_files)

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

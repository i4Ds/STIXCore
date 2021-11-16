import logging
import warnings
from time import perf_counter
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from sunpy.util.datatype_factory_base import NoMatchError

from stixcore.io.fits.processors import FitsL2Processor
from stixcore.processing.sswidl import SSWIDLProcessor
from stixcore.products import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.INFO)


class Level2:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level1_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL2Processor(self.output_dir)

    def process_fits_files(self, files=None):
        all_files = list()
        if files is None:
            files = self.level1_files

        product_types = defaultdict(list)
        for file in files:
            # group by product: '(HK,maxi)'
            mission, level, identifier, *_ = file.name.split('_')
            tm_type = tuple(identifier.split('-')[1:])
            product_types[tm_type].append(file)

        jobs = []
        with ProcessPoolExecutor() as executor:
            for pt, files in product_types.items():
                jobs.append(executor.submit(process_type, files, FitsL2Processor(self.output_dir),
                                            SOOPManager.instance))

        for job in jobs:
            try:
                new_files = job.result()
                all_files.extend(new_files)
            except Exception:
                logger.error('error', exc_info=True)

        return list(set(all_files))


def process_type(files, processor, soopmanager):
    SOOPManager.instance = soopmanager
    idlprocessor = SSWIDLProcessor(processor)

    all_files = list()
    for file in files:
        l1 = Product(file)
        try:
            tmp = Product._check_registered_widget(level='L2', service_type=l1.service_type,
                                                   service_subtype=l1.service_subtype,
                                                   ssid=l1.ssid, data=None, control=None)
            for l2 in tmp.from_level1(l1, parent=file, idlprocessor=idlprocessor):
                files = processor.write_fits(l2)
                all_files.extend(files)
        except NoMatchError:
            logger.debug('No match for product %s', l1)
        except Exception as e:
            logger.error('Error processing file %s', file, exc_info=True)
            raise e

    idlfitsfiles = idlprocessor.process()
    all_files.extend(idlfitsfiles)

    return all_files


if __name__ == '__main__':
    tstart = perf_counter()
    warnings.filterwarnings('ignore', module='astropy.io.fits.card')
    warnings.filterwarnings('ignore', module='stixcore.soop.manager')
    warnings.filterwarnings('ignore', module='astropy.utils.metadata')

    fits_path = Path('/home/shane/fits_test/L0')
    bd = Path('/home/shane/fits_test/')

    l2processor = Level2(fits_path, bd)
    all_files = l2processor.process_fits_files()
    logger.info(all_files)
    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

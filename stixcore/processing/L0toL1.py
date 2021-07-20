import logging
from time import perf_counter
from pathlib import Path

from sunpy.util.datatype_factory_base import NoMatchError

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


class Level1:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL1Processor(self.output_dir)

    def process_fits_files(self):
        all_files = []
        for file in self.level0_files:
            l0 = Product(file)
            try:
                tmp = Product._check_registered_widget(level='L1', service_type=l0.service_type,
                                                       service_subtype=l0.service_subtype,
                                                       ssid=l0.ssid, data=None, control=None)
                l1 = tmp.from_level0(l0)
                files = self.processor.write_fits(l1)
                all_files.extend(files)
            except NoMatchError:
                logger.debug('No match for product %s', l0)
            except Exception as e:
                logger.error('Error processing file %s', file)
                logger.error(e)
                # raise e
        return all_files


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/home/shane/fits/L0/21/6/24')
    bd = Path('/home/shane/fits')

    l1processor = Level1(fits_path, bd)
    all_files = l1processor.process_fits_files()
    logger.info(all_files)
    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

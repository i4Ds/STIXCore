from time import perf_counter
from pathlib import Path

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Level1:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL1Processor(self.output_dir)

    def process_fits_files(self):
        for file in self.level0_files:
            l0 = Product(file)
            tmp = Product._check_registered_widget(level='L1', service_type=l0.service_type,
                                                   service_subtype=l0.service_subtype,
                                                   ssid=l0.ssid, data=None, control=None)
            l1 = tmp.from_level0(l0)
            self.processor.write_fits(l1)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/time_test')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/time_test/')

    l1processor = Level1(fits_path, bd)
    l1processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

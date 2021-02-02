from time import perf_counter
from pathlib import Path

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products.level1.quicklook import LightCurve
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Level1:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level1_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL1Processor(self.output_dir)

    def process_fits_files(self):
        for file in self.level1_files:
            prod = LightCurve.from_fits(file)
            self.processor.write_fits(prod)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/test_new/L0/21/6/30')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/test_new')

    l1processor = Level1(fits_path, bd)
    l1processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

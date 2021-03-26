import os
from time import perf_counter
from pathlib import Path

from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Level0:
    """

    """
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.levelb_files = sorted(list(self.source_dir.rglob('*.fits')),  key=os.path.getctime)
        self.processor = FitsL0Processor(output_dir)

    def process_fits_files(self):
        for i, file in enumerate(self.levelb_files):
            if i == 0:
                cur_lb = Product(self.levelb_files[0])
                cur_complete, cur_incomplete = cur_lb.extract_sequences()

            next_lb = Product(file)
            next_complete, next_incomplete = next_lb.extract_sequences()

            if cur_incomplete and next_incomplete:
                incomplete_combined, incomplete_remaining = \
                    (cur_incomplete[0] + next_incomplete[0]).extract_sequences()

                if incomplete_combined:
                    complete = cur_complete
                    complete = complete + incomplete_combined
                else:
                    complete = cur_complete

                next_incomplete = incomplete_remaining
            else:
                complete = cur_complete

            cur_complete = next_complete
            cur_incomplete = next_incomplete

            if complete:
                for comp in complete:
                    tmp = Product._check_registered_widget(
                        level='L0', service_type=cur_lb.service_type,
                        service_subtype=cur_lb.service_subtype, ssid=cur_lb.ssid, data=None,
                        control=None)

                    # TODO need to carry better information for logging like index from oginianl
                    # files and file names
                    try:
                        level0 = tmp.from_levelb(comp)
                    except Exception as e:
                        logger.error('%s', e)
                        continue

                    self.processor.write_fits(level0)

            # if incomplete:
            #     tmp = Product._check_registered_widget(level='L0',
            #     service_type=cur_lb.service_type,
            #                                            service_subtype=cur_lb.service_subtype,
            #                                            ssid=cur_lb.ssid, data=None, control=None)
            #     level0 = tmp.from_levelb(incomplete)
            #     self.processor.write_fits(level0)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/asdfadsf/LB/21/6/43')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/asdfadsf')

    l0processor = Level0(fits_path, bd)
    l0processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

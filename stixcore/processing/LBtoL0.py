import os
import logging
from time import perf_counter
from pathlib import Path
from operator import add
from functools import reduce
from collections import defaultdict

from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


class Level0:
    """

    """
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.levelb_files = sorted(list(self.source_dir.rglob('*.fits')),  key=os.path.getctime)
        self.processor = FitsL0Processor(output_dir)

    def process_fits_files(self):
        all_files = []
        tm = defaultdict(list)

        # Create list of file by type
        for file in sorted(self.levelb_files):
            mission, level, identifier, *_ = file.name.split('_')
            tm_type = tuple(map(int, identifier.split('-')[1:]))
            if tm_type[-1] in {30, 31, 32, 33, 34, 41, 20, 21, 22, 23, 24}:  # TODO Fix 43
                tm[tm_type].append(file)

        # For each type
        for tm_type, files in tm.items():

            # Stand alane packet data
            if tm_type[0] in {3, 21} and tm_type[-1] in {1, 2, 30, 31, 32, 33, 34}:
                levelb = map(Product, files)
                levelb_combined = reduce(add, levelb)
                tmp = Product._check_registered_widget(
                    level='L0', service_type=levelb_combined.service_type,
                    service_subtype=levelb_combined.service_subtype, ssid=levelb_combined.ssid,
                    data=None, control=None)
                level0 = tmp.from_levelb(levelb_combined)
                fits_files = self.processor.write_fits(level0)
                all_files.extend(fits_files)
            else:
                last_incomplete = []
                # for each file
                for file in files:
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
                            try:
                                tmp = Product._check_registered_widget(
                                    level='L0', service_type=comp.service_type,
                                    service_subtype=comp.service_subtype, ssid=comp.ssid, data=None,
                                    control=None)
                                level0 = tmp.from_levelb(comp)
                                fits_files = self.processor.write_fits(level0)
                                all_files.extend(fits_files)
                            except Exception as e:
                                logger.error('Error processing file %s for %s, %s, %s', file,
                                             comp.service_type, comp.service_subtype, comp.ssid)
                                logger.error('%s', e)
                                raise e
                        complete = []
                    try:
                        last_incomplete = last_incomplete[0] + incomplete[0]
                    except IndexError:
                        last_incomplete = []

                if last_incomplete:
                    for inc in last_incomplete:
                        tmp = Product._check_registered_widget(level='L0',
                                                               service_type=inc.service_type,
                                                               service_subtype=inc.service_subtype,
                                                               ssid=inc.ssid, data=None,
                                                               control=None)
                        level0 = tmp.from_levelb(inc)
                        fits_files = self.processor.write_fits(level0)
                        all_files.extend(fits_files)

        return all_files


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/tm_test/LB')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/tm_test')

    l0processor = Level0(fits_path, bd)
    l0_files = l0processor.process_fits_files()
    logger.info(l0_files)

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

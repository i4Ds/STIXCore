import warnings
from time import perf_counter
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.level0.scienceL0 import NotCombineException
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Level0:
    """

    """
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.levelb_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL0Processor(self.output_dir)

    def process_fits_files(self, files=None):
        all_files = list()
        tm = defaultdict(list)
        if files is None:
            files = self.levelb_files
        # Create list of file by type
        for file in files:
            mission, level, identifier, *_ = file.name.split('_')
            tm_type = tuple(map(int, identifier.split('-')[1:]))
            tm[tm_type].append(file)

        # For each type
        with ProcessPoolExecutor() as executor:
            jobs = [
                executor.submit(process_tm_type, files, tm_type, self.processor,
                                # keep track of the used Spice kernel
                                spice_kernel_path=Spice.instance.meta_kernel_path,
                                config=CONFIG)
                for tm_type, files in tm.items()
            ]

        for job in jobs:
            try:
                created_files = job.result()
                all_files.extend(created_files)
            except Exception as e:
                logger.error('Problem processing files', exc_info=True)
                if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                    raise e
        return list(set(all_files))


def process_tm_type(files, tm_type, processor, spice_kernel_path, config):
    all_files = []
    Spice.instance = Spice(spice_kernel_path)
    CONFIG = config

    # Stand alone packet data
    if (tm_type[0] == 21 and tm_type[-1] not in {20, 21, 22, 23, 24}) or tm_type[0] != 21:
        for file in files:
            levelb = Product(file)
            tmp = Product._check_registered_widget(
                level='L0', service_type=levelb.service_type,
                service_subtype=levelb.service_subtype, ssid=levelb.ssid,
                data=None, control=None)
            try:
                level0 = tmp.from_levelb(levelb, parent=file.name)
                if level0:
                    fits_files = processor.write_fits(level0)
                    all_files.extend(fits_files)
            except Exception as e:
                logger.error('Error processing file %s for %s, %s, %s', file,
                             levelb.service_type, levelb.service_subtype, levelb.ssid)
                logger.error('%s', e)
                if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                    raise e

    else:
        # for each file
        for file in files:
            levelb = Product(file)
            complete, _ = levelb.extract_sequences()

            if complete:
                for comp in complete:

                    # TODO need to carry better information for logging like index from
                    #  original files and file names
                    try:
                        tmp = Product._check_registered_widget(
                            level='L0', service_type=comp.service_type,
                            service_subtype=comp.service_subtype, ssid=comp.ssid, data=None,
                            control=None)
                        level0 = tmp.from_levelb(comp, parent=file.name)
                        fits_files = processor.write_fits(level0)
                        all_files.extend(fits_files)
                    except NotCombineException as nc:
                        logger.info(nc)
                    except Exception as e:
                        logger.error('Error processing file %s for %s, %s, %s', file,
                                     comp.service_type, comp.service_subtype, comp.ssid,
                                     exc_info=True)
                        logger.error('%s', e)
                        if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                            raise e
    return all_files


if __name__ == '__main__':
    tstart = perf_counter()

    warnings.filterwarnings('ignore', module='astropy.io.fits.card')

    fits_path = Path('/Users/shane/Projects/STIX/fits_test/LB/21/6/24/')
    bd = Path('/Users/shane/Projects/STIX/fits_test')

    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(_spm.get_latest_mk())

    l0processor = Level0(fits_path, bd)
    l0_files = l0processor.process_fits_files()
    logger.info(len(l0_files))

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

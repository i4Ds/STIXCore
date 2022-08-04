import warnings
from time import perf_counter
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from sunpy.util.datatype_factory_base import NoMatchError

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products import Product
from stixcore.products.level0.scienceL0 import NotCombineException
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class Level1:
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')))
        self.processor = FitsL1Processor(self.output_dir)

    def process_fits_files(self, files=None):
        all_files = list()
        if files is None:
            files = self.level0_files

        product_types = defaultdict(list)
        for file in files:
            # group by service,subservice,ssid example: 'L0/21/6/30'
            product_types[str(file.parent)].append(file)

        jobs = []
        with ProcessPoolExecutor() as executor:
            for pt, files in product_types.items():
                jobs.append(executor.submit(process_type, files,
                                            processor=FitsL1Processor(self.output_dir),
                                            soopmanager=SOOPManager.instance,
                                            spice_kernel_path=Spice.instance.meta_kernel_path,
                                            config=CONFIG))

        for job in jobs:
            try:
                new_files = job.result()
                all_files.extend(new_files)
            except Exception:
                logger.error('error', exc_info=True)

        return list(set(all_files))


def process_type(files, *, processor, soopmanager, spice_kernel_path, config):
    SOOPManager.instance = soopmanager
    all_files = list()
    Spice.instance = Spice(spice_kernel_path)
    CONFIG = config

    for file in files:
        l0 = Product(file)
        try:
            tmp = Product._check_registered_widget(level='L1', service_type=l0.service_type,
                                                   service_subtype=l0.service_subtype,
                                                   ssid=l0.ssid, data=None, control=None)
            l1 = tmp.from_level0(l0, parent=file.name)
            files = processor.write_fits(l1)
            all_files.extend(files)
        except NoMatchError:
            logger.warning('No match for product %s', l0)
        except NotCombineException as nc:
            logger.info(nc)
        except Exception as e:
            logger.error('Error processing file %s', file, exc_info=True)
            logger.error('%s', e)
            if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                raise e
    return all_files


if __name__ == '__main__':
    tstart = perf_counter()
    warnings.filterwarnings('ignore', module='astropy.io.fits.card')
    warnings.filterwarnings('ignore', module='stixcore.soop.manager')
    warnings.filterwarnings('ignore', module='astropy.utils.metadata')

    fits_path = Path('/home/shane/fits_test_latest/L0')
    bd = Path('/home/shane/fits_test_latest')

    # possible set an alternative spice kernel if not the latest should be used
    spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(spm.get_latest_mk())

    l1processor = Level1(fits_path, bd)
    all_files = l1processor.process_fits_files()
    logger.info(len(all_files))
    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

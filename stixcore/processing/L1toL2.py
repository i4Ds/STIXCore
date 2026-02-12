import warnings
from time import perf_counter
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from sunpy.util.datatype_factory_base import NoMatchError

from stixcore.config.config import CONFIG
from stixcore.ecc.manager import ECCManager
from stixcore.ephemeris.manager import Spice
from stixcore.io.product_processors.fits.processors import FitsL2Processor
from stixcore.processing.sswidl import SSWIDLProcessor
from stixcore.products import Product
from stixcore.products.level0.scienceL0 import NotCombineException
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name_and_path

logger = get_logger(__name__)


class Level2:
    """Processing step from L1 to L2.

    Input are L1 Fits files and the result is written to FITS as well. Daily products will be added
    to existing FITS files if already present.

    Groups all files into parallel processes by product type if daily products. For by request
    products it is also grouped by type but additional into batches of a given size.
    """

    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level1_files = sorted(list(self.source_dir.rglob("*.fits")))
        self.processor = FitsL2Processor(self.output_dir)

    def process_fits_files(self, files=None):
        all_files = list()
        if files is None:
            files = self.level1_files

        product_types = defaultdict(list)
        product_types_batch = defaultdict(int)
        batch_size = CONFIG.getint("Pipeline", "parallel_batchsize_L2", fallback=100)

        for file in files:
            # group by product: '(HK,maxi)'
            mission, level, identifier, *_ = file.name.split("_")
            tm_type = tuple(identifier.split("-")[1:])
            if tm_type[0] == "sci":
                product_types_batch[tm_type] += 1
            batch = product_types_batch[tm_type] // batch_size
            product_types[tm_type + (batch,)].append(file)

        jobs = []
        with ProcessPoolExecutor() as executor:
            for pt, files in product_types.items():
                jobs.append(
                    executor.submit(
                        process_type,
                        files,
                        processor=FitsL2Processor(self.output_dir),
                        soopmanager=SOOPManager.instance,
                        spice_kernel_path=Spice.instance.meta_kernel_path,
                        config=CONFIG,
                    )
                )

        for job in jobs:
            try:
                new_files = job.result()
                all_files.extend(new_files)
            except Exception:
                logger.error("error", exc_info=True)

        return list(set(all_files))


def process_type(files, *, processor, soopmanager, spice_kernel_path, config):
    SOOPManager.instance = soopmanager
    Spice.instance = Spice(spice_kernel_path)
    CONFIG = config

    all_files = list()
    max_idlbatch = CONFIG.getint("IDLBridge", "batchsize", fallback=20)
    idlprocessor = SSWIDLProcessor(processor)
    ecc_manager = ECCManager()
    for file in files:
        try:
            l1 = Product(file)
            tmp = Product._check_registered_widget(
                level="L2",
                service_type=l1.service_type,
                service_subtype=l1.service_subtype,
                ssid=l1.ssid,
                data=None,
                control=None,
            )

            # see https://github.com/i4Ds/STIXCore/issues/350
            complete_file_name = get_complete_file_name_and_path(file)
            for l2 in tmp.from_level1(
                l1, parent=complete_file_name, idlprocessor=idlprocessor, ecc_manager=ecc_manager
            ):
                new_files = processor.write_fits(l2)
                all_files.extend(new_files)
            # if a batch of X files have summed up run the IDL processing
            if idlprocessor.opentasks >= max_idlbatch:
                all_files.extend(idlprocessor.process())
                idlprocessor = SSWIDLProcessor(processor)
        except (NoMatchError, KeyError):
            logger.warning("No L2 product match for product %s", l1)
        except NotCombineException as nc:
            logger.info(nc)
        except Exception as e:
            logger.error("Error processing file %s", file, exc_info=True)
            logger.error("%s", e)
            if CONFIG.getboolean("Logging", "stop_on_error", fallback=False):
                raise e

    # if some more file batches left for IDL processing do it
    if idlprocessor.opentasks > 0:
        all_files.extend(idlprocessor.process())

    return all_files


if __name__ == "__main__":
    tstart = perf_counter()
    warnings.filterwarnings("ignore", module="astropy.io.fits.card")
    warnings.filterwarnings("ignore", module="stixcore.soop.manager")
    warnings.filterwarnings("ignore", module="astropy.utils.metadata")

    fits_path = Path("/home/shane/fits_test/L0")
    bd = Path("/home/shane/fits_test/")

    l2processor = Level2(fits_path, bd)
    all_files = l2processor.process_fits_files()
    logger.info(all_files)
    tend = perf_counter()
    logger.info("Time taken %f", tend - tstart)

import re
from enum import Enum
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice
from stixcore.io.fits.processors import FitsL1Processor, FitsL3Processor
from stixcore.products.product import GenericProduct
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name

__all__ = ['TestForProcessingResult', 'SingleProductProcessingStepMixin']

logger = get_logger(__name__)


class TestForProcessingResult(Enum):
    # do nothing with the candidate
    NotSuitable = 0
    # add to ignore list so it will not be tested again
    ToIgnore = 1
    # go on with the processing
    Suitable = 2


class SingleProcessingStepResult():
    def __init__(self, name: str, level: str, type: str, version: int,
                 out_path: Path, in_path: Path, date: datetime):
        """Creates a SingleProcessingStepResult

        Parameters
        ----------
        name : str
            the name of the generated product
        level : str
            the level of the generated product
        type : str
            the type of the generated product
        version : int
            the version of the generated product
        out_path : Path
            path to the generated file
        in_path : Path
            the path of the used input file
        date : datetime
            when was the processing performed
        """
        self.name = name
        self.level = level
        self.type = type
        self.version = version
        self.out_path = out_path
        self.in_path = in_path
        self.date = date


class SingleProductProcessingStepMixin():
    INPUT_PATTERN = "*.fits"
    VERSION_PATTERN = re.compile(r"(.*_V)([0-9]+)U?([\._].*)")

    @property
    def ProductInputPattern(cls):
        return cls.INPUT_PATTERN

    @property
    def test_for_processing(self, path: Path) -> TestForProcessingResult:
        pass

    def find_processing_candidates(self) -> list[Path]:
        pass

    def get_version(cls, candidates: list[Path], version='latest') -> list[Path]:

        if not version == "latest":
            version = int(str(version).lower().replace("v", ""))

        index = defaultdict(dict)
        for f in candidates:
            f_name = f.name
            match = cls.VERSION_PATTERN.match(f_name)
            if match:
                f_key = f"{match.group(1)}__{match.group(3)}"
                f_version = int(match.group(2))
            else:
                f_key = f_name
                f_version = -1

            index[f_key][f_version] = f

        version_files = []
        for f_key in index:
            versions = index[f_key].keys()
            v = max(versions) if version == "latest" else version
            if v in versions:
                version_files.append(index[f_key][v])

        return version_files

    def process(self, product: GenericProduct) -> GenericProduct:
        pass

    def write_fits(self, product: GenericProduct, folder: Path) -> Path:
        pass


# NOT NEEDED NOW
class FLLevel3:
    """Processing step from a flare list entry to L3.
    """
    def __init__(self, source_dir, output_dir, dbfile):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.dbfile = dbfile
        self.processor = FitsL3Processor(self.output_dir)

    def process_fits_files(self, files):
        all_files = list()
        if files is None:
            files = self.level0_files
        product_types = defaultdict(list)
        product_types_batch = defaultdict(int)
        batch_size = CONFIG.getint('Pipeline', 'parallel_batchsize_L1', fallback=150)

        for file in files:
            # group by service,subservice, ssid example: 'L0/21/6/30' as default
            # or (prio, service, subservice, [SSID], [BATCH]) if all data is available
            batch = 0
            prio = 3
            product_type = str(file.parent)
            if 'L0' in file._parts:
                product_type = tuple(map(int, file._parts[file._parts.index('L0')+1:-1]))
                if (product_type[0] == 21 and
                        product_type[-1] in {20, 21, 22, 23, 24, 42}):  # sci data
                    product_types_batch[product_type] += 1
                    prio = 2
                elif product_type[0] == 21:  # ql data
                    prio = 1
                batch = product_types_batch[product_type] // batch_size
            product_types[(prio, ) + product_type + (batch, )].append(file)

        jobs = []
        with ProcessPoolExecutor() as executor:
            # simple heuristic that the daily QL data takes longest so we start early
            for pt, files in sorted(product_types.items()):
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


def process_type(timeranges, productcls, flarelistparent, *, processor,
                 soopmanager, spice_kernel_path, config):
    SOOPManager.instance = soopmanager
    all_files = list()
    Spice.instance = Spice(spice_kernel_path)
    CONFIG = config
    file = 1
    prod = productcls()
    for tr in timeranges:
        logger.info(f"processing timerange: {timeranges}")
        try:
            # see https://github.com/i4Ds/STIXCore/issues/350
            get_complete_file_name(file.name)
            l3 = prod.from_timerange(tr, flarelistparent=flarelistparent)
            all_files.extend(processor.write_fits(l3))
        except Exception as e:
            logger.error('Error processing timerange %s', tr, exc_info=True)
            logger.error('%s', e)
            if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                raise e
    return all_files

from pathlib import Path
from datetime import datetime, timedelta

from stixpy.net.client import STIXClient

from astropy.io import fits
from astropy.table import QTable

from stixcore.ephemeris.manager import Spice
from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.processing.SingleStep import (
    SingleProcessingStepResult,
    SingleProductProcessingStepMixin,
    TestForProcessingResult,
)
from stixcore.products.level3.flarelist import (
    FlareList,
    FlarePeekPreviewMixin,
    FlarePositionMixin,
    FlareSOOPMixin,
)
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name_and_path

__all__ = ['FLtoFL']

logger = get_logger(__name__)


class FLtoFL(SingleProductProcessingStepMixin):
    """Processing step from enhance monthly flare list files to next processing steep
    """
    INPUT_PATTERN = "solo_ANC_stix-flarelist-sdc*.fits"

    def __init__(self, source_dir: Path, output_dir: Path, *,
                 input_pattern: str = '', products_in_out,
                 cadence: timedelta = timedelta(hours=5)):
        """Crates a new Processor.

        Parameters
        ----------
        source_dir : Path or pathlike
            where to search for input fits files
        output_dir : Path or pathlike
            where to write out the generated fits files
        """
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.cadence = cadence
        self.in_products = {}

        for in_product, out_product in products_in_out:
            self.in_products[in_product] = out_product

    def find_processing_candidates(self):
        """Performs file pattern search in the source directory

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        for product_in, product_out in self.in_products.items():
            pattern = (f"solo_{product_in.LEVEL}_stix-{product_in.TYPE}-"
                       f"{product_in.NAME}_*.fits")
            for c in self.source_dir.rglob(pattern):
                yield product_in, product_out, c

    def get_processing_files(self, phs: ProcessingHistoryStorage) -> list[Path]:
        """Performs file pattern search in the source directory

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        fl_to_process = list()
        for product_in, product_out, fl_can in self.find_processing_candidates():
            res = self.test_for_processing(fl_can, product_in, product_out, phs)
            if res == TestForProcessingResult.Suitable:
                fl_to_process.append((product_in, product_out, fl_can))
        return fl_to_process

    def test_for_processing(self, candidate: Path,
                            product_in: FlareList,
                            product_out: FlareList,
                            phm: ProcessingHistoryStorage) -> TestForProcessingResult:
        """_summary_

        Parameters
        ----------
        candidate : Path
            a fits file candidate
        phm : ProcessingHistoryStorage
            the processing history persistent handler

        Returns
        -------
        TestForProcessingResult
         what should happen with the candidate in the next processing step
        """
        try:
            c_header = fits.getheader(candidate)
            f_data_end = datetime.fromisoformat(c_header['DATE-END'])
            f_create_date = datetime.fromisoformat(c_header['DATE'])

            cfn = get_complete_file_name_and_path(candidate)

            wp = phm.has_processed_fits_products(product_out.NAME,
                                                 product_out.LEVEL,
                                                 product_out.TYPE,
                                                 product_out.get_cls_processing_version(),
                                                 str(cfn), f_create_date)

            # found already in the processing history
            if wp:
                return TestForProcessingResult.NotSuitable

            # safety margin of 1day until we process higher products with position and pointing
            # only use flown spice kernels not predicted once as pointing information
            # can be "very off"
            if (f_data_end > (Spice.instance.get_mk_date(meta_kernel_type="flown")
                              - timedelta(hours=24))):
                return TestForProcessingResult.NotSuitable

            # safety margin of x until we start with processing the list files
            if (f_create_date >= (datetime.now() - self.cadence)):
                return TestForProcessingResult.NotSuitable

            return TestForProcessingResult.Suitable
        except Exception as e:
            logger.error(e)
        return TestForProcessingResult.NotSuitable

    def process_fits_files(self, flarelists, *, soopmanager: SOOPManager,
                           spice_kernel_path: Path, fl_processor,
                           img_processor, config) -> list[Path]:
        """Performs the processing (expected to run in a dedicated python process) from a
        list flare list product to an enhanced flare list product.

        Parameters
        ----------
        files : list[Path]
             list input flare list product fits files
        soopmanager : SOOPManager
            the SOOPManager from the main process
        spice_kernel_path : Path
            the used spice kernel paths from the main process
        fl_processor : _type_
            the processor from the main process
        config :
            the config from the main process

        Returns
        -------
        list[Path]
            list of all generated fits files
        """
        CONFIG = config
        SOOPManager.instance = soopmanager
        Spice.instance = Spice(spice_kernel_path)
        fido_client = STIXClient(source=CONFIG.get('Paths', 'fido_search_url',
                                                   fallback=STIXClient.baseurl))
        all_files = list()

        for in_product, out_product, file_path in flarelists:
            try:
                prod = Product(file_path)
                control = QTable()

                data = prod.data
                energy = prod.energies
                month = prod.utc_timerange.start.datetime.date()

                # add flare position if not already present
                if issubclass(out_product, FlarePositionMixin) and \
                   not issubclass(in_product, FlarePositionMixin):
                    out_product.add_flare_position(data, fido_client, month=month)

                # add soop information if not already present
                if issubclass(out_product, FlareSOOPMixin) and \
                   not issubclass(in_product, FlareSOOPMixin):
                    out_product.add_soop(data)

                # add peek preview images if not already present
                if issubclass(out_product, FlarePeekPreviewMixin) and \
                   not issubclass(in_product, FlarePeekPreviewMixin):
                    out_product.add_peek_preview(data, energy, file_path.name, fido_client,
                                                 img_processor, month=month)

                out_prod = out_product(control=control, data=data, month=month, energy=energy)
                out_prod.parent = file_path.name

                # call upgrade method to enhance the product
                out_prod.enhance_from_product(prod)

                new_f = [SingleProcessingStepResult(out_product.NAME, out_product.LEVEL,
                                                    out_product.TYPE,
                                                    out_product.get_cls_processing_version(),
                                                    fop, get_complete_file_name_and_path(file_path),
                                                    datetime.now())
                         for fop in fl_processor.write_fits(out_prod)]

                all_files.extend(new_f)
            except Exception as e:
                logger.error(e, stack_info=True)

        return all_files

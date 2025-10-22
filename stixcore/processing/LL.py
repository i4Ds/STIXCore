from pathlib import Path
from datetime import datetime, timedelta

from astropy.io import fits

from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.io.product_processors.plots.processors import PlotProcessor
from stixcore.processing.SingleStep import (
    SingleProcessingStepResult,
    SingleProductProcessingStepMixin,
    TestForProcessingResult,
)
from stixcore.products.level1.quicklookL1 import LightCurve
from stixcore.products.lowlatency.quicklookLL import LightCurveL3
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name_and_path

__all__ = ["LL03QL"]

logger = get_logger(__name__)


class LL03QL(SingleProductProcessingStepMixin):
    """Processing step from a ql ligtcurve L1 fits file to LL level 3 plot chart image."""

    INPUT_PATTERN = "solo_ANC_stix-flarelist-sdc*.fits"

    def __init__(
        self,
        source_dir: Path,
        output_dir: Path,
        *,
        input_pattern: str = "",
        in_product: LightCurve,
        out_product: LightCurveL3,
        cadence: timedelta = timedelta(minutes=1),
    ):
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
        self.in_product = in_product
        self.out_product = out_product

        self.input_pattern = "solo_L1_stix-ql-lightcurve_*.fits"

    def find_processing_candidates(self) -> list[Path]:
        """Performs file pattern search in the source directory

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        return list(self.source_dir.rglob(self.input_pattern))

    def get_processing_files(self, phs: ProcessingHistoryStorage) -> list[Path]:
        """find all input files that are ready for processing

        Returns
        -------
        list[Path]
            a list of fits files for processing
        """
        ql_to_process = list()
        for ql_can in self.find_processing_candidates():
            if self.test_for_processing(ql_can, phs) == TestForProcessingResult.Suitable:
                ql_to_process.append(ql_can)
        return ql_to_process

    def test_for_processing(self, candidate: Path, phm: ProcessingHistoryStorage) -> TestForProcessingResult:
        """Performs a check on each found candidate if it should be processed or skipped

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
            f_data_end = datetime.fromisoformat(c_header["DATE-END"])
            f_create_date = datetime.fromisoformat(c_header["DATE"])

            cfn = get_complete_file_name_and_path(candidate)

            wp = phm.has_processed_fits_products(
                self.out_product.NAME,
                self.out_product.LEVEL,
                self.out_product.TYPE,
                self.out_product.get_cls_processing_version(),
                str(cfn),
                f_create_date,
            )

            # found already in the processing history
            if wp:
                return TestForProcessingResult.NotSuitable

            # safety margin of x until we start with processing the list files
            if f_create_date >= (datetime.now() - self.cadence):
                return TestForProcessingResult.NotSuitable

            # only process QL files with data for full 24 hours no created of half a day plots
            header_data = fits.getheader(candidate, "DATA")
            tbins = header_data["NAXIS2"]
            # small rounding margin of 4 minutes is allowed (-60)
            # input files oldr then a month will be processed also as for not full 24 hours
            file_age = (datetime.now() - f_data_end).days
            if (tbins < 60 / 4 * 60 * 24 - 60) and file_age < 30:
                return TestForProcessingResult.NotSuitable

            return TestForProcessingResult.Suitable
        except Exception as e:
            logger.error(e)
        return TestForProcessingResult.NotSuitable

    def process_fits_files(
        self, files: list[Path], *, soopmanager: SOOPManager, spice_kernel_path: Path, processor: PlotProcessor, config
    ) -> list[Path]:
        """Performs the processing (expected to run in a dedicated python process) from a
        list of ql lightcurve products into LL plots.

        Parameters
        ----------
        files : list[Path]
             list input flare list product fits files
        processor : _type_
            the processor from the main process
        config :
            the config from the main process

        Returns
        -------
        list[Path]
            list of all generated plot files
        """

        all_files = list()

        for file_path in files:
            try:
                in_prod = Product(file_path)

                control = in_prod.control
                data = in_prod.data
                energy = in_prod.energies

                out_prod = self.out_product(
                    control=control, data=data, energy=energy, parent_file_path=file_path, header=in_prod.fits_header
                )

                plot_file = processor.write_plot(out_prod)
                logger.info(f"Generated plot file: {plot_file}")

                new_f = SingleProcessingStepResult(
                    self.out_product.NAME,
                    self.out_product.LEVEL,
                    self.out_product.TYPE,
                    self.out_product.get_cls_processing_version(),
                    plot_file,
                    get_complete_file_name_and_path(file_path),
                    datetime.now(),
                )

                all_files.append(new_f)
            except Exception as e:
                logger.error(e, stack_info=True)

        return all_files

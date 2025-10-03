from pathlib import Path
from datetime import date, datetime, timedelta

import pandas as pd
from stixpy.net.client import STIXClient

from stixcore.ephemeris.manager import Spice
from stixcore.io import FlareListManager
from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.processing.SingleStep import (
    SingleProcessingStepResult,
    SingleProductProcessingStepMixin,
    TestForProcessingResult,
)
from stixcore.products.level3.flarelist import FlarePositionMixin, FlareSOOPMixin
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

__all__ = ["FlareListL3"]

logger = get_logger(__name__)


class FlareListL3(SingleProductProcessingStepMixin):
    """Processing step from a FlareListManager to monthly solo_L3_stix-flarelist-*.fits file."""

    STARTDATE = date(2024, 1, 1)

    def __init__(self, flm: FlareListManager, output_dir: Path):
        """Crates a new Processor.

        Parameters
        ----------
        flm : FlareListManager
            a FlareListManager instance
        output_dir : Path or pathlike
            where to write out the generated fits files
        """
        self.flm = flm
        self.output_dir = Path(output_dir)

    def find_processing_months(self, phs: ProcessingHistoryStorage) -> list[date]:
        """Performs file pattern search in the source directory

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        fl_months = list()
        for month in pd.date_range(FlareListL3.STARTDATE, date.today(), freq="MS").date:
            if self.test_for_processing(month, phs) == TestForProcessingResult.Suitable:
                fl_months.append(month)
        return fl_months

    def test_for_processing(self, month: date, phm: ProcessingHistoryStorage) -> TestForProcessingResult:
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
            wp = phm.has_processed_fits_products(
                self.flm.productCls.NAME,
                self.flm.productCls.LEVEL,
                self.flm.productCls.TYPE,
                self.flm.productCls.get_cls_processing_version(),
                str(month),
                FlareListL3.STARTDATE,
            )

            # found already in the processing history
            if wp:
                return TestForProcessingResult.NotSuitable

            # safety margin of 2 month until we start with processing of monthly flare list files
            if month >= (date.today() - timedelta(days=50)).replace(day=1):
                return TestForProcessingResult.NotSuitable

            return TestForProcessingResult.Suitable
        except Exception as e:
            logger.error(e)
        return TestForProcessingResult.NotSuitable

    def process_fits_files(
        self, months: list[date], *, soopmanager: SOOPManager, spice_kernel_path: Path, processor, config
    ) -> list[Path]:
        """Performs the processing (expected to run in a dedicated python process) from a
        list of time periods (months) and creates a FITS file for each one.

        Parameters
        ----------
        files : list[Path]
             list of HK level 1  fits files
        soopmanager : SOOPManager
            the SOOPManager from the main process
        spice_kernel_path : Path
            the used spice kernel paths from the main process
        fits_processor : _type_
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
        all_files = list()
        fido_url = CONFIG.get("Paths", "fido_search_url", fallback="https://pub099.cs.technik.fhnw.ch/data/fits")
        fido_client = STIXClient(source=fido_url)

        for month in months:
            data, control, energy = self.flm.get_data(
                start=month, end=(month + pd.DateOffset(months=1)).date(), fido_client=fido_client
            )

            if not data:
                logger.warning(f"No data found for month {month}")
                continue

            if issubclass(self.flm.productCls, FlarePositionMixin):
                self.flm.productCls.add_flare_position(
                    data, fido_client, filter_function=self.flm.filter_flare_function
                )  # noqa

            if issubclass(self.flm.productCls, FlareSOOPMixin):
                self.flm.productCls.add_soop(data)

            anc = self.flm.productCls(control=control, data=data, month=month, energy=energy)

            all_files.extend(
                [
                    SingleProcessingStepResult(
                        anc.name, anc.level, anc.type, anc.get_processing_version(), fop, str(month), datetime.now()
                    )
                    for fop in processor.write_fits(anc)
                ]
            )
        return all_files

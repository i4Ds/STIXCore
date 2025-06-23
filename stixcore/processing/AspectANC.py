from pathlib import Path
from datetime import datetime, timedelta

import numpy as np

from astropy.io import fits
from astropy.table import QTable

from stixcore.ephemeris.manager import Spice
from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.processing.SingleStep import SingleProductProcessingStepMixin, TestForProcessingResult
from stixcore.processing.sswidl import SSWIDLProcessor
from stixcore.products.ANC.aspect import AspectIDLProcessing, Ephemeris
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name_and_path, get_incomplete_file_name_and_path

__all__ = ["AspectANC"]

logger = get_logger(__name__)


class AspectANC(SingleProductProcessingStepMixin):
    """Processing step from a HK L1 fits file to a solo_ANC_stix-asp-ephemeris*.fits file."""

    INPUT_PATTERN = "solo_L1_stix-hk-maxi_*.fits"

    def __init__(self, source_dir: Path, output_dir: Path):
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

    def get_processing_files(self, phs) -> list[Path]:
        """Returns a list of all files which should be processed by this step.

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        hk_in_files = []

        for fc in self.find_processing_candidates():
            tr = self.test_for_processing(fc, phs)
            if tr == TestForProcessingResult.Suitable:
                hk_in_files.append(fc)
        return hk_in_files

    def find_processing_candidates(self) -> list[Path]:
        """Performs file pattern search in the source directory

        Returns
        -------
        list[Path]
            a list of fits files candidates
        """
        return list(self.source_dir.rglob(self.ProductInputPattern))

    def test_for_processing(self, candidate: Path, phm: ProcessingHistoryStorage) -> TestForProcessingResult:
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
            f_data_end = datetime.fromisoformat(c_header["DATE-END"])
            f_create_date = datetime.fromisoformat(c_header["DATE"])
            f_version = int(c_header["VERSION"])

            cfn = get_complete_file_name_and_path(candidate)

            was_processed = phm.has_processed_fits_products(
                "ephemeris", "ANC", "asp", Ephemeris.get_cls_processing_version(), str(cfn), f_create_date
            )

            # found already in the processing history
            if was_processed:
                return TestForProcessingResult.NotSuitable

            # test if the version of the input fits files is older as the target version
            # TODO check if this is not a to restrictive check
            # search danamic for the latest version
            if f_version < Ephemeris.get_cls_processing_version():
                return TestForProcessingResult.NotSuitable

            # safety margin of 1day until we start with processing of HK files into ANC-asp files
            # only use flown spice kernels not predicted once as pointing information
            # can be "very off"
            if f_data_end > (Spice.instance.get_mk_date(meta_kernel_type="flown") - timedelta(hours=24)):
                return TestForProcessingResult.NotSuitable

            return TestForProcessingResult.Suitable
        except Exception as e:
            logger.error(e)
        return TestForProcessingResult.NotSuitable

    def process_fits_files(
        self, files: list[Path], *, soopmanager: SOOPManager, spice_kernel_path: Path, processor, config
    ) -> list[Path]:
        """Performs the processing (expected to run in a dedicated python process) from a
        list of solo_L1_stix-hk-maxi_*.fits into solo_ANC_stix-asp-ephemeris*.fits files.

        Parameters
        ----------
        files : list[Path]
             list of HK level 1  fits files
        soopmanager : SOOPManager
            the SOOPManager from the main process
        spice_kernel_path : Path
            the used spice kernel paths from the main process
        processor : _type_
            the processor from the main process
        config :
            the config from the main process

        Returns
        -------
        list[Path]
            list of all generated fits files
        """
        CONFIG = config
        max_idlbatch = CONFIG.getint("IDLBridge", "batchsize", fallback=20)
        SOOPManager.instance = soopmanager
        Spice.instance = Spice(spice_kernel_path)
        all_files = list()

        idlprocessor = SSWIDLProcessor(processor)

        for pf in files:
            l1hk = Product(pf)

            data = QTable()
            data["cha_diode0"] = l1hk.data["hk_asp_photoa0_v"]
            data["cha_diode1"] = l1hk.data["hk_asp_photoa1_v"]
            data["chb_diode0"] = l1hk.data["hk_asp_photob0_v"]
            data["chb_diode1"] = l1hk.data["hk_asp_photob1_v"]
            data["time"] = [d.strftime("%Y-%m-%dT%H:%M:%S.%f") for d in l1hk.data["time"].to_datetime()]
            data["scet_time_f"] = l1hk.data["time"].fine
            data["scet_time_c"] = l1hk.data["time"].coarse

            # TODO set to seconds
            dur = (l1hk.data["time"][1:] - l1hk.data["time"][0:-1]).as_float().value
            data["duration"] = dur[0]
            data["duration"][0:-1] = dur
            data["duration"][:] = dur[-1]

            data["spice_disc_size"] = [Spice.instance.get_sun_disc_size(date=d) for d in l1hk.data["time"]]

            data["y_srf"] = 0.0
            data["z_srf"] = 0.0
            data["calib"] = 0.0
            data["sas_ok"] = np.byte(0)
            data["error"] = ""
            data["control_index"] = l1hk.data["control_index"]

            dataobj = dict()
            for coln in data.colnames:
                dataobj[coln] = data[coln].value.tolist()

            f = {"parentfits": str(get_incomplete_file_name_and_path(pf)), "data": dataobj}

            idlprocessor[AspectIDLProcessing].params["hk_files"].append(f)
            idlprocessor.opentasks += 1

            if idlprocessor.opentasks >= max_idlbatch:
                all_files.extend(idlprocessor.process())
                idlprocessor = SSWIDLProcessor(processor)

        if idlprocessor.opentasks > 0:
            all_files.extend(idlprocessor.process())

        return all_files

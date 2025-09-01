import sys
import shutil
import logging
import smtplib
import argparse
from pathlib import Path
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor

from stixpy.net.client import STIXClient

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.fits.processors import (
    FitsANCProcessor,
    FitsL2Processor,
    FitsL3Processor,
    PlotProcessor,
)
from stixcore.io.FlareListManager import SCFlareListManager, SDCFlareListManager
from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.io.RidLutManager import RidLutManager
from stixcore.processing.AspectANC import AspectANC
from stixcore.processing.FlareListL3 import FlareListL3
from stixcore.processing.FLtoFL import FLtoFL
from stixcore.processing.LL import LL03QL
from stixcore.processing.pipeline import PipelineStatus
from stixcore.processing.SingleStep import SingleProcessingStepResult
from stixcore.products.level1.quicklookL1 import LightCurve
from stixcore.products.level3.flarelist import (
    FlarelistSC,
    FlarelistSCLoc,
    FlarelistSCLocImg,
    FlarelistSDC,
    FlarelistSDCLoc,
    FlarelistSDCLocImg,
)
from stixcore.products.lowlatency.quicklookLL import LightCurveL3
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import STX_LOGGER_DATE_FORMAT, STX_LOGGER_FORMAT, get_logger

logger = get_logger(__name__)


class DailyPipelineErrorReport(logging.StreamHandler):
    """Adds file and mail report Handler to a processing step."""

    def __init__(self, log_file: Path, log_level):
        """Create a PipelineErrorReport"""
        logging.StreamHandler.__init__(self)

        self.log_file = log_file
        self.err_file = log_file.with_suffix(".err")

        self.fh = logging.FileHandler(filename=self.log_file, mode="a+")
        self.fh.setFormatter(logging.Formatter(STX_LOGGER_FORMAT, datefmt=STX_LOGGER_DATE_FORMAT))
        self.fh.setLevel(logging.getLevelName(log_level))

        self.setLevel(logging.ERROR)
        self.allright = True
        self.error = None
        logging.getLogger().addHandler(self)
        logging.getLogger().addHandler(self.fh)

    def emit(self, record):
        """Called in case of a logging event."""
        self.allright = False
        self.error = record

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        logging.getLogger().removeHandler(self)
        self.fh.flush()
        logging.getLogger().removeHandler(self.fh)

        if not self.allright:
            shutil.copyfile(self.log_file, self.err_file)
            if CONFIG.getboolean("Pipeline", "error_mail_send", fallback=False):
                try:
                    sender = CONFIG.get("Pipeline", "error_mail_sender", fallback="")
                    receivers = CONFIG.get("Pipeline", "error_mail_receivers").split(",")
                    host = CONFIG.get("Pipeline", "error_mail_smpt_host", fallback="localhost")
                    port = CONFIG.getint("Pipeline", "error_mail_smpt_port", fallback=25)
                    smtp_server = smtplib.SMTP(host=host, port=port)
                    message = f"""Subject: StixCore Daily Processing Error

Error while processing

login to pub099.cs.technik.fhnw.ch and check:

{self.err_file}

StixCore
==========================
do not answer to this mail.
"""
                    smtp_server.sendmail(sender, receivers, message)
                except Exception as e:
                    logger.error(f"Error: unable to send error email: {e}")


def run_daily_pipeline(args):
    """CLI STIX daily processing step to generate fits products

    Parameters
    ----------
    args : list
        see -h for details

    Returns
    -------
    list
        list of generated fits files paths
    """

    parser = argparse.ArgumentParser(
        description="STIX publish to ESA processing step", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # paths
    parser.add_argument(
        "-d",
        "--db_file",
        help="Path to the history publishing database",
        type=str,
        default=CONFIG.get("Pipeline", "history_db_file", fallback=str(Path.home() / "processed.sqlite")),
    )

    parser.add_argument(
        "-i", "--fits_in_dir", help="input fits directory", default=CONFIG.get("Paths", "fits_archive"), type=str
    )

    parser.add_argument(
        "-o", "--fits_out_dir", help="output fits directory", default=CONFIG.get("Paths", "fits_archive"), type=str
    )

    parser.add_argument(
        "-s",
        "--spice_dir",
        help="directory to the spice kernels files",
        default=CONFIG.get("Paths", "spice_kernels"),
        type=str,
    )

    parser.add_argument("-S", "--spice_file", help="path to the spice meta kernel", default=None, type=str)

    parser.add_argument(
        "-p", "--soop_dir", help="directory to the SOOP files", default=CONFIG.get("Paths", "soop_files"), type=str
    )

    parser.add_argument(
        "-O",
        "--log_dir",
        help="output directory for daily logging ",
        default=CONFIG.get("Publish", "log_dir", fallback=str(Path.home())),
        type=str,
        dest="log_dir",
    )

    parser.add_argument(
        "--continue_on_error",
        help="the pipeline reports any error and continues processing",
        default=not CONFIG.getboolean("Logging", "stop_on_error", fallback=False),
        action="store_false",
        dest="stop_on_error",
    )

    parser.add_argument(
        "--log_level",
        help="the level of logging",
        default="INFO",
        type=str,
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
        dest="log_level",
    )

    parser.add_argument("-r", "--rid_lut_file",
                        help=("Path to the rid LUT file"),
                        default=CONFIG.get('Publish', 'rid_lut_file'), type=str)

    args = parser.parse_args(args)

    # paths
    CONFIG.set("Paths", "fits_archive", args.fits_in_dir)
    CONFIG.set("Paths", "spice_kernels", args.spice_dir)
    CONFIG.set("Paths", "soop_files", args.soop_dir)

    # logging
    CONFIG.set("Logging", "stop_on_error", str(args.stop_on_error))

    # generate a log file for each run and an error file in case of any errors
    processing_day = date.today().strftime("%Y%m%d")
    with DailyPipelineErrorReport(Path(args.log_dir) / f"dailypipeline_{processing_day}.log", args.log_level):
        # set up the singletons
        if args.spice_file:
            spicemeta = [SpiceKernelManager.get_mk_meta(Path(args.spice_file))]
        else:
            _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
            spicemeta = _spm.get_latest_mk_and_pred()

        Spice.instance = Spice(spicemeta)

        SOOPManager.instance = SOOPManager(Path(CONFIG.get('Paths', 'soop_files')))

        Path(CONFIG.get("Paths", "fits_archive"))

        fido_url = CONFIG.get('Paths', 'fido_search_url',
                              fallback='https://pub099.cs.technik.fhnw.ch/data/fits')
        fido_client = STIXClient(source=fido_url)

        flare_lut_file = Path(CONFIG.get('Pipeline', 'flareid_sc_lut_file'))
        SCFlareListManager.instance = SCFlareListManager(flare_lut_file, fido_client, update=True)

        flare_lut_file = Path(CONFIG.get('Pipeline', 'flareid_sdc_lut_file'))
        SDCFlareListManager.instance = SDCFlareListManager(flare_lut_file, update=False)

        RidLutManager.instance = RidLutManager(Path(CONFIG.get('Publish', 'rid_lut_file')),
                                               update=False)

        db_file = Path(args.db_file)
        fits_in_dir = Path(args.fits_in_dir)
        if not fits_in_dir.exists():
            logger.error(f"path not found to input files: {fits_in_dir}")
            return

        fits_out_dir = Path(args.fits_out_dir)
        if not fits_out_dir.exists():
            logger.error(f"path not found to input files: {fits_out_dir}")
            return

        PipelineStatus.log_setup()

        logger.info("PARAMETER:")
        logger.info(f"db_file: {db_file}")
        logger.info(f"fits_in_dir: {fits_in_dir}")
        logger.info(f"fits_out_dir: {fits_out_dir}")
        logger.info(f"send Mail report: {CONFIG.getboolean('Pipeline', 'error_mail_send')}")
        logger.info(f"receivers: {CONFIG.get('Pipeline', 'error_mail_receivers')}")
        logger.info(f"log dir: {args.log_dir}")
        logger.info(f"log level: {args.log_level}")

        logger.info("\nstart daily pipeline\n")

        phs = ProcessingHistoryStorage(db_file)

        aspect_anc_processor = AspectANC(fits_in_dir, fits_out_dir)

        flarelist_sdc = FlareListL3(SDCFlareListManager.instance, fits_out_dir)
        flarelist_sc = FlareListL3(SCFlareListManager.instance, fits_out_dir)
        fl_to_fl = FLtoFL(fits_in_dir,
                          fits_out_dir,
                          products_in_out=[(FlarelistSDC, FlarelistSDCLoc),
                                           (FlarelistSDCLoc, FlarelistSDCLocImg),
                                           (FlarelistSC, FlarelistSCLoc),
                                           (FlarelistSCLoc, FlarelistSCLocImg)],
                          cadence=timedelta(seconds=1))

        ll03ql = LL03QL(fits_in_dir, fits_out_dir, in_product=LightCurve, out_product=LightCurveL3,
                        cadence=timedelta(seconds=1))

        plot_writer = PlotProcessor(fits_out_dir)
        l2_fits_writer = FitsL2Processor(fits_out_dir)
        l3_fits_writer = FitsL3Processor(fits_out_dir)
        anc_fits_writer = FitsANCProcessor(fits_out_dir)

        # hk_in_files = aspect_anc_processor.get_processing_files(phs)
        hk_in_files = []

        fl_sdc_months = flarelist_sdc.find_processing_months(phs)
        fl_sdc_months = []

        # fl_sc_months = flarelist_sc.find_processing_months(phs)
        fl_sc_months = []

        ll_candidates = ll03ql.get_processing_files(phs)
        # ll_candidates = []
        # ll_candidates = ll_candidates[0:40]

        fl_to_fl_files = fl_to_fl.get_processing_files(phs)
        fl_to_fl_files = []

        # all processing files should be terminated before the next step as the different
        # processing steeps might create new candidates
        # let each processing "task" run in its own process
        jobs = []
        with ProcessPoolExecutor() as executor:
            jobs.append(executor.submit(aspect_anc_processor.process_fits_files, hk_in_files,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        processor=l2_fits_writer,
                                        config=CONFIG))

            jobs.append(executor.submit(flarelist_sdc.process_fits_files, fl_sdc_months,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        processor=l3_fits_writer,
                                        config=CONFIG))

            jobs.append(executor.submit(flarelist_sc.process_fits_files, fl_sc_months,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        processor=l3_fits_writer,
                                        config=CONFIG))

            # TODO a owen processing step for each flarelist file?
            # for fl_to_fl_file in fl_to_fl_files:
            jobs.append(executor.submit(fl_to_fl.process_fits_files,
                                        fl_to_fl_files,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        fl_processor=anc_fits_writer,
                                        img_processor=l3_fits_writer,
                                        config=CONFIG))

            jobs.append(executor.submit(ll03ql.process_fits_files, ll_candidates,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        processor=plot_writer,
                                        config=CONFIG))

        # wait for all processes to end
        all_files = []
        for job in jobs:
            try:
                # collect all generated fits files form each process
                new_files = job.result()
                all_files.extend(new_files)
            except Exception:
                logger.error("error", exc_info=True)

        # create an entry for each generated file in the ProcessingHistoryStorage
        for pr in all_files:
            if isinstance(pr, SingleProcessingStepResult):
                phs.add_processed_fits_products(pr.name, pr.level, pr.type,
                                                pr.version, pr.in_path,
                                                pr.out_path, pr.date)

        phs.close()

        all_files = list(set(all_files))

        # write out all generated fits file in a dedicated log file
        out_file = Path(args.log_dir) / f"dailypipeline_{processing_day}.out"
        with open(out_file, "a+") as res_f:
            for f in all_files:
                res_f.write(f"{str(f.in_path)}\n")

    return all_files


def main():
    run_daily_pipeline(sys.argv[1:])


if __name__ == "__main__":
    main()

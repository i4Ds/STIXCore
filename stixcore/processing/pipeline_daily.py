import sys
import shutil
import logging
import smtplib
import argparse
from pathlib import Path
from datetime import date
from concurrent.futures import ProcessPoolExecutor

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.fits.processors import FitsL2Processor
from stixcore.io.ProcessingHistoryStorage import ProcessingHistoryStorage
from stixcore.io.RidLutManager import RidLutManager
from stixcore.processing.AspectANC import AspectANC
from stixcore.processing.pipeline import PipelineStatus
from stixcore.processing.SingleStep import SingleProcessingStepResult, TestForProcessingResult
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import STX_LOGGER_DATE_FORMAT, STX_LOGGER_FORMAT, get_logger
from stixcore.util.util import get_complete_file_name_and_path

logger = get_logger(__name__)


class DailyPipelineErrorReport(logging.StreamHandler):
    """Adds file and mail report Handler to a processing step."""
    def __init__(self, log_file: Path, log_level):
        """Create a PipelineErrorReport
        """
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
            if CONFIG.getboolean('Pipeline', 'error_mail_send', fallback=False):
                try:
                    sender = CONFIG.get('Pipeline', 'error_mail_sender', fallback='')
                    receivers = CONFIG.get('Pipeline', 'error_mail_receivers').split(",")
                    host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
                    port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
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

    parser = argparse.ArgumentParser(description='STIX publish to ESA processing step',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # pathes
    parser.add_argument("-d", "--db_file",
                        help="Path to the history publishing database", type=str,
                        default=CONFIG.get('Pipeline', 'history_db_file',
                                           fallback=str(Path.home() / "processed.sqlite")))

    parser.add_argument("-i", "--fits_in_dir",
                        help="input fits directory",
                        default=CONFIG.get('Paths', 'fits_archive'), type=str)

    parser.add_argument("-o", "--fits_out_dir",
                        help="output fits directory",
                        default=CONFIG.get('Paths', 'fits_archive'), type=str)

    parser.add_argument("-s", "--spice_dir",
                        help="directory to the spice kernels files",
                        default=CONFIG.get('Paths', 'spice_kernels'), type=str)

    parser.add_argument("-S", "--spice_file",
                        help="path to the spice meta kernel",
                        default=None, type=str)

    parser.add_argument("-p", "--soop_dir",
                        help="directory to the SOOP files",
                        default=CONFIG.get('Paths', 'soop_files'), type=str)

    parser.add_argument("-O", "--log_dir",
                        help="output directory for daily logging ",
                        default=CONFIG.get('Publish', 'log_dir', fallback=str(Path.home())),
                        type=str, dest='log_dir')

    parser.add_argument("--continue_on_error",
                        help="the pipeline reports any error and continues processing",
                        default=not CONFIG.getboolean('Logging', 'stop_on_error', fallback=False),
                        action='store_false', dest='stop_on_error')

    parser.add_argument("--log_level",
                        help="the level of logging",
                        default="INFO", type=str,
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
                        dest='log_level')

    parser.add_argument("-r", "--rid_lut_file",
                        help=("Path to the rid LUT file"),
                        default=CONFIG.get('Publish', 'rid_lut_file'), type=str)

    parser.add_argument("--update_rid_lut",
                        help="update rid lut file before publishing",
                        default=False,
                        action='store_true', dest='update_rid_lut')

    args = parser.parse_args(args)

    # pathes
    CONFIG.set('Paths', 'fits_archive', args.fits_in_dir)
    CONFIG.set('Paths', 'spice_kernels', args.spice_dir)
    CONFIG.set('Paths', 'soop_files', args.soop_dir)

    # logging
    CONFIG.set('Logging', 'stop_on_error', str(args.stop_on_error))

    # generate a log file for each run and an error file in case of any errors
    with DailyPipelineErrorReport(Path(args.log_dir) /
                                  f"dailypipeline_{date.today().strftime('%Y%m%d')}.log",
                                  args.log_level):

        # set up the singletons
        if args.spice_file:
            spicemeta = [SpiceKernelManager.get_mk_meta(Path(args.spice_file))]
        else:
            _spm = SpiceKernelManager(Path(CONFIG.get('Paths', 'spice_kernels')))
            spicemeta = _spm.get_latest_mk_and_pred()

        Spice.instance = Spice(spicemeta)

        SOOPManager.instance = SOOPManager(Path(CONFIG.get('Paths', 'soop_files')))

        RidLutManager.instance = RidLutManager(Path(args.rid_lut_file), update=args.update_rid_lut)

        Path(CONFIG.get('Paths', 'fits_archive'))

        db_file = Path(args.db_file)
        fits_in_dir = Path(args.fits_in_dir)
        if not fits_in_dir.exists():
            logger.error(f'path not found to input files: {fits_in_dir}')
            return

        fits_out_dir = Path(args.fits_out_dir)
        if not fits_out_dir.exists():
            logger.error(f'path not found to input files: {fits_out_dir}')
            return

        PipelineStatus.log_setup()

        logger.info("PARAMETER:")
        logger.info(f'db_file: {db_file}')
        logger.info(f'fits_in_dir: {fits_in_dir}')
        logger.info(f'fits_out_dir: {fits_out_dir}')
        logger.info(f"send Mail report: {CONFIG.getboolean('Pipeline', 'error_mail_send')}")
        logger.info(f"receivers: {CONFIG.get('Pipeline', 'error_mail_receivers')}")
        logger.info(f'log dir: {args.log_dir}')
        logger.info(f'log level: {args.log_level}')

        logger.info("\nstart daily pipeline\n")

        phs = ProcessingHistoryStorage(db_file)

        aspect_anc_processor = AspectANC(fits_in_dir, fits_out_dir)

        l2_fits_writer = FitsL2Processor(fits_out_dir)

        # should be done later: internally
        hk_in_files = []
        candidates = aspect_anc_processor.find_processing_candidates()
        v_candidates = aspect_anc_processor.get_version(candidates, version="latest")
        for fc in v_candidates:
            tr = aspect_anc_processor.test_for_processing(fc, phs)
            if tr == TestForProcessingResult.Suitable:
                hk_in_files.append(fc)

        # let each processing "task" run in its own process
        jobs = []
        with ProcessPoolExecutor() as executor:
            jobs.append(executor.submit(aspect_anc_processor.process_fits_files, hk_in_files,
                                        soopmanager=SOOPManager.instance,
                                        spice_kernel_path=Spice.instance.meta_kernel_path,
                                        processor=l2_fits_writer,
                                        config=CONFIG))

        # wait for all processes to end
        all_files = []
        for job in jobs:
            try:
                # collect all generated fits files form each process
                new_files = job.result()
                all_files.extend(new_files)
            except Exception:
                logger.error('error', exc_info=True)

        # create an entry for each generated file in the ProcessingHistoryStorage
        for pr in all_files:
            if isinstance(pr, SingleProcessingStepResult):
                phs.add_processed_fits_products(pr.name, pr.level, pr.type, pr.version,
                                                get_complete_file_name_and_path(pr.in_path),
                                                pr.out_path, pr.date)

        phs.close()

        all_files = list(set(all_files))

        # write out all generated fits file in a dedicated log file
        out_file = Path(args.log_dir) / f"dailypipeline_{date.today().strftime('%Y%m%d')}.out"
        with open(out_file, 'a+') as res_f:
            for f in all_files:
                res_f.write(f"{str(f)}\n")

    return all_files


def main():
    run_daily_pipeline(sys.argv[1:])


if __name__ == '__main__':
    main()

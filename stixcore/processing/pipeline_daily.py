import sys
import logging
import smtplib
import argparse
from pprint import pformat
from pathlib import Path
from datetime import date

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.processing.FLtoL3 import FLLevel3
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


def send_mail_report(timeranges):
    """Sends a report mail to configured receivers after each run.

    Parameters
    ----------
    files : dict
        all handled files grouped by the success/error state
    """
    if CONFIG.getboolean('Pipeline', 'report_mail_send', fallback=False):
        try:
            sender = CONFIG.get('Pipeline', 'error_mail_sender', fallback='')
            receivers = CONFIG.get('Pipeline', 'error_mail_receivers').split(",")
            host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
            port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
            smtp_server = smtplib.SMTP(host=host, port=port)
            su = "subject"
            error = "" if len(timeranges["errors"]) <= 0 else "ERROR-"
            message = f"""Subject: StixCore Daily Pipeline {error}Report {su}

ERRORS
******
{pformat(timeranges["errors"])}

PROCESSED
*********
{pformat(timeranges["processed"])}


StixCore

==========================

do not answer to this mail.
"""
            smtp_server.sendmail(sender, receivers, message)
        except Exception as e:
            logger.error(f"Error: unable to send report email: {e}")


def run_daily_pipeline(args):
    """CLI STIX publish to ESA processing step

    Parameters
    ----------
    args : list
        see -h for details

    Returns
    -------
    list
        list of published files
    """

    parser = argparse.ArgumentParser(description='STIX publish to ESA processing step',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # pathes
    parser.add_argument("-d", "--db_file",
                        help="Path to the history publishing database", type=str,
                        default=CONFIG.get('Publish', 'db_file',
                                           fallback=str(Path.home() / "published.sqlite")))

    parser.add_argument("-f", "--fits_dir",
                        help="output directory for the ",
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

    parser.add_argument("-o", "--log_dir",
                        help="output directory for daily logging ",
                        default=CONFIG.get('Publish', 'log_dir', fallback=str(Path.home())),
                        type=str, dest='log_dir')

    parser.add_argument("--continue_on_error",
                        help="the pipeline reports any error and continues processing",
                        default=not CONFIG.getboolean('Logging', 'stop_on_error', fallback=False),
                        action='store_false', dest='stop_on_error')

    parser.add_argument("--log_level",
                        help="the level of logging",
                        default=None, type=str,
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
                        dest='log_level')

    args = parser.parse_args(args)

    logging.basicConfig(format='%(asctime)s %(message)s', force=True,
                        filename=(Path(args.log_dir) /
                                  f"dailypipeline_{date.today().strftime('%Y%m%d')}.log"),
                        filemode="a+")

    if args.log_level:
        logging.getLogger().setLevel(logging.getLevelName(args.log_level))

    # pathes
    CONFIG.set('Paths', 'fits_archive', args.fits_dir)
    CONFIG.set('Paths', 'spice_kernels', args.spice_dir)
    CONFIG.set('Paths', 'soop_files', args.soop_dir)

    # logging
    CONFIG.set('Logging', 'stop_on_error', str(args.stop_on_error))

    if args.spice_file:
        spicemeta = Path(args.spice_file)
    else:
        _spm = SpiceKernelManager(Path(CONFIG.get('Paths', 'spice_kernels')))
        spicemeta = _spm.get_latest_mk_and_pred()

    Spice.instance = Spice(spicemeta)

    SOOPManager.instance = SOOPManager(Path(CONFIG.get('Paths', 'soop_files')))

    fitsdir = Path(CONFIG.get('Paths', 'fits_archive'))

    db_file = Path(args.db_file)
    fits_dir = Path(args.fits_dir)
    if not fits_dir.exists():
        logger.error(f'path not found to input files: {fits_dir}')
        return

    logger.info(f'db_file: {db_file}')
    logger.info(f'fits_dir: {fits_dir}')
    logger.info(f"send Mail report: {CONFIG.getboolean('Pipeline', 'error_mail_send')}")
    logger.info(f"receivers: {CONFIG.get('Pipeline', 'error_mail_receivers')}")
    logger.info(f'log dir: {args.log_dir}')
    logger.info(f'log level: {args.log_level}')
    logger.info(f'Spice: {Spice.instance.meta_kernel_path}')

    logger.info("\nstart daily pipeline\n")

    processed_timeranges = {}

    l3_proc = FLLevel3(fitsdir, fitsdir, db_file)
    processed_timeranges = l3_proc.process_fits_files()

    send_mail_report(processed_timeranges)
    return processed_timeranges


def main():
    run_daily_pipeline(sys.argv[1:])


if __name__ == '__main__':
    main()

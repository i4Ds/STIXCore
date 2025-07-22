import io
import re
import sys
import shutil
import socket
import inspect
import logging
import smtplib
import warnings
import importlib
import threading
import subprocess
from pprint import pformat
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

import stixcore
from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.manager import IDBManager
from stixcore.io.RidLutManager import RidLutManager
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.L1toL2 import Level2
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products import Product
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import STX_LOGGER_DATE_FORMAT, STX_LOGGER_FORMAT, get_logger
from stixcore.util.singleton import Singleton
from stixcore.version_conf import get_conf_version

__all__ = ['process_tm', 'PipelineErrorReport', 'PipelineStatus']

logger = get_logger(__name__)
warnings.filterwarnings('ignore', module='astropy.io.fits.card')
warnings.filterwarnings('ignore', module='astropy.utils.metadata')
warnings.filterwarnings('ignore', module='watchdog.events')

TM_REGEX = re.compile(r'.*PktTmRaw.*.xml$')


class PipelineErrorReport(logging.StreamHandler):
    """Adds file and mail report Handler to a processing step."""
    def __init__(self, tm_file):
        """Create a PipelineErrorReport

        Parameters
        ----------
        tm_file : Path
            The TM file that is process in this step.
        """
        logging.StreamHandler.__init__(self)

        self.tm_file = tm_file

        self.log_dir = Path(CONFIG.get('Pipeline', 'log_dir'))
        self.log_file = self.log_dir / (tm_file.name + ".log")
        self.err_file = self.log_dir / (tm_file.name + ".log.err")
        self.res_file = self.log_dir / (tm_file.name + ".out")

        self.fh = logging.FileHandler(filename=self.log_file, mode="a+")
        self.fh.setFormatter(logging.Formatter(STX_LOGGER_FORMAT, datefmt=STX_LOGGER_DATE_FORMAT))
        self.fh.setLevel(logging.getLevelName(CONFIG.get('Pipeline', 'log_level')))

        self.setLevel(logging.ERROR)
        self.allright = True
        self.error = None
        logging.getLogger().addHandler(self)
        logging.getLogger().addHandler(self.fh)
        PipelineStatus.log_setup()

    def emit(self, record):
        """Called in case of a logging event."""
        self.allright = False
        self.error = record
        self.err_file.touch()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        logging.getLogger().removeHandler(self)
        self.fh.flush()
        logging.getLogger().removeHandler(self.fh)
        if not self.allright or self.err_file.exists():
            shutil.copyfile(self.log_file, self.err_file)
            if CONFIG.getboolean('Pipeline', 'error_mail_send', fallback=False):
                try:
                    sender = CONFIG.get('Pipeline', 'error_mail_sender', fallback='')
                    receivers = CONFIG.get('Pipeline', 'error_mail_receivers').split(",")
                    host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
                    port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
                    smtp_server = smtplib.SMTP(host=host, port=port)
                    message = f"""Subject: StixCore TMTC Processing Error

Error while processing {self.tm_file}

login to pub099.cs.technik.fhnw.ch and check:

{self.err_file}

StixCore

==========================

do not answer to this mail.
"""
                    smtp_server.sendmail(sender, receivers, message)
                except Exception as e:
                    logger.error(f"Error: unable to send error email: {e}")

    def log_result(self, gen_files):
        """Dumps out all derived FITS files into a text file.

        Parameters
        ----------
        gen_files : list of list
            all generated FITS files
        """
        with open(self.res_file, 'w') as res_f:
            for level in gen_files:
                for f in level:
                    res_f.write(f"{str(f)}\n")


def process_tm(path, **args):
    with PipelineErrorReport(path) as error_report:
        # update the rid LUT file from the API and read in again
        RidLutManager.instance.update_lut()

        # set the latest spice kernel files for each run
        if ((args['spm'].get_latest_mk()[0] not in Spice.instance.meta_kernel_path) or
           (args['spm'].get_latest_mk_pred()[0] not in Spice.instance.meta_kernel_path)):
            Spice.instance = Spice(args['spm'].get_latest_mk_and_pred())
            logger.info("new spice kernels detected and loaded")

        # update version of common config it might have changed
        if get_conf_version() != stixcore.__version_conf__:
            importlib.reload(stixcore.version_conf)
            importlib.reload(stixcore)
            logger.info(f"new common conf detected new version is: {stixcore.__version_conf__}")

        lb_files = process_tmtc_to_levelbinary([SOCPacketFile(path)])
        logger.info(f"generated LB files: \n{pformat(lb_files)}")

        l0_proc = Level0(CONFIG.get('Paths', 'tm_archive'), CONFIG.get('Paths', 'fits_archive'))
        l0_files = l0_proc.process_fits_files(files=lb_files)
        logger.info(f"generated L0 files: \n{pformat(l0_files)}")

        l1_proc = Level1(CONFIG.get('Paths', 'tm_archive'), CONFIG.get('Paths', 'fits_archive'))
        l1_files = l1_proc.process_fits_files(files=l0_files)
        logger.info(f"generated L1 files: \n{pformat(l1_files)}")

        l2_proc = Level2(CONFIG.get('Paths', 'tm_archive'), CONFIG.get('Paths', 'fits_archive'))
        l2_files = l2_proc.process_fits_files(files=l1_files)
        logger.info(f"generated L2 files: \n{pformat(l2_files)}")
        l2_files = []

        error_report.log_result([list(lb_files), l0_files, l1_files, l2_files])


class PipelineStatus(metaclass=Singleton):

    def __init__(self, tm_list):
        self.last_error = (None,  datetime.now())
        self.last_tm = (None,  datetime.now())
        self.current_tm = (None,  datetime.now())
        self.tm_list = tm_list

        self.status_server_thread = threading.Thread(target=self.status_server)
        self.status_server_thread.daemon = True
        self.status_server_thread.start()

    @staticmethod
    def get_config():
        s = io.StringIO()
        CONFIG.write(s)
        s.seek(0)
        clone_config = ConfigParser()
        clone_config.read_file(s)
        # curate the pw
        clone_config.set("SOOP", "password", "xxxx")
        s = io.StringIO()
        s.write("\nCONFIG\n\n")
        clone_config.write(s)
        s.seek(0)
        return s.read()

    @staticmethod
    def log_config(level=logging.INFO):
        logger.log(level, PipelineStatus.get_config())

    @staticmethod
    def get_singletons():
        s = io.StringIO()
        s.write("\nSINGLETONS\n\n")
        s.write(f"SOOPManager: {SOOPManager.instance.data_root}\n")
        s.write(f"RidLutManager: {RidLutManager.instance}\n")
        s.write(f"SPICE: {Spice.instance.meta_kernel_path}\n")
        s.write(f"IDBManager: {IDBManager.instance.data_root}\n"
                f"Versions:\n{IDBManager.instance.get_versions()}\n"
                f"Force version: {IDBManager.instance.force_version}\n"
                f"History:\n{IDBManager.instance.history}\n")
        s.seek(0)
        return s.read()

    @staticmethod
    def get_version():
        s = io.StringIO()
        s.write("\nPIPELINE VERSION\n\n")
        s.write(f"Version: {str(stixcore.__version__)}\n")
        s.write(f"Common instrument config version: {str(stixcore.__version_conf__)}\n")
        s.write("PROCESSING VERSIONS\n\n")
        for p in Product.registry:
            s.write(f"Prod: {p.__name__}\n    File: {inspect.getfile(p)}\n"
                    f"    Vers: {p.get_cls_processing_version()}\n")
        s.seek(0)
        return s.read()

    @staticmethod
    def log_singletons(level=logging.INFO):
        logger.log(level, PipelineStatus.get_singletons())

    @staticmethod
    def log_version(level=logging.INFO):
        logger.log(level, PipelineStatus.get_version())

    @staticmethod
    def log_setup(level=logging.INFO):
        PipelineStatus.log_version(level=level)
        PipelineStatus.log_config(level=level)
        PipelineStatus.log_singletons(level=level)

    @staticmethod
    def get_setup():
        return PipelineStatus.get_version() +\
               PipelineStatus.get_config() +\
               PipelineStatus.get_singletons()

    def status_next(self):
        if not self.tm_list:
            return "File observer not initialized"
        return f"open files: {len(self.tm_list)}"

    def status_last(self):
        return "\n".join([str(p) for p in self.last_tm])

    def status_current(self):
        return "\n".join([str(p) for p in self.current_tm])

    def status_error(self):
        return "\n".join([str(p) for p in self.last_error])

    def status_config(self):
        return PipelineStatus.get_setup()

    def get_status(self, cmd):
        function = [getattr(self, func) for func in dir(self)
                    if callable(getattr(self, func)) and func == f"status_{cmd}"]

        if len(function) == 1:
            return function[0]()
        else:
            return f"call cmd {cmd} not found"

    def status_server(self):
        try:
            sock = socket.socket()
            server_address = ("localhost", CONFIG.getint('Pipeline', 'status_server_port',
                              fallback=12345))
            sock.bind(server_address)
            sock.listen(1)
            logger.info(f"Pipeline Server started at {server_address[0]}:{server_address[1]}")
        except OSError as e:
            logger.error(e, stack_info=True)
            sys.exit()

        while True:
            # Wait for a connection
            logger.debug('waiting for a connection')
            connection, client_address = sock.accept()

            try:
                logger.debug(f'connection from {client_address}')

                client = connection.makefile("rb")
                cmd = self.get_status(client.readline().decode().rstrip())
                client.close()
                connection.sendall(cmd.encode())
                connection.sendall(b"\n")
                connection.sendall(b"")

            finally:
                # Clean up the connection
                logger.debug('closing connection')
                connection.close()


def search_unprocessed_tm_files(logging_dir, tm_dir, last_processed):

    unprocessed_tm_files = list()
    latest_log_file = logging_dir / last_processed
    tm_file = Path(tm_dir / str(latest_log_file.name)[0:-4])
    ftime = tm_file.stat().st_mtime

    for tmf in tm_dir.glob("*.xml"):
        log_out_file = logging_dir / (tmf.name + ".out")
        log_file = logging_dir / (tmf.name + ".log")
        logger.info(f"test: {tmf.name}")
        if TM_REGEX.match(tmf.name) and tmf.stat().st_mtime > ftime and (not log_out_file.exists()
                                                                         or not log_file.exists()):
            unprocessed_tm_files.append(tmf)
            logger.info(f"NOT FOUND: {log_out_file.name}")
        else:
            logger.info(f"found: {log_out_file.name}")
    return unprocessed_tm_files


def main():
    log_dir = Path(CONFIG.get('Pipeline', 'log_dir'))
    log_dir.mkdir(parents=True, exist_ok=True)

    tmpath = Path(CONFIG.get('Paths', 'tm_archive'))

    if CONFIG.getboolean('Pipeline', 'sync_tm_at_start', fallback=False):
        logger.info("start sync_tm_at_start")
        res = subprocess.run(f"rsync -av /data/stix/SOLSOC/from_edds/tm/incomming/*PktTmRaw*.xml {str(tmpath)}", shell=True)  # noqa
        logger.info(f"done sync_tm_at_start: {str(res)}")

    soop_path = Path(CONFIG.get('Paths', 'soop_files'))
    spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(spm.get_latest_mk_and_pred())

    RidLutManager.instance = RidLutManager(Path(CONFIG.get('Publish', 'rid_lut_file')), update=True)

    soop_manager = SOOPManager(soop_path)
    SOOPManager.instance = soop_manager

    tm_files = []

    logger.info("Searching for unprocessed tm files")
    last_processed = CONFIG.get('Pipeline', 'last_processed', fallback="")
    unprocessed_tm_files = search_unprocessed_tm_files(log_dir, tmpath, last_processed)
    if unprocessed_tm_files:
        fl = '\n    '.join([f.name for f in unprocessed_tm_files])
        logger.info(f"Found unprocessed tm files: \n    {fl}\nadding to queue.")
        tm_files.extend(unprocessed_tm_files)

    logger.info("start processing once")

    PipelineStatus.instance = PipelineStatus(tm_files)
    while tm_files:
        tm_file = tm_files.pop(0)
        logger.info(f"start processing tm file: {tm_file}")
        PipelineStatus.instance.current_tm = (tm_file, datetime.now())
        process_tm(tm_file, **{"spm":  spm})
        PipelineStatus.instance.last_tm = (tm_file,  datetime.now())
        PipelineStatus.instance.current_tm = (None,  datetime.now())
    logger.info("stop processing once")
    return 1


if __name__ == '__main__':
    main()
    logger.info("all done")

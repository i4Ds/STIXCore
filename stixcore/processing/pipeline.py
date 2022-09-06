import io
import re
import sys
import time
import shutil
import socket
import logging
import smtplib
import warnings
import threading
from enum import Enum
from queue import Queue
from pprint import pformat
from pathlib import Path
from datetime import datetime
from functools import partial

from polling2 import poll_decorator
from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from watchdog.observers import Observer

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.L1toL2 import Level2
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import STX_LOGGER_DATE_FORMAT, STX_LOGGER_FORMAT, get_logger

__all__ = ['GFTSFileHandler', 'process_tm', 'PipelineErrorReport', 'log_config', 'log_setup',
           'log_singletons']

logger = get_logger(__name__)
warnings.filterwarnings('ignore', module='astropy.io.fits.card')
warnings.filterwarnings('ignore', module='astropy.utils.metadata')

TM_REGEX = re.compile(r'.*PktTmRaw.*.xml$')

TM_HANDLER = None


class GFTSFileHandler(FileSystemEventHandler):
    """
    Handler to detect and process new files send from GFTS

    As rsync is used to transfer the files to process need to take into account how rsync works.
    Rsync works by first creating a temporary file of the same with name as the file being
    transferred `myfile.xml` with an random extra extension `myfile.xml.NmRJ4x` it then
    transfers the data to the temporary file and once the transfer is complete it them move/renames
    the file back to the original name `myfile.xml`. Can detect file move event that match the TM
    filename pattern.
    """
    def __init__(self, func, regex, name="name", **args):
        """

        Parameters
        ----------
        func : `callable`
            The method to call when new TM is received with the path to the file as the argument
        regex : `Pattern`
            a filter filename pattern that have to match in order to invoke the 'func'
        """
        if not callable(func):
            raise TypeError('func must be a callable')
        self.func = func

        # TODO should be Pattern but not compatible with py 3.6
        if not isinstance(regex, type(re.compile('.'))):
            raise TypeError('regex must be a regex Pattern')
        self.regex = regex
        self.args = args
        self.queue = Queue(maxsize=0)
        self.name = name
        self.lp = threading.Thread(target=self.process)
        self.lp.start()

    @poll_decorator(step=1, poll_forever=True)
    def process(self):
        """Worker function to process the queue of detected files."""
        logger.info(f"GFTSFileHandler:{self.name} start working")
        path = self.queue.get()  # this will wait until the next
        logger.info(f"GFTSFileHandler:{self.name} found: {path}")
        try:
            self.func(path, **self.args)
        except Exception as e:
            logger.error(e)
            if CONFIG.getboolean('Logging', 'stop_on_error', fallback=False):
                raise e
        logger.info(f"GFTSFileHandler:{self.name} end working")

    def on_moved(self, event):
        """Callback if a file was moved

        Parameters
        ----------
        event : Event
            The event object with access to the file
        """
        if self.regex.match(event.dest_path):
            self.queue.put(Path(event.dest_path))


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

        PipelineErrorReport.CURRENT_TM = (tm_file, datetime.now())

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
        log_setup()

    LAST_ERROR = (None,  datetime.now())
    LAST_TM = (None,  datetime.now())
    CURRENT_TM = (None,  datetime.now())

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
        PipelineErrorReport.LAST_TM = (self.tm_file,  datetime.now())
        PipelineErrorReport.CURRENT_TM = (None,  datetime.now())
        if not self.allright:
            shutil.copyfile(self.log_file, self.err_file)
            PipelineErrorReport.LAST_ERROR = (self.tm_file,  datetime.now(),
                                              self.error, self.err_file)
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
        # set the latest spice file for each run
        if args['spm'].get_latest_mk() != Spice.instance.meta_kernel_path:
            Spice.instance = Spice(args['spm'].get_latest_mk())
            logger.info("new spice kernel detected and loaded")

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

        error_report.log_result([list(lb_files), l0_files, l1_files, l2_files])


def get_config():
    s = io.StringIO()
    s.write("\nCONFIG\n\n")
    CONFIG.write(s)

    s.seek(0)
    return s.read()


def log_config(level=logging.INFO):
    logger.log(level, get_config())


def get_singletons():
    s = io.StringIO()
    s.write("\nSINGLETONS\n\n")
    s.write(f"SOOPManager: {SOOPManager.instance.data_root}\n")
    s.write(f"SPICE: {Spice.instance.meta_kernel_path}\n")
    s.write(f"IDBManager: {IDBManager.instance.data_root}\n"
            f"Versions:\n{IDBManager.instance.get_versions()}\n"
            f"Force version: {IDBManager.instance.force_version}\n"
            f"History:\n{IDBManager.instance.history}\n")
    s.seek(0)
    return s.read()


def log_singletons(level=logging.INFO):
    logger.log(level, get_singletons)


def log_setup(level=logging.INFO):
    log_config(level=level)
    log_singletons(level=level)


def get_setup():
    return get_config() + get_singletons()


def status_next():
    if not TM_HANDLER:
        return "File observer not initialized"
    return f"open files: {TM_HANDLER.queue.qsize()}"


def status_last():
    return "\n".join([str(p) for p in PipelineErrorReport.LAST_TM])


def status_current():
    return "\n".join([str(p) for p in PipelineErrorReport.CURRENT_TM])


def status_error():
    return "\n".join([str(p) for p in PipelineErrorReport.LAST_ERROR])


def status_config():
    return get_setup()


class StatusCMD(Enum):
    """Enum Type for Processing steps to make them sortable"""
    NEXT = partial(status_next)
    LAST = partial(status_last)
    ERROR = partial(status_error)
    CONFIG = partial(status_config)
    CURRENT = partial(status_current)

    def __str__(self):
        return self.name

    @staticmethod
    def from_str(label):
        label = label.upper()
        for e in StatusCMD:
            if e.name == label:
                return e
        return StatusCMD.LAST


def status_server():
    try:
        sock = socket.socket()
        server_address = ("localhost", CONFIG.getint('Pipeline', 'status_server_port',
                          fallback=12345))
        sock.bind(server_address)
        sock.listen(1)
        logger.info(f"Pipeline Status Server started at {server_address[0]}:{server_address[1]}")
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
            cmd = StatusCMD.from_str(client.readline().decode().rstrip())
            client.close()
            connection.sendall(cmd.value().encode())
            connection.sendall(b"\n")
            connection.sendall(b"")

        finally:
            # Clean up the connection
            logger.debug('closing connection')
            connection.close()


def main():
    log_dir = Path(CONFIG.get('Pipeline', 'log_dir'))
    log_dir.mkdir(parents=True, exist_ok=True)

    status_server_thread = threading.Thread(target=status_server)
    status_server_thread.daemon = True
    status_server_thread.start()

    time.perf_counter()
    observer = Observer()
    tmpath = Path(CONFIG.get('Paths', 'tm_archive'))
    soop_path = Path(CONFIG.get('Paths', 'soop_files'))
    spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(spm.get_latest_mk())

    logging_handler = LoggingEventHandler(logger=logger)
    TM_HANDLER = GFTSFileHandler(process_tm, TM_REGEX, name="tm_xml", spm=spm)

    soop_manager = SOOPManager(soop_path)
    soop_handler = GFTSFileHandler(soop_manager.add_soop_file_to_index,
                                   SOOPManager.SOOP_FILE_REGEX, name="soop")
    SOOPManager.instance = soop_manager

    observer.schedule(soop_handler, soop_manager.data_root,  recursive=False)
    observer.schedule(logging_handler, tmpath,  recursive=True)
    observer.schedule(TM_HANDLER, tmpath, recursive=True)

    observer.start()
    try:
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()

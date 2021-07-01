import re
import time
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from watchdog.observers import Observer

from stixcore.config.config import CONFIG
from stixcore.io.soc.manager import SOCPacketFile
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.util.logging import get_logger

logger = get_logger(__name__, level=logging.DEBUG)

TM_REGEX = re.compile(r'.*PktTmRaw.*.xml$')


class TMTCFileHandler(FileSystemEventHandler):
    """
    Handler to detect and process new TM files

    As rsync is used to transfer the files to process need to take into account how rsync works.
    Rsync works by first creating a temporary file of the same with name as the file being
    transferred `myfile.xml` with an random extra extension `myfile.xml.NmRJ4x` it then
    transfers the data to the temporary file and once the transfer is complete it them move/renames
    the file back to the original name `myfile.xml`. Can detect file move event that match the TM
    filename pattern.
    """
    def __init__(self, func):
        """

        Parameters
        ----------
        func : `callable`
            The method to call when new TM is received
        """
        if not callable(func):
            raise TypeError('func must be a callable')
        self.func = func

    def on_moved(self, event):
        if TM_REGEX.match(event.dest_path):
            self.func(Path(event.dest_path))


def process(path):
    lb_files = process_tmtc_to_levelbinary([SOCPacketFile(path)])
    l0_proc = Level0(CONFIG.get('Paths', 'tm_archive'), CONFIG.get('Paths', 'fits_archive'))
    l0_files = l0_proc.process_fits_files(files=lb_files)
    l1_proc = Level1(CONFIG.get('Paths', 'tm_archive'), CONFIG.get('Paths', 'fits_archive'))
    l1_files = l1_proc.process_fits_files(files=l0_files)
    logger.debug(l1_files)


if __name__ == '__main__':
    tstart = time.perf_counter()
    observer = Observer()
    path = Path('/home/shane/tm')
    logging_handler = LoggingEventHandler(logger=logger)
    tm_handler = TMTCFileHandler(process)
    observer.schedule(logging_handler, path,  recursive=True)
    observer.schedule(tm_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

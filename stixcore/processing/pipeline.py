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
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

__all__ = ['GFTSFileHandler']

logger = get_logger(__name__, level=logging.DEBUG)

TM_REGEX = re.compile(r'.*PktTmRaw.*.xml$')


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
    def __init__(self, func, regex):
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

    def on_moved(self, event):
        if self.regex.match(event.dest_path):
            self.func(Path(event.dest_path))


def process_tm(path):
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
    tm_handler = GFTSFileHandler(process_tm, TM_REGEX)

    # TODO should be an deticated path from the config
    soop_manager = SOOPManager(path)
    soop_handler = GFTSFileHandler(soop_manager.add_soop_file_to_index, SOOPManager.SOOP_FILE_REGEX)

    observer.schedule(soop_handler, soop_manager.data_root,  recursive=False)
    observer.schedule(logging_handler, path,  recursive=True)
    observer.schedule(tm_handler, path, recursive=True)

    observer.start()
    try:
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

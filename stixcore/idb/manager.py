import os
import json
import shutil
import sqlite3
import zipfile
import threading
import urllib.request
from pathlib import Path
from datetime import datetime

from dateutil import parser as dtparser

from stixcore.idb.idb import IDB
from stixcore.util.logging import get_logger

thread_lock = threading.Lock()

__all__ = ['IDBManager']

IDB_FILENAME = "idb.sqlite"
IDB_VERSION_PREFIX = "v"
IDB_VERSION_DELIM = "."
IDB_VERSION_HISTORY_FILE = "idbVersionHistory.json"

logger = get_logger(__name__)


class IDBManager:
    """Manages IDB (definition of TM/TC packet structures) Versions and provides a IDB reader."""

    def __init__(self, data_root):
        """Create the manager for a given data path root.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`
            Path to the directory with all IDB versions
        """
        self.data_root = data_root
        self.idb_cache = dict()

    @property
    def data_root(self):
        """Get the data path root directory.

        Returns
        -------
        `pathlib.Path`
            path of the root directory
        """
        return self._data_root

    @data_root.setter
    def data_root(self, value):
        """Set the data path root.

        Parameters
        ----------
        data_root : `str` or `pathlib.Path`
            Path to the directory with all IDB versions
        """
        path = Path(value)
        if not path.exists():
            raise ValueError(f'path not found: {value}')

        self._data_root = path
        try:
            with open(self._data_root / IDB_VERSION_HISTORY_FILE) as f:
                self.history = json.load(f)
        except EnvironmentError:
            raise ValueError(f'No IDB version history found at: '
                             f'{self._data_root / IDB_VERSION_HISTORY_FILE}')

    def find_version(self, utc=None):
        """Find IDB version operational at a given time.

        Parameters
        ----------
        utc : `datetime`, optional
            the time point of the IDB operation, by default None

        Returns
        -------
        `str`
            a version label
        """
        if not utc:
            try:
                return self.history[0]['version']
            except IndexError as e:
                logger.error(str(e))
            return ''
        for item in self.history:
            if dtparser.parse(item['validityPeriod'][0]) < utc \
                    <= dtparser.parse(item['validityPeriod'][1]):
                return item['version']

        logger.error(f"No IDB version found for Time: {utc}")
        return ''

    def download_version(self, version_label, force=False,
                         url="http://pub099.cs.technik.fhnw.ch/data/idb/"):
        """Download and installs an IDB version of a public available URL.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition
        force : `bool`, optional
            set to True to override the local version, by default False
        url : `str`, optional
            public available IDB versions folder, by default
            "https://nicky.thecrag.com/public/stix/"

        Returns
        -------
        `bool`
            was the download and installation successfully

        Raises
        ------
        ValueError
        """
        if self.has_version(version_label) and force is False:
            raise ValueError(f'IDB version {version_label} already available locally. '
                             f'Use force=True if you would like to override')

        if force:
            try:
                Path(self._get_filename_for_version(version_label)).unlink()
            except Exception as e:
                logger.warn(e)

        vlabel = (IDB_VERSION_PREFIX + IDBManager.convert_version_label(version_label))
        vdir = self.data_root / vlabel
        try:
            if not vdir.exists():
                os.mkdir(vdir)
            urllib.request.urlretrieve(url + vlabel + ".zip", vdir / "idb.zip")

            with zipfile.ZipFile(vdir / "idb.zip", 'r') as zip_ref:
                zip_ref.extractall(vdir / "raw")

            IDBManager.convert_mib_2_sqlite(
                in_folder=vdir / "raw" / ("STIX-IDB-" +
                                          IDBManager.convert_version_label(version_label)) / "idb",
                out_file=self._get_filename_for_version(version_label),
                version_label=IDBManager.convert_version_label(version_label))

        except Exception as e:
            logger.error(e)
            return False
        finally:
            shutil.rmtree(str(vdir / "raw"))
            (vdir / "idb.zip").unlink()

        return self.has_version(version_label)

    @staticmethod
    def convert_mib_2_sqlite(*, in_folder, out_file, version_label):
        """Convert a raw IDB version (set of .dat files) into a SqlLite DB.

        Parameters
        ----------
        in_folder : `Path`
            path to the folder with the IDB raw data files
        out_file : `Path`
            path and filename of the SqlLite DB file to generate
        version_label : `str`
            the version label to be included into the DB
        """
        try:
            file_list = in_folder.glob('*.dat')
            with sqlite3.connect(str(out_file)) as conn:
                cur = conn.cursor()

                thread_lock.acquire(True)

                create_table = open(Path(os.path.abspath(__file__)).parent
                                    / 'createIdb.sql', 'r').read()
                logger.info('creating database')
                cur.executescript(create_table)

                for fname in file_list:
                    name = fname.name

                    with open(fname, 'r') as datafile:
                        try:
                            cursor = cur.execute(f"select * from {name} limit 1;")
                        except sqlite3.Error:
                            logger.info(f"Skip import for {name}: is not needed")
                            continue
                        logger.info(f"import data for {name}")
                        names = list(map(lambda x: x[0], cursor.description))
                        num = len(names)
                        for line in datafile:
                            cols = [e.strip() for e in line.split("\t")]

                            # fill tailing NULL values as they might not part of the dat file
                            if num > len(cols):
                                cols.extend(['NULL'] * (num - len(cols)))

                            qmark = ", ".join(["?"] * len(cols))

                            sql = f"insert into {name} values ({qmark})"
                            if num != len(cols):
                                logger.warn(f"Found inconsistent data in idb files: "
                                            f"{names} : {cols}")
                            else:
                                cur.execute(sql, cols)

                update_db = open(Path(os.path.abspath(__file__)).parent
                                 / 'updateIdb.sql', 'r').read()
                logger.info('updating database')
                cur.executescript(update_db)
                cur.execute("insert into IDB (creation_datetime, version) "
                            "values (current_timestamp, ?);", (version_label,))

        finally:
            conn.commit()
            conn.close()
            thread_lock.release()

    def get_versions(self):
        r"""Get all available versions in the root directory. Does not check for version conflicts.

        Returns
        -------
        `list`
            List of available versions e.g.
            `[{'label': '2.26.34', 'path': 'a\path\v2.26.34', 'version': ['2', '26', '34']}`
        """
        versions = list()

        for root, dirs, files in os.walk(self._data_root):
            for file in files:
                if file == IDB_FILENAME:
                    label = root.split(os.sep)[-1].replace(IDB_VERSION_PREFIX, "")
                    versions.append({'label': label, 'path': root,
                                     'version': label.split(IDB_VERSION_DELIM)})

        return versions

    @staticmethod
    def convert_version_label(version_label):
        """Convert a label or version tuple into a version label.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition

        Returns
        -------
        `str`
            a label like '1.2.3'
        """
        if(isinstance(version_label, str)):
            return version_label
        if(isinstance(version_label, (list, tuple))):
            return IDB_VERSION_DELIM.join(map(str, version_label))

    def _get_filename_for_version(self, version_label):
        """Return filename and path for label or version tuple.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition (major, minor, patch) or "major.minor.patch"

        Returns
        -------
        `str`
            a filename like 'data/v1.2.3/idb.sqlite'
        """
        folder = IDB_VERSION_PREFIX + IDBManager.convert_version_label(version_label)

        return os.path.join(self._data_root, folder, IDB_FILENAME)

    def has_version(self, version_label):
        """Test if the IDB version is available.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition

        Returns
        -------
        `True|False`
            does the IDB exists and matches the version
        """
        if IDBManager.convert_version_label(version_label) in self.idb_cache:
            return True

        file = Path(self._get_filename_for_version(version_label))
        if not file.exists():
            logger.debug("IDB version file not found")
            return False

        idb = IDB(file)
        ver = idb.version
        idb.close()
        if ver != IDBManager.convert_version_label(version_label):
            logger.debug("IDB version missmatch")
        return ver == IDBManager.convert_version_label(version_label)

    def get_idb(self, version_label='', utc=None):
        """Get the IDB for the specified version (or the latest available).

        Parameters
        ----------
        version_label : `str` | `(int, int, int)`
            a version definition (major, minor, patch) or "major.minor.patch"
        utc : `datetime`, optional
            a date for autodetect the IDB version for operation period

        Returns
        -------
        `~stixcore.idb.idb.IDB`
            reference to a IDB reader
        """
        if isinstance(utc, datetime):
            utc_version = self.find_version(utc)
            if self.has_version(utc_version):
                version_label = utc_version
            else:
                logger.warning("No valid IDB version found for time {utc}."
                               "Falling back to version {version_label}")

        if self.has_version(version_label):
            if version_label not in self.idb_cache:
                self.idb_cache[version_label] = \
                    IDB(Path(self._get_filename_for_version(version_label)))
            return self.idb_cache[version_label]
        raise ValueError(f'Version "{version_label}" not found in: '
                         f'"{self._get_filename_for_version(version_label)}"')

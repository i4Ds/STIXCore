import os
import re
import sys
import json
import shutil
import sqlite3
import zipfile
import urllib.request
from pathlib import Path

from intervaltree import IntervalTree

from stixcore.data.test import test_data
from stixcore.idb.idb import IDB
from stixcore.time import SCETime
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ["IDBManager"]

IDB_FILENAME = "idb.sqlite"
IDB_VERSION_PREFIX = "v"
IDB_VERSION_DELIM = "."
IDB_VERSION_HISTORY_FILE = Path(__file__).parent.parent / "data" / "idb" / "idbVersionHistory.json"

IDB_FORCE_VERSION_KEY = "__FORCE_VERSION__"

logger = get_logger(__name__)


class IDBManager(metaclass=Singleton):
    """Manages IDB (definition of TM/TC packet structures) Versions and provides a IDB reader."""

    def __init__(self, data_root, force_version=None):
        """Create the manager for a given data path root.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`
            Path to the directory with all IDB versions
        force_version : `str` | `pathlib.Path`
            `pathlib.Path`: Path to a directory with a specific IDB version
            `str` : Version Label to a IDB version within the data_root directory
        """
        self.idb_cache = dict()
        self._force_version = None
        self.data_root = data_root
        self.force_version = force_version

    @property
    def force_version(self):
        """Get the forced IDB version.

        Returns
        -------
        `pathlib.Path`
            path to the IDB directory
        """
        return self._force_version

    @force_version.setter
    def force_version(self, value):
        """Set a forced IDB version to be used for all processing.

        Parameters
        ----------
        force_version : `str` or `pathlib.Path`
            `pathlib.Path`: Path to a directory with a specific IDB version
            `str` : Version Label to a IDB version within the data_root directory
        """
        idb = None
        if isinstance(value, str) and self.has_version(value):
            idb = self.get_idb(value)

        if isinstance(value, Path) and value.exists():
            idb = IDB(value)

        if idb:
            if not idb.is_connected():
                idb._connect_database()
            self.idb_cache[IDB_FORCE_VERSION_KEY] = idb
            self._force_version = idb.get_idb_filename()
        else:
            if IDB_FORCE_VERSION_KEY in self.idb_cache:
                del self.idb_cache[IDB_FORCE_VERSION_KEY]
            self._force_version = None

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
            logger.info(f"path not found: {value} creating dir")
            path.mkdir(parents=True, exist_ok=True)

        self._data_root = path
        try:
            with open(IDB_VERSION_HISTORY_FILE) as f:
                self.history = IntervalTree()

                for item in json.load(f):
                    item["validityPeriodOBT"][0] = SCETime(
                        coarse=item["validityPeriodOBT"][0]["coarse"], fine=item["validityPeriodOBT"][0]["fine"]
                    )
                    item["validityPeriodOBT"][1] = SCETime(
                        coarse=item["validityPeriodOBT"][1]["coarse"], fine=item["validityPeriodOBT"][1]["fine"]
                    )
                    self.history.addi(
                        item["validityPeriodOBT"][0].as_float().value,
                        item["validityPeriodOBT"][1].as_float().value,
                        item["version"],
                    )
                    try:
                        if not self.has_version(item["version"]):
                            available = self.download_version(item["version"], force=False)
                            if not available:
                                raise ValueError(
                                    f"was not able to download IDB version {item['version']} into {self._data_root}"
                                )

                    except OSError:
                        pass

        except OSError:
            raise ValueError(f"No IDB version history found at: {IDB_VERSION_HISTORY_FILE}")

    def find_version(self, obt=None):
        """Find IDB version operational at a given time.

        Parameters
        ----------
        obt : `datetime`, optional
            the time point of the IDB operation, by default None

        Returns
        -------
        `str`
            a version label
        """
        try:
            if not obt:
                return next(iter(self.history.at(self.history.begin()))).data
            return next(iter(self.history.at(obt.as_float().value))).data
        except IndexError as e:
            logger.error(f"No IDB version found for Time: {obt}\n{e}")
        return ""

    def compile_version(self, version_label, force=False, url="https://pub099.cs.technik.fhnw.ch/data/idb/"):
        """Download compiles and installs an IDB version of a public available URL.
           Some IDB parameters will be injected to support the raw tw engineering framework.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition
        force : `bool`, optional
            set to True to override the local version, by default False
        url : `str`, optional
            public available IDB versions folder, by default
            "https://pub099.cs.technik.fhnw.ch/data/idb/"

        Returns
        -------
        `bool`
            was the download and installation successfully

        Raises
        ------
        ValueError
        """
        if force is False and self.has_version(version_label):
            raise ValueError(
                f"IDB version {version_label} already available locally. Use force=True if you would like to override"
            )

        if force:
            try:
                Path(self._get_filename_for_version(version_label)).unlink()
            except Exception as e:
                logger.warning(e)

        vlabel = IDB_VERSION_PREFIX + IDBManager.convert_version_label(version_label)
        vdir = self.data_root / vlabel

        try:
            vdir.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(url + vlabel + ".raw.zip", vdir / "idb.zip")

            with zipfile.ZipFile(vdir / "idb.zip", "r") as zip_ref:
                zip_ref.extractall(vdir / "raw")

            IDBManager.convert_mib_2_sqlite(
                in_folder=vdir
                / "raw"
                / ("v" + IDBManager.convert_version_label(version_label))
                / ("STIX-IDB-" + IDBManager.convert_version_label(version_label))
                / "idb",
                out_file=self._get_filename_for_version(version_label),
                version_label=IDBManager.convert_version_label(version_label),
            )

        except Exception as e:
            logger.error(e)
            return False
        finally:
            shutil.rmtree(str(vdir / "raw"))
            (vdir / "idb.zip").unlink()

        return self.has_version(version_label)

    def download_version(self, version_label, force=False, url="https://pub099.cs.technik.fhnw.ch/data/idb/"):
        """Download and installs an IDB version of a public available URL.

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition
        force : `bool`, optional
            set to True to override the local version, by default False
        url : `str`, optional
            public available IDB versions folder, by default
            "https://pub099.cs.technik.fhnw.ch/data/idb/"

        Returns
        -------
        `bool`
            was the download and installation successfully

        Raises
        ------
        ValueError
        """
        if force is False and self.has_version(version_label):
            logger.warning(
                f"IDB version {version_label} already available locally. Use force=True if you would like to override"
            )
            return True

        if force:
            try:
                Path(self._get_filename_for_version(version_label)).unlink()
            except Exception as e:
                logger.warning(e)

        vlabel = IDB_VERSION_PREFIX + IDBManager.convert_version_label(version_label)
        vdir = self.data_root / vlabel
        try:
            vdir.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(url + vlabel + ".zip", vdir / "idb.zip")

            with zipfile.ZipFile(vdir / "idb.zip", "r") as zip_ref:
                zip_ref.extractall(vdir)

            shutil.move(vdir / vlabel / "idb.sqlite", vdir / "idb.sqlite")

            (vdir / "idb.zip").unlink()
            shutil.rmtree(str(vdir / vlabel))
            logger.info(f"Downloaded IDB version: {vlabel} from {url}")

        except Exception as e:
            logger.error(e)
            return False

        return self.has_version(version_label)

    @staticmethod
    def convert_mib_2_sqlite(*, in_folder, out_file, version_label):
        """Convert a raw IDB version (set of .dat files) into a sqlite DB.

        Parameters
        ----------
        in_folder : `Path`
            path to the folder with the IDB raw data files
        out_file : `Path`
            path and filename of the sqlite DB file to generate
        version_label : `str`
            the version label to be included into the DB
        """
        try:
            file_list = in_folder.glob("*.dat")
            with sqlite3.connect(str(out_file)) as conn:
                cur = conn.cursor()

                # thread_lock.acquire(True)

                create_table = open(Path(os.path.abspath(__file__)).parent / "createIdb.sql").read()
                logger.info("creating database")
                cur.executescript(create_table)

                for fname in file_list:
                    name = fname.stem

                    with open(fname) as datafile:
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
                            cols = [None if c == "" else c for c in cols]
                            # fill tailing NULL values as they might not part of the dat file
                            if num > len(cols):
                                cols.extend(["NULL"] * (num - len(cols)))

                            qmark = ", ".join(["?"] * len(cols))

                            sql = f"insert into {name} values ({qmark})"
                            if num != len(cols):
                                logger.warning(f"Found inconsistent data in idb files: {names} : {cols}")
                            else:
                                cur.execute(sql, cols)

                update_db = open(Path(os.path.abspath(__file__)).parent / "updateIdb.sql").read()
                logger.info("updating database")
                cur.executescript(update_db)
                cur.execute(
                    "insert into IDB (creation_datetime, version) values (current_timestamp, ?);", (version_label,)
                )

                # inject custom calibrations

                nextID = 0
                for (calibN,) in cur.execute(
                    "select distinct PCF_CURTX from PCF " + "where PCF_CURTX not NULL"
                ).fetchall():
                    nr = int(re.match(r"([a-z]+)([0-9]+)([a-z]+)", calibN, re.IGNORECASE).group(2))
                    if nr > nextID:
                        nextID = nr
                nextID += 1

                # inject polynomial calibrations

                duration = ("duration", 0, 0.1, 0, 0, 0)
                duration_p1 = ("duration + 0.1", 0.1, 0.1, 0, 0, 0)
                duration_ms = ("duration in ms", 0, 1, 0, 0, 0)
                binary_seconds = ("binary seconds", 0, 1.0 / 65535, 0, 0, 0)
                cpu_load = ("cpu load", 0, 4, 0, 0, 0)

                # TODO take IDB version into account
                for nix, config, unit in [
                    ("NIX00269", duration, "s"),
                    ("NIX00441", duration, "s"),
                    ("NIX00122", duration, "s"),
                    ("NIX00405", duration, "s"),
                    ("NIX00124", duration_ms, "ms"),
                    ("NIX00404", duration_p1, "s"),
                    ("NIX00123", binary_seconds, "s"),
                    ("NIXD0002", cpu_load, "%"),
                ]:
                    (count,) = cur.execute(
                        "select count(*) from PCF where PCF_NAME = ? " + "AND PCF_CURTX not NULL", (nix,)
                    ).fetchone()
                    if count == 0:
                        pname, nextID = IDB.generate_calibration_name("CIX", nextID)
                        cur.execute(
                            """update PCF set
                                        PCF_CURTX = ?,
                                        PCF_CATEG = 'N',
                                        PCF_UNIT = ?
                                    where PCF_NAME = ?""",
                            (pname, unit, nix),
                        )

                        cur.execute(
                            """insert into MCF (MCF_IDENT, MCF_DESCR, MCF_POL1,
                                        MCF_POL2, MCF_POL3, MCF_POL4, MCF_POL5, SDB_IMPORTED)
                                    values
                                        (?,?,?,?,?,?,?, 0)""",
                            ((pname,) + config),
                        )
                        logger.info(f"calibration injection for {nix}: {((pname,) + config)}")

                    else:
                        logger.info(f"Skip calibration injection for {nix}: already present")

        finally:
            conn.commit()
            conn.close()
            # thread_lock.release()

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
                    versions.append({"label": label, "path": root, "version": label.split(IDB_VERSION_DELIM)})

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
        if isinstance(version_label, str):
            return version_label
        if isinstance(version_label, (list, tuple)):
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
            logger.debug("IDB version mismatch")
        return ver == IDBManager.convert_version_label(version_label)

    def get_idb(self, version_label="2.26.34", obt=None):
        """Get the IDB for the specified version (or the latest available).

        Parameters
        ----------
        version_label : `str` | `(int, int, int)`
            a version definition (major, minor, patch) or "major.minor.patch"
            default to '2.26.34'
        obt : `datetime`, optional
            a date for autodetect the IDB version for operation period

        Returns
        -------
        `~stixcore.idb.idb.IDB`
            reference to a IDB reader
        """

        if self._force_version:
            logger.debug(f"Use Forced IDB version: {self._force_version}")
            return self.idb_cache[IDB_FORCE_VERSION_KEY]

        if isinstance(obt, SCETime):
            obt_version = self.find_version(obt=obt)
            if self.has_version(obt_version):
                version_label = obt_version
            else:
                logger.warning(f"No valid IDB version found for time {obt}Falling back to version {version_label}")

        if self.has_version(version_label):
            if version_label not in self.idb_cache:
                self.idb_cache[version_label] = IDB(Path(self._get_filename_for_version(version_label)))

            idb = self.idb_cache[version_label]
            if not idb.is_connected():
                idb._connect_database()
            return idb
        raise ValueError(f'Version "{version_label}" not found in: "{self._get_filename_for_version(version_label)}"')


if "pytest" in sys.modules:
    IDBManager.instance = IDBManager(test_data.idb.DIR)
else:
    IDBManager.instance = IDBManager(Path(__file__).parent.parent / "data" / "idb")

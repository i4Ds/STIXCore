from datetime import datetime
from pathlib import Path
from stixcore.idb.idb import IDB
import os

__all__ = ['IdbManager']

IDB_FILENAME = "idb.sqlite"
IDB_VERSION_PREFIX = "v"
IDB_VERSION_DELIM = "."

class IdbManager:
    """
    Manages IDB (definition of TM/TC packet structures) Versions
    and provides a IDB reader
    """
    def __init__(self, data_root):
        """
        Creates the manager for a given data path root

        Parameters
        ----------
        data_path : `str` or `pathlib.Path`
            Path to the directory with all IDB versions

        """
        self.data_root = data_root

    @property
    def data_root(self):
        """
        gets the data path root directory

        Returns
        -------
        `pathlib.Path`
            path of the root directory
        """
        return self._data_root

    @data_root.setter
    def data_root(self, value):
        """
        sets data path root

        Parameters
        ----------
        data_path : `str` or `pathlib.Path`
            Path to the directory with all IDB versions

        """
        path = Path(value)
        if not os.path.exists(path):
            raise ValueError(f'path not found: {value}')
        self._data_root = path

    def get_versions(self):
        """
        gets all awailable versions of the root directory.
        Does not check for version conflicts.

        Returns
        -------
        `list` of availabe Versions
            {'label': '2.26.34', 'path': 'stixcore\\idb\\tests\\data\\v2.26.34', 'version': ['2', '26', '34']}
        """
        versions = list()

        for root, dirs, files in os.walk(self._data_root):
            for file in files:
                if file == IDB_FILENAME :
                    label = root.split(os.sep)[-1].replace(IDB_VERSION_PREFIX,"")
                    versions.append({'label' : label, 'path': root, 'version' : label.split(IDB_VERSION_DELIM)});

        return versions

    def _get_label(self, version_label):
        """
        coverts a label or version tupel into a version label

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition

        Returns
        -------
        `str` a label like '1.2.3'

        """
        if(isinstance(version_label, str)):
            return version_label
        if(isinstance(version_label, (list, tuple))):
            return IDB_VERSION_DELIM.join(map(str, version_label))

    def _get_filename_for_version(self, version_label):
        """
        coverts a label or version tupel into a file name with path

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition (major, minor, patch) or "major.minor.patch"

        Returns
        -------
        `str` a filename like 'data/v1.2.3/idb.sqlite'

        """
        folder = IDB_VERSION_PREFIX + self._get_label(version_label)

        return os.path.join(self._data_root, folder, IDB_FILENAME)

    def has_version(self, version_label):
        """
        test if the IDB version is available

        Parameters
        ----------
        version_label : `str` or (`int`, `int`, `int`)
            a version definition

        Returns
        -------
        `True|False` does the IDB exists and matches the version

        """

        file = Path(self._get_filename_for_version(version_label))
        if not os.path.exists(file):
            print(file)
            return False

        idb = IDB(file)
        ver = idb.get_idb_version()
        idb.close()
        print(ver)
        return ver == self._get_label(version_label)

    def get_idb(self, version_label):
        """
        gets an IDB reference of the specified version

        Parameters
        ----------
        version_label : `str` or `(int, int, int)`
            a version definition (major, minor, patch) or "major.minor.patch"

        Returns
        -------
        `IDB` reference to a IDB reader

        """
        if self.has_version(version_label) : return IDB(Path(self._get_filename_for_version(version_label)))
        raise ValueError(f'Version "{version_label}" not found in: "{self._get_filename_for_version(version_label)}"')

if __name__ == '__main__': # pragma: no cover
    a = IdbManager("./stixcore/idb/tests/data")
    print(a.data_root)
    versions = a.get_versions()
    print(versions)
    for v in versions :
        print(v, a.has_version(v['version']))
    print(a.get_idb('2.26.3'))

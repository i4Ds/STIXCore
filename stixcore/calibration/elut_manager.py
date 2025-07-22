import sys
from pathlib import Path

from stixpy.calibration.detector import get_sci_channels
from stixpy.io.readers import read_elut, read_elut_index

from astropy.table import QTable

from stixcore.data.test import test_data
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ['ELUTManager']

ELUT_DATA_DIR = Path(__file__).parent.parent / "config" / "data" / "common" / "elut"

logger = get_logger(__name__)


class ELUTManager(metaclass=Singleton):
    """Manages ELUT (Energy Look-Up Table) data and provides date-based access to ELUT tables."""

    def __init__(self, data_root=None):
        """Create the manager for ELUT data.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`, optional
            Path to the directory with ELUT data. If None, uses default path.
        """
        self.elut_cache = {}
        if data_root is None:
            data_root = ELUT_DATA_DIR
        self.data_root = Path(data_root)
        self.elut_index_file = self.data_root / "elut_index.csv"
        self._load_index()

    def _load_index(self):
        """Load the ELUT index file and ensure it's ordered by start_date."""
        try:
            self.elut_index = read_elut_index(self.elut_index_file)
            logger.info(f"Loaded {len(self.elut_index)} ELUT entries from index")

        except FileNotFoundError:
            logger.warning(f'No ELUT index found at: {self.elut_index_file}')
            self.elut_index = []
        except Exception as e:
            logger.error(f'Error loading ELUT index: {e}')
            self.elut_index = []

    def _find_elut_file(self, date):
        """Find the appropriate ELUT file for a given date using binary search.

        Parameters
        ----------
        date : `datetime`
            The date for which to find the ELUT

        Returns
        -------
        `str` or `None`
            The filename of the appropriate ELUT file, or None if not found
        """
        if not self.elut_index:
            logger.warning("No ELUT index loaded")
            return None
        elut_info = self.elut_index.at(date)
        if len(elut_info) == 0:
            raise ValueError(f"No ELUT for for date {date}")
        elif len(elut_info) > 1:
            raise ValueError(f"Multiple ELUTs for for date {date}")
        start_date, end_date, elut_file = list(elut_info)[0]

        return elut_file

    def read_elut(self, elut_file, sci_channels):
        """Read an ELUT file and return as astropy QTable.

        Parameters
        ----------
        elut_file : `str`
            The filename of the ELUT file to read

        Returns
        -------
        `stixpy.io.readers.ELUT`
            The ELUT data with appropriate units

        Raises
        ------
        FileNotFoundError
            If the ELUT file doesn't exist
        """
        elut_path = self.data_root / elut_file

        if not elut_path.exists():
            raise FileNotFoundError(f"ELUT file not found: {elut_path}")

        try:
            elut_table = read_elut(elut_path, sci_channels)

            logger.info(f"Successfully read ELUT file: {elut_file}")
            return elut_table

        except Exception as e:
            logger.error(f"Error reading ELUT file {elut_file}: {e}")
            raise

    def get_elut(self, date) -> tuple[object, QTable]:
        """Get the ELUT table for a given date.

        Parameters
        ----------
        date : `datetime`
            The date for which to get the ELUT

        Returns
        -------
        `tuple`
            A tuple containing:
            `stixpy.io.readers.ELUT` The ELUT data
            `QTable` The science channel definition

        Raises
        ------
        ValueError
            If no ELUT is found for the given date
        FileNotFoundError
            If the ELUT file doesn't exist
        """
        # Find the appropriate ELUT file
        elut_file = self._find_elut_file(date)
        if not elut_file:
            raise ValueError(f"No ELUT found for date: {date}")

        # Check cache first
        if elut_file in self.elut_cache:
            logger.debug(f"Using cached ELUT: {elut_file}")
            return self.elut_cache[elut_file]

        # Read ELUT and cache it
        sci_channels = get_sci_channels(date)
        elut_table = self.read_elut(elut_file, sci_channels)
        self.elut_cache[elut_file] = (elut_table, sci_channels)

        return self.elut_cache[elut_file]

    def get_available_eluts(self):
        """Get list of all available ELUT files with their date ranges.

        Returns
        -------
        `list`
            List of dictionaries containing ELUT information
        """
        return self.elut_index

    def clear_cache(self):
        """Clear the ELUT cache."""
        self.elut_cache.clear()
        logger.info("ELUT cache cleared")

    @property
    def cache_size(self):
        """Get the current size of the ELUT cache.

        Returns
        -------
        `int`
            Number of cached ELUT tables
        """
        return len(self.elut_cache)


# Create singleton instance
if 'pytest' in sys.modules:
    ELUTManager.instance = ELUTManager(test_data.elut if hasattr(test_data, 'elut') else None)
else:
    ELUTManager.instance = ELUTManager()

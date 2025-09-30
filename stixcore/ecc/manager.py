import sys
import json
import shutil
import tempfile
from types import SimpleNamespace
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from configparser import ConfigParser

from stixcore.data.test import test_data
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ['ECCManager']

ECC_CONF_INDEX_FILE = Path(__file__).parent.parent / "config" / "data" / "common"\
    / "ecc" / "ecc_conf_index.json"

logger = get_logger(__name__)


class ECCManager(metaclass=Singleton):
    """Manages ECC configurations and provides access to configuration data."""

    def __init__(self, data_root=None):
        """Create the manager for ECC configurations.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`, optional
            Path to the directory with all ECC configurations. If None, uses default path.
        """
        self.config_cache = dict()
        if data_root is None:
            data_root = ECC_CONF_INDEX_FILE.parent
        self.data_root = data_root
        self._load_index()

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
            Path to the directory with all ECC configuration versions
        """
        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"Data root path does not exist: {path}")

        self._data_root = path

    def _load_index(self):
        """Load the ECC configuration index file."""
        try:
            with open(ECC_CONF_INDEX_FILE) as f:
                self.configurations = json.load(f)
                logger.info(f"Loaded {len(self.configurations)} ECC configurations from index")
        except FileNotFoundError:
            logger.warning(f'No ECC configuration index found at: {ECC_CONF_INDEX_FILE}')
            self.configurations = []
        except json.JSONDecodeError as e:
            logger.error(f'Error parsing ECC configuration index: {e}')
            self.configurations = []

    def find_configuration(self, date=None):
        """Find ECC configuration valid for a given date.

        Parameters
        ----------
        date : `datetime`, optional
            the date for which to find the configuration, by default None (uses first available)

        Returns
        -------
        `str`
            configuration name/identifier
        """
        if not self.configurations:
            logger.warning("No ECC configurations available")
            return None

        if date is None:
            return self.configurations[0]['configuration']

        # Convert date to string for comparison if it's a datetime object
        if isinstance(date, datetime):
            date_str = date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        else:
            date_str = str(date)

        for config in self.configurations:
            validity_period = config.get('validityPeriodUTC', [])
            if len(validity_period) == 2:
                start_date, end_date = validity_period
                if start_date <= date_str <= end_date:
                    return config['configuration']

        logger.warning(f"No ECC configuration found for date: {date}")
        return None

    def get_configurations(self):
        """Get all available ECC configurations.

        Returns
        -------
        `list`
            List of available configuration dictionaries
        """
        # make a copy of the configurations to avoid modifying the original
        return json.loads(json.dumps(self.configurations))

    def has_configuration(self, configuration_name):
        """Test if the ECC configuration is available.

        Parameters
        ----------
        configuration_name : `str`
            configuration identifier

        Returns
        -------
        `bool`
            does the configuration exist
        """
        config_path = self._data_root / configuration_name
        return config_path.exists() and config_path.is_dir()

    def get_configuration_path(self, configuration_name):
        """Get the path to a specific ECC configuration.

        Parameters
        ----------
        configuration_name : `str`
            configuration identifier

        Returns
        -------
        `pathlib.Path`
            path to the configuration directory
        """
        return self._data_root / configuration_name

    def create_context(self, date=None):
        """Create a temporary folder with ECC configuration files for a given date.

        Parameters
        ----------
        date : `datetime`, optional
            the date for which to create the context, by default None

        Returns
        -------
        `pathlib.Path`
            path to the temporary directory containing the configuration files
        `SimpleNamespace`
            config read from post_ecc.ini

        Raises
        ------
        ValueError
            if no configuration is found for the given date
        FileNotFoundError
            if the configuration directory doesn't exist
        """
        configuration_name = self.find_configuration(date)
        if not configuration_name:
            raise ValueError(f"No ECC configuration found for date: {date}")

        if not self.has_configuration(configuration_name):
            raise FileNotFoundError(f"Configuration directory not found: {configuration_name}")

        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp(prefix=f"ecc_context_{configuration_name}_"))

        try:
            # Copy configuration files to temporary directory
            config_source = self.get_configuration_path(configuration_name)
            shutil.copytree(config_source, temp_dir, dirs_exist_ok=True)

            logger.info(f"Created ECC context in: {temp_dir}")

            config = ConfigParser()
            config.read(temp_dir / "post_ecc.ini")

            ESS_Config = SimpleNamespace(Max_Gain_Prime=config.getfloat("DEFAULT", "Max_Gain_Prime",
                                                                        fallback=1.4),
                                         Min_Gain_Prime=config.getfloat("DEFAULT", "Min_Gain_Prime",
                                                                        fallback=0.4),
                                         Min_Gain=config.getfloat("DEFAULT", "Min_Gain",
                                                                  fallback=0.4),
                                         Ignore_Max_Gain_Prime_Det_Pix_List=json.loads(
                                             config.get("DEFAULT",
                                                        "Ignore_Max_Gain_Prime_Det_Pix_List",
                                                        fallback="[]")),
                                         Ignore_Min_Gain_Prime_Det_Pix_List=json.loads(
                                             config.get("DEFAULT",
                                                        "Ignore_Min_Gain_Prime_Det_Pix_List",
                                                        fallback="[]")),
                                         Ignore_Min_Gain_Det_Pix_List=json.loads(
                                             config.get("DEFAULT", "Ignore_Min_Gain_Det_Pix_List",
                                                        fallback="[]")))

            logger.info(f"Read config from in: {temp_dir / 'post_ecc.ini'}")
            return temp_dir, ESS_Config

        except Exception as e:
            # Clean up on error
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise e

    def cleanup_context(self, context):
        """Clean up a temporary context directory.

        Parameters
        ----------
        context_path : `pathlib.Path`
            path to the temporary context directory to clean up
        """
        try:
            context_path, _ = context
            if context_path.exists():
                shutil.rmtree(context_path)
                logger.info(f"Cleaned up ECC context: {context_path}")
        except Exception as e:
            logger.warning(f"Error cleaning up context {context_path}: {e}")

    @contextmanager
    def context(self, date=None):
        """Context manager for ECC configuration context.

        This provides a convenient way to use ECC configurations with automatic
        cleanup using Python's 'with' statement.

        Parameters
        ----------
        date : `datetime`, optional
            the date for which to create the context, by default None

        Yields
        ------
        `pathlib.Path`
            path to the temporary directory containing the configuration files

        Raises
        ------
        ValueError
            if no configuration is found for the given date
        FileNotFoundError
            if the configuration directory doesn't exist

        Examples
        --------
        >>> with ecc_manager.context(datetime(2021, 6, 15)) as context_path:
        ...     # Use configuration files in context_path
        ...     config_file = context_path / "ecc_cfg_1" / "config.json"
        ...     # Files are automatically cleaned up when exiting the with block
        """
        context_path = None
        try:
            context_path = self.create_context(date)
            yield context_path
        finally:
            if context_path is not None:
                self.cleanup_context(context_path)


# Create singleton instance
if 'pytest' in sys.modules:
    ECCManager.instance = ECCManager(test_data.ecc)
else:
    ECCManager.instance = ECCManager()

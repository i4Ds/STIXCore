from pathlib import Path

from stixcore.util.logging import get_logger

__all__ = ['SOCManager']


logger = get_logger(__name__)


class SOCManager:
    """Manages the SOC data exchange directory and provides excess and search methods."""

    def __init__(self, data_root):
        """Create the manager for a given data path root.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`
            Path to the directory with all SOC data files for processing
        """
        self.data_root = Path(data_root)
        if not self.data_root.exists():
            raise ValueError(f"path not found: {data_root}")
        logger.info(f"Create SOCManager @ {self.data_root}")

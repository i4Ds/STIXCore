# Licensed under a 3-clause BSD style license - see LICENSE.rst
from stixcore.util.logging import get_logger

try:
    from .version import __version__
except ImportError:
    __version__ = "unknown"

logger = get_logger(__name__)

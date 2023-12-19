# Licensed under a 3-clause BSD style license - see LICENSE.rst
from stixcore.util.logging import get_logger
from .version import version as __version__
from .version_conf import __version_conf__

try:
    from .version_conf import __version_conf__
except ImportError:
    __version_conf__ = "unknown"

logger = get_logger(__name__)

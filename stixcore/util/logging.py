"""
The logging module provides a common logging using pythons logging module. A root logger is
configured for the stixcore package on import and all subsequent loggers should be created as
children of the logger.

Examples
--------
.. code-block:: python

   #stixcore.mymodule (stixcore/mymodule.py)
   import logging
   from stixcore.util.logging import get_logger

   logger = get_logger(__name__)
   logger.debug('you will not see me')
   logger = get_logger(__name__, level=logging.DEBUG)
   logger.debug('you will see me')

   2020-11-25T12:21:27Z DEBUG: you will see me - stixcore.mymodule

"""
import logging


def get_logger(name, level=logging.WARNING):
    """
    Return a configured logger instance.

    Parameters
    ----------
    name : `str`
        Name of the logger
    level : `int` or level, optional
        Level of the logger e.g `logging.DEBUG`

    Returns
    -------
    `logging.Logger`
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s - %(name)s',
                                  datefmt='%Y-%m-%dT%H:%M:%SZ')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

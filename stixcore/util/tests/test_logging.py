import logging

from stixcore import logger
from stixcore.util.logging import get_logger


def test_root_logger():
    assert logger.name == "stixcore"
    assert logger.level == logging.INFO


def test_get_logger():
    test_logger = get_logger(__name__)
    assert test_logger.name == "stixcore.util.tests.test_logging"
    assert test_logger.level == logging.INFO

    test_logger = get_logger("another", level=logging.DEBUG)
    assert test_logger.name == "another"
    assert test_logger.level == logging.DEBUG

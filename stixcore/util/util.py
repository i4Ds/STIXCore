import re
from pathlib import Path

__all__ = [
    "get_complete_file_name",
    "get_incomplete_file_name",
    "get_complete_file_name_and_path",
    "get_incomplete_file_name_and_path",
    "is_incomplete_file_name",
]


def get_complete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return re.sub(r"_V([0-9]+)U([\._])", r"_V\1\2", name)


def get_complete_file_name_and_path(path):
    name = Path(path).name
    return path.parent / get_complete_file_name(name)


def is_incomplete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return re.match(r".*_V([0-9]+)U([\._]).*fits", name) is not None


def get_incomplete_file_name_and_path(path):
    name = Path(path).name
    return path.parent / get_incomplete_file_name(name)


def get_incomplete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return re.sub(r"_V([0-9]+)([\._])", r"_V\1U\2", name)

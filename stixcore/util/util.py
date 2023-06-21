import re

__all__ = ['get_complete_file_name', 'get_incomplete_file_name']


def get_complete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return re.sub(r'_V([0-9]+)U([\._])', r'_V\1\2', name)


def is_incomplete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return not (re.match(r'.*_V([0-9]+)U([\._]).*fits', name) is None)


def get_incomplete_file_name(name):
    # see https://github.com/i4Ds/STIXCore/issues/350
    return re.sub(r'_V([0-9]+)([\._])', r'_V\1U\2', name)

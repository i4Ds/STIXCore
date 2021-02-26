"""Wrap all test files into data object."""
import os
from pathlib import Path
from collections import defaultdict


def nested_dict():
    return defaultdict(nested_dict)


data_dir = Path(__file__).parent
test_dir = data_dir.joinpath('test')

TEST_DATA_FILES = nested_dict()


def _read_test_data():
    doc = ''
    start = len(test_dir.parts)
    for path, subdirs, files in os.walk(test_dir):
        for name in files:
            f = Path(os.path.join(path, name))
            c = TEST_DATA_FILES
            for i in range(start, len(f.parts)-1):
                c = c[f.parts[i]]
                doc += f'{f.parts[i]} > '
            c[name] = f
            fp = str(f).replace('\\', '\\\\')
            doc += f"{name} : {fp}\n\n"
    return doc


__doc__ = _read_test_data()


__all__ = ['TEST_DATA_FILES', 'data_dir', 'test_dir']

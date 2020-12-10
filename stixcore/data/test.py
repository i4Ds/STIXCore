import tarfile
from pathlib import Path
from urllib.request import urlopen

data_dir = Path(__file__).parent
test_dir = data_dir.joinpath('test')

__all__ = ['data_dir', 'test_dir']

# TODO update to production server when ready
TEST_DATA_BASE_URL = 'https://homepages.dias.ie/smaloney/stix-data/'
TEST_DATA_NAME = 'test_data.tar.gz'


def _download_test_tar(overwrite=False):
    destination = data_dir / TEST_DATA_NAME
    if destination.exists() and overwrite is False:
        return
    with urlopen(TEST_DATA_BASE_URL+TEST_DATA_NAME) as req:
        with destination.open(mode='wb') as file:
            file.write(req.read())


def _unzip_test_tar():
    data_tar = data_dir / TEST_DATA_NAME
    if data_tar.exists():
        with tarfile.open(data_tar.as_posix(), 'r:gz') as tar:
            tar.extractall(test_dir.absolute())


_download_test_tar()
_unzip_test_tar()

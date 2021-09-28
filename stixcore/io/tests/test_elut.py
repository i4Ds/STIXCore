from pathlib import Path
from unittest import mock

from astropy.table import Table

from stixcore.io.elut import read_elut, read_elut_index


@mock.patch('astropy.table.table.Table.read')
def test_read_elut_index(moc_table):
    t = Table(rows=[(0, '2020-02-14T00:00:00', '2021-01-01T00:00:00', 'elut_table_20200519.csv'),
                    (1, '2021-01-01T00:00:00', '2021-05-24T00:00:00', 'elut_table_20201204.csv'),
                    (2, '2021-05-24T00:00:00', 'none', 'elut_table_20210625.csv')],
              names=['index', 'start_date', 'end_date', 'elut_file'])
    moc_table.return_value = t
    root = Path(__file__).parent.parent.parent
    elut_path = Path(root, *['config', 'data', 'common', 'elut', 'elut_index.csv'])
    elut_index = read_elut_index(elut_path)

    assert len(elut_index) == 3
    for trow, iv in zip(t.iterrows(), elut_index.items()):
        trow[1] == iv[0].isoformat()
        trow[2] == iv[1].isoformat()
        trow[3] == iv[2].name


def test_read_elut():
    root = Path(__file__).parent.parent.parent
    elut_file = Path(root, *['config', 'data', 'common', 'elut', 'elut_table_20201204.csv'])
    read_elut(elut_file)
    assert True

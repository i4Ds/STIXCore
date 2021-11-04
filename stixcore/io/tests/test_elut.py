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
    elut = read_elut(elut_file)

    # make sure a few values are as expected
    assert elut.offset[0, 0] == 897.7778
    assert elut.offset[-1, -1] == 897.037
    assert elut.gain[0, 0] == 0.109012
    assert elut.gain[-1, -1] == 0.106871
    assert elut.adc[0, 0, 0] == 934
    assert elut.adc[-1, -1, -1] == 2301
    assert elut.e_actual[0, 0, 0] == 3.9486544664000047
    assert elut.e_actual[-1, -1, -1] == 150.042929773
    assert elut.e_width_actual[0, 0, 0] == 1.0901200000000002
    assert elut.e_width_actual[-1, -1, -1] == 30.03075100000001

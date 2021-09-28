from datetime import datetime

from dateutil.parser import parse
from intervaltree import IntervalTree

from astropy.table import Table


def read_elut_index(elut_index):
    elut = Table.read(elut_index)
    elut_it = IntervalTree()
    for i, start, end, file in elut.iterrows():
        date_start = parse(start)
        date_end = parse(end) if end != 'none' else datetime.now()
        elut_it.addi(date_start, date_end, elut_index.parent / file)
    return elut_it


def read_elut(elut_file):
    elut = Table.read(elut_file, header_start=2)
    return elut

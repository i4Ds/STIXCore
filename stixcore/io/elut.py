from pathlib import Path
from datetime import datetime

import numpy as np
from dateutil.parser import parse
from intervaltree import IntervalTree

from astropy.table import Table

from stixcore.config.reader import read_energy_channels


def read_elut_index(elut_index):
    elut = Table.read(elut_index)
    elut_it = IntervalTree()
    for i, start, end, file in elut.iterrows():
        date_start = parse(start)
        date_end = parse(end) if end != 'none' else datetime(2100, 1, 1)
        elut_it.addi(date_start, date_end, elut_index.parent / file)
    return elut_it


def read_elut(elut_file):
    root = Path(__file__).parent.parent
    sci_channels = read_energy_channels(Path(root, *['config', 'data', 'common',
                                                     'detector', 'ScienceEnergyChannels_1000.csv']))
    elut_table = Table.read(elut_file, header_start=2)

    elut = type('ELUT', (object,), dict())
    elut.file = elut_file.name
    elut.offset = elut_table['Offset'].reshape(32, 12)
    elut.gain = elut_table['Gain keV/ADC'].reshape(32, 12)
    elut.pixel = elut_table['Pixel'].reshape(32, 12)
    elut.detector = elut_table['Detector'].reshape(32, 12)
    adc = np.vstack(elut_table.columns[4:].values()).reshape(31, 32, 12)
    adc = np.moveaxis(adc, 0, 2)
    elut.adc = adc
    elut.e_actual = (elut.adc - elut.offset[..., None]) * elut.gain[..., None]
    elut.e_width_actual = (elut.e_actual[..., 1:] - elut.e_actual[..., :-1])

    elut.e = np.array([sc.e_lower for sc in sci_channels.values()])
    elut.e_width = np.array([sc.e_upper - sc.e_lower for sc in sci_channels.values()][1:-1])

    return elut

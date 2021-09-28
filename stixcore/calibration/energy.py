from pathlib import Path

import numpy as np

from stixcore.config.reader import read_energy_channels
from stixcore.io.elut import read_elut, read_elut_index


def energy_width_correction(date):
    root = Path(__file__).parent.parent
    elut_index_file = Path(root, *['config', 'data', 'common', 'elut', 'elut_index.csv'])
    sci_channels = read_energy_channels(Path(root, *['config', 'data', 'common',
                                                     'detector', 'ScienceEnergyChannels_1000.csv']))

    elut_index = read_elut_index(elut_index_file)
    elut_info = elut_index.at(date)
    if len(elut_info) == 0:
        raise ValueError('No ELUT for for date %s', date)
    elif len(elut_info) > 1:
        raise ValueError('Multiple ELUTs for for date %s', date)
    start_date, end_date, elut_file = list(elut_info)[0]
    elut_table = read_elut(elut_file)
    elut = type('ELUT', (object,), dict())

    elut.offset = elut_table['Offset'].reshape(32, 12)
    elut.gain = elut_table['Gain keV/ADC'].reshape(32, 12)
    elut.pixel = elut_table['Pixel'].reshape(32, 12)
    elut.detector = elut_table['Detector'].reshape(32, 12)
    adc = np.vstack(elut_table.columns[4:].values()).reshape(31, 32, 12)
    adc = np.moveaxis(adc, 0, 2)
    elut.adc = adc
    elut.e_actual = (elut.adc - elut.offset[..., None])*elut.gain[..., None]
    elut.e = [sc.e_lower for sc in sci_channels.values()]

    ebin_width = (elut.e_actual[..., 1:] - elut.e_actual[..., :-1])
    return ebin_width

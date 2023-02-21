import csv
from pathlib import Path
from datetime import datetime

import numpy as np
from dateutil.parser import parse
from intervaltree import IntervalTree

import astropy.units as u
from astropy.table import QTable, Table

from stixcore.config.data_types import EnergyChannel

__all__ = ['read_energy_channels', 'read_subc_params']

SCI_INDEX = None
SCI_CHANNELS = {}


def float_def(value, default=np.inf):
    """Parse the value into a float or return the default value.

    Parameters
    ----------
    value : `str`
        the value to parse
    default : `double`, optional
        default value to return in case of pasring errors, by default numpy.inf

    Returns
    -------
    `double`
        the parsed value
    """
    try:
        return float(value)
    except ValueError:
        return default


def int_def(value, default=0):
    """Parse the value into a int or return the default value.

    Parameters
    ----------
    value : `str`
        the value to parse
    default : `int`, optional
        default value to return in case of pasring errors, by default 0

    Returns
    -------
    `int`
        the parsed value
    """
    try:
        return int(value)
    except ValueError:
        return default


def get_sci_channels(date):
    r"""
    Get the science energy channel info for given date

    Parameters
    ----------
    date : `datetime.datetime`

    Returns
    -------
    `astropy.table.QTable`
        Science Energy Channels
    """
    global SCI_INDEX, SCI_CHANNELS

    # Cache index
    if SCI_INDEX is None:
        root = Path(__file__).parent.parent
        sci_chan_index_file = Path(root, *['config', 'data', 'common',
                                   'detector', 'science_echan_index.csv'])
        sci_chan_index = read_energy_channel_index(sci_chan_index_file)
        SCI_INDEX = sci_chan_index

    sci_info = SCI_INDEX.at(date)
    if len(sci_info) == 0:
        raise ValueError(f'No Science Energy Channel file found for date {date}')
    elif len(sci_info) > 1:
        raise ValueError(f'Multiple Science Energy Channel file for date {date}')
    start_date, end_date, sci_echan_file = list(sci_info)[0]

    # Cache sci channels
    if sci_echan_file.name in SCI_CHANNELS:
        sci_echan_table = SCI_CHANNELS[sci_echan_file.name]
    else:
        sci_echan_table = read_sci_energy_channels(sci_echan_file)
        SCI_CHANNELS[sci_echan_file.name] = sci_echan_table

    return sci_echan_table


def read_energy_channel_index(echan_index_file):
    r"""
    Read science energy channel index file

    Parameters
    ----------
    echan_index_file: `str` or `pathlib.Path`

    Returns
    -------
    Science Energy Channel lookup
    """
    echans = Table.read(echan_index_file)
    echan_it = IntervalTree()
    for i, start, end, file in echans.iterrows():
        date_start = parse(start)
        date_end = parse(end) if end != 'none' else datetime(2100, 1, 1)
        echan_it.addi(date_start, date_end, echan_index_file.parent / file)
    return echan_it


def read_sci_energy_channels(path):
    """
    Read science energy channel definitions.

    Parameters
    ----------
    path : `pathlib.Path`
        path to the config file

    Returns
    -------
    `astropy.table.QTable`
        The science energy channels
    """
    converters = {'Channel Number': int,
                  'Channel Edge': float,
                  'Energy Edge': float,
                  'Elower': float,
                  'Eupper': float,
                  'BinWidth': float,
                  'dE/E': float,
                  'QL channel': int}

    # tuples of (<match string>, '0')
    bad_data = (('max ADC', '0'), ('maxADC', '0'), ('n/a', '0'), ('', '0'))
    sci_chans = QTable.read(path, delimiter=',', data_start=24, header_start=21,
                            converters=converters, fill_values=bad_data)
    # set units can't use list comp
    for col in ['Elower', 'Eupper', 'BinWidth']:
        sci_chans[col].unit = u.keV
    return sci_chans


def read_energy_channels(path):
    """Read the energy channels from the configuration file.

    Parameters
    ----------
    path : `pathlib.Path`
        path to the config file

    Returns
    -------
    `dict`
        set of `EnergyChannel` accessible by index
    """
    energy_channels = dict()

    with open(path, newline='') as csvfile:
        csvreader = csv.reader(csvfile, dialect='excel')
        for _ in range(24):
            next(csvreader)

        for row in csvreader:
            idx = int_def(row[0], -1)
            if idx == -1:
                continue
            energy_channels[idx] = EnergyChannel(
                channel_edge=int_def(row[1]),
                energy_edge=int_def(row[2]),
                e_lower=float_def(row[3]),
                e_upper=float_def(row[4]),
                bin_width=float_def(row[5]),
                dE_E=float_def(row[6])
            )

    return energy_channels


def read_subc_params(path):
    """Read the configuration of the sub-collimator from the configuration file.

    Parameters
    ----------
    path : `pathlib.Path`
        path to the config file

    Returns
    -------
    `Table`
        params for all 32 sub-collimators
    """
    return Table.read(path, format='ascii')

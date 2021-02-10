import csv

import numpy as np

from stixcore.config.data_types import EnergyChannel

__all__ = ['read_energy_channels']


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

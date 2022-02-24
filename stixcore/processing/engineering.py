"""Processing module for converting raw to engineering values."""
import re
from collections.abc import Iterable

import numpy as np

from astropy.table.table import QTable

from stixcore.time import SCETime
from stixcore.tmtc.parameter import EngineeringParameter, Parameter
from stixcore.util.logging import get_logger

CCN = "__converted_column__"

__all__ = ['EngineeringParameter', 'raw_to_engineering']

logger = get_logger(__name__)


# TODO decide on raw as array input
def apply_raw_to_engineering(raw, args):
    """Wrap the raw to engineering converting algorithm into a callback.

    Parameters
    ----------
    raw : `stixcore.tmtc.parser.Parameter
        The the original parameter value
    args : `tuple`
        (IDBCalibrationParameter, IDB)

    Returns
    -------
    `EngineeringParameter`
        The raw and engineering value
    """
    param, idb = args
    en = None
    if param.PCF_CATEG == 'S':
        if isinstance(raw.value, Iterable):
            if isinstance(raw.value, list):
                raw.value = np.array(raw.value)

            en = np.array([idb.textual_interpret(param.PCF_CURTX, val.item())
                           for val in np.ravel(raw.value)]).reshape(raw.value.shape)

        else:
            en = idb.textual_interpret(param.PCF_CURTX, raw.value)
    elif param.PCF_CATEG == 'N':
        prefix = re.split(r'\d+', param.PCF_CURTX)[0]
        if prefix == 'CIXP':
            curve = idb.get_calibration_curve(param)
            en = curve(raw.value)
            if en is None:
                logger.error(f'Failed curve calibrate {param.PCF_NAME} / \
                               {param.PCF_CURTX} due to bad coefficients {curve}')
        elif prefix == 'CIX':
            poly = idb.get_calibration_polynomial(param.PCF_CURTX)
            en = poly(raw.value)
            if en is None:
                logger.error(f'Failed polynomial calibrate {param.PCF_NAME} / \
                               {param.PCF_CURTX} due to bad coefficients {poly}')
    else:
        er = (f'Unsupported calibration method: {param.PCF_CATEG} for ' +
              f'{param.PCF_NAME} / {param.PCF_CURTX}')
        logger.error(er)
        raise ValueError(er)

    # hardcoding RCR override do not pass back "State_0" ...
    if raw.name in ['NIX00276', 'NIX00401']:
        en = raw.value

    return EngineeringParameter(name=raw.name, value=raw.value, idb_info=raw.idb_info,
                                engineering=en, unit=param.PCF_UNIT)


def raw_to_engineering(packet):
    """Apply parameter raw to engineering conversion for the entire packet.

    Parameters
    ----------
    packet : `GenericTMPacket`
        The TM packet

    Returns
    -------
    `int`
        How many times the raw to engineering algorithm was called.
    """
    idb, calib_parameters = packet.get_calibration_params()

    if not calib_parameters:
        return 0

    c = 0
    for param in calib_parameters:
        c += packet.data.apply(param.PCF_NAME, apply_raw_to_engineering, (param, idb))
    return c


def raw_to_engineering_product(product, idbm):
    """Apply parameter raw to engineering conversion for the entire product.

    Parameters
    ----------
    product : `BaseProduct`
        The TM product as level 0

    Returns
    -------
    `int`
        How many columns where calibrated.
    """
    col_n = 0

    idb_ranges = QTable(rows=[(version, range.start.as_float(), range.end.as_float())
                              for version, range in product.idb_versions.items()],
                        names=["version", "obt_start", "obt_end"])
    idb_ranges.sort("obt_start")

    idb_ranges['obt_start'][0] = SCETime.min_time().as_float()
    for i in range(0, len(idb_ranges)-1):
        idb_ranges['obt_end'][i] = idb_ranges['obt_start'][i+1]
    idb_ranges['obt_end'][-1] = SCETime.max_time().as_float()

    for table, timecol in [(product.data, 'time'), (product.control, 'scet_coarse')]:
        if timecol == 'scet_coarse':
            if 'scet_coarse' in table.colnames:
                timevector = SCETime(coarse=table['scet_coarse'],
                                     fine=table['scet_fine']).as_float()
            else:
                # product per request (xray: no 'scet_coarse' in control)
                # do not have engineering values in control
                continue
        else:  # time
            timevector = table['time'].as_float()

        for col in table.colnames:
            if not (hasattr(table[col], "meta")
                    # and table[col].meta.get("PCF_CURTX", None) is not None
                    and not isinstance(table[col].meta.get("PCF_CURTX", None), (type(None), list))
                    and table[col].meta["NIXS"] is not None):
                continue
            col_n += 1
            c = 0

            # clone the current column into a new column as the content might be replaced chunk wise
            table[CCN] = table[col]

            for idbversion, starttime, endtime in idb_ranges.iterrows():
                idb = idbm.get_idb(idbversion)

                idb_time_period = np.where((starttime <= timevector) & (timevector < endtime))[0]
                if len(idb_time_period) < 1:
                    continue
                c += len(idb_time_period)
                calib_param = idb.get_params_for_calibration(
                                        product.service_type,
                                        product.service_subtype,
                                        (product.ssid if hasattr(product, "ssid") else None),
                                        table[col].meta["NIXS"],
                                        table[col].meta["PCF_CURTX"])[0]

                raw = Parameter(table[col].meta["NIXS"],
                                table[idb_time_period][col], None)

                eng = apply_raw_to_engineering(raw, (calib_param, idb))

                # cast the type of the column if needed
                if table[CCN].dtype != eng.engineering.dtype:
                    table[CCN] = table[CCN].astype(eng.engineering.dtype)

                # set the unit if needed
                if hasattr(eng.engineering, "unit") and table[CCN].unit != eng.engineering.unit:
                    meta = table[col].meta
                    table[CCN].unit = eng.engineering.unit
                    # restore the meta info
                    setattr(table[CCN], "meta", meta)

                # override the data into the new column
                table[CCN][idb_time_period] = eng.engineering

            # replace the old column with the converted
            table[col] = table[CCN]
            table[col].meta = table[CCN].meta
            # delete the generic column for conversion
            del table[CCN]
            # delete the calibration key from meta as it is now processed
            del table[col].meta["PCF_CURTX"]

            if c != len(table):
                logger.warning("Not all time bins got converted to engineering" +
                               "values due to bad idb periods." +
                               f"\n Converted bins: {c}\ntotal bins {len(table)}")

    return col_n

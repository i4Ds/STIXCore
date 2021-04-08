"""Processing module for converting raw to engineering values."""
import re
from collections.abc import Iterable

import numpy as np

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
    for col in product.data.colnames:
        if (not (hasattr(product.data[col], "meta")
                 and "PCF_CURTX" in product.data[col].meta
                 and product.data[col].meta["PCF_CURTX"] is not None
                 and product.data[col].meta["NIXS"] is not None
                 and hasattr(product, "idb")
                 )):
            continue
        col_n += 1
        c = 0

        # clone the current column into a new column as the content might be replaced chunk wise
        product.data[CCN] = product.data[col]

        for idbversion, time_range in product.idb.items():
            starttime = time_range.start.as_float()
            endtime = time_range.end.as_float()

            idb = idbm.get_idb(idbversion)
            idb_time_period = np.where((starttime <= product.data['time']) &
                                       (product.data['time'] <= endtime))[0]
            if len(idb_time_period) < 1:
                continue
            c += len(idb_time_period)
            calib_param = idb.get_params_for_calibration(
                                    product.service_type,
                                    product.service_subtype,
                                    (product.ssid if hasattr(product, "ssid") else None),
                                    product.data[col].meta["NIXS"],
                                    product.data[col].meta["PCF_CURTX"])[0]

            raw = Parameter(product.data[col].meta["NIXS"],
                            product.data[idb_time_period][col], None)

            eng = apply_raw_to_engineering(raw, (calib_param, idb))

            # cast the type of the column if needed
            if product.data[CCN].dtype != eng.engineering.dtype:
                product.data[CCN] = product.data[CCN].astype(eng.engineering.dtype)

            # set the unit if needed
            if hasattr(eng.engineering, "unit") and \
               product.data[CCN].unit != eng.engineering.unit:
                meta = product.data[col].meta
                product.data[CCN].unit = eng.engineering.unit
                # restore the meta info
                setattr(product.data[CCN], "meta", meta)

            # override the data into the new column
            product.data[CCN][idb_time_period] = eng.engineering

        # replace the old column with the converted
        product.data[col] = product.data[CCN]
        # delete the generic column for conversion
        del product.data[CCN]
        assert c == len(product.data)

    return col_n

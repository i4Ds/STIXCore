"""Processing module for converting raw to engineering values."""
import re

import numpy as np

import astropy.units as u

from stixcore.tmtc.parameter import EngineeringParameter, Parameter
from stixcore.util.logging import get_logger

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
        en = idb.textual_interpret(param.PCF_CURTX, raw.name)
        # if en is None:
        #   logger.error(f'Missing textual calibration info for: {param.PCF_NAME} / \
        #               {param.PCF_CURTX} value={raw}')
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
        for idbversion, (starttime, endtime) in product.idb.items():
            starttime = starttime * u.s
            endtime = endtime * u.s

            print(idbversion, starttime, endtime)
            idb = idbm.get_idb(idbversion)
            idb_time_period = np.where((starttime <= product.data['time']) &
                                       (product.data['time'] < endtime))[0]
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
            if product.data[col].dtype != eng.engineering.dtype:
                product.data[col] = product.data[col].astype(eng.engineering.dtype)

            # set the unit if needed
            if product.data[col].unit != eng.engineering.unit:
                meta = product.data[col].meta
                product.data[col].unit = eng.engineering.unit
                # restore the meta info
                setattr(product.data[col], "meta", meta)

            # override the data
            product.data[col][idb_time_period] = eng.engineering

        assert c == len(product.data)

    return col_n

"""Processing module for converting raw to engineering values."""

from scipy import interpolate

from stixcore.util.logging import get_logger

__all__ = ['EngineeringParameter', 'raw_to_engineering']

logger = get_logger(__name__)


class EngineeringParameter:
    """A class to combine the raw and engineering values and settings of a parameter.

    Attributes
    ----------
    raw : `int`|`list`
        The raw values before the decompression.
    engineering : `int`|`list`
        The Engineering values.
    error : `int`|`list`
        TODO tbd
    settings : `tuple`
        TODO tbd
    """

    def __init__(self, *, raw, engineering, unit):
        """Create a EngineeringParameter object.

        Parameters
        ----------
        raw : `int`|`list`
            The raw values.
        engineering : `int`|`list`
            The engineering values.
        """
        self.raw = raw
        self.engineering = engineering
        self.unit = unit

    def __repr__(self):
        return f'{self.__class__.__name__}(raw={self.raw}, engineering={self.engineering}, \
        unit={self.unit})'

    def __str__(self):
        return f'{self.__class__.__name__}(raw: len({len(self.raw)}), engineering: \
        len({len(self.engineering)}), unit={self.unit})'


# TODO deside on raw as array input
def apply_raw_to_engineering(raw, args):
    """Wrap the converting algorithm into a callback.

    Parameters
    ----------
    raw : `int`|`list`
        will be the original parameter value (input)
    args : `list`
        list of compression settings [s, k, m]

    Returns
    -------
    [type]
        [description]
    """
    param, idb = args
    en = None
    if param.PCF_CATEG == 'S':
        en = idb.textual_interpret(param.PCF_CURTX, raw)
        if en is None:
            logger.error(f'Missing textual calibration info for: {param.PCF_NAME} / \
                            {param.PCF_CURTX} value={raw}')
    elif param.PCF_CATEG == 'N':
        curve = idb.get_calibration_curve(param.PCF_CURTX)
        if len(curve) <= 1:
            logger.error(f'Invalid calibration parameter {param.PCF_NAME} / \
                          {param.PCF_CURTX}: at least two data points needed')
        elif len(curve) == 2:
            en = round((curve.y[1] - curve.y[0]) /
                       (curve.x[1] - curve.x[0]) *
                       (raw - curve.x[0]) + curve.y[0], 3)
        else:
            try:
                tck = interpolate.splrep(curve.x, curve.y)
                val = interpolate.splev(raw, tck)
                en = round(float(val), 3)
            except Exception as e:
                logger.error(f'Failed to calibrate {param.PCF_NAME} / \
                            {param.PCF_CURTX} due to {e}')
    else:
        er = (f'Unsupported calibration method: {param.PCF_CATEG} for ' +
              f'{param.PCF_NAME} / {param.PCF_CURTX}')
        logger.error(er)
        raise ValueError(er)

    return EngineeringParameter(raw=raw, engineering=en, unit=param.PCF_UNIT)


def raw_to_engineering(packet):
    """Apply parameter raw to engineering convertion for the entire packet.

    Parameters
    ----------
    packet : `GenericTMPacket`
        The TM packet

    Returns
    -------
    `int`
        How many times the raw to engineering algorithm was called.
    """
    calib_parameters = packet.get_calibration_params()

    if not calib_parameters:
        return 0

    idb = packet.get_idb()
    c = 0
    for param in calib_parameters:
        c += packet.data.apply(param.PCF_NAME, apply_raw_to_engineering, (param, idb))
    return c

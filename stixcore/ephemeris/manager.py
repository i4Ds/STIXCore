import os
import re
import logging
import tempfile
import functools
from pathlib import Path
from textwrap import wrap

import numpy as np
import spiceypy

import astropy.units as u
from astropy.time.core import Time as ApTime

from stixcore.util.logging import get_logger

SOLAR_ORBITER_ID = -144
SOLAR_ORBITER_SRF_FRAME_ID = -144000
SOLAR_ORBITER_STIX_ILS_FRAME_ID = -144851
SOLAR_ORBITER_STIX_OPT_FRAME_D = -144852

__all__ = ['SpiceManager', 'Time', 'Position']

logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)


class SpiceManager:
    """
    A context manager to ensure kernels are loaded and unloaded properly before and after use.
    """
    def __init__(self, meta_kernel_path):
        """
        Create an instance loading the spice kernel in the given meta kernel file

        Parameters
        ----------
        meta_kernel_path : `str` or `pathlib.Path`
            Path to the meta kernel

        """
        self.__spice_context_manager = False
        self.meta_kernel_path = Path(meta_kernel_path)
        if not self.meta_kernel_path.exists():
            raise ValueError(f'Meta kernel not found: {self.meta_kernel_path}')

        with self.meta_kernel_path.open('r') as mk:
            original_mk = mk.read()
            kernel_dir = str(self.meta_kernel_path.parent.parent.resolve())
            kernel_dir = kernel_dir.replace('\\', '\\\\')
            # spice meta kernel seems to have a max variable length of 80 characters
            # https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/kernel.html#Additional%20Meta-kernel%20Specifications # noqa
            wrapped = self._wrap_value_field(kernel_dir)
            path_value = f"PATH_VALUES       = ( {wrapped} )"
            logger.debug(f'Kernel directory {kernel_dir}')
            new_mk = re.sub(r"PATH_VALUES\s*= \( '.*' \)", path_value, original_mk)

            handle, path = tempfile.mkstemp()
            with os.fdopen(handle, 'w') as f:
                f.write(new_mk)

            self.mod_meta_kernel_path = Path(path)

        # *_, datestamp, version = self.meta_kernel_path.name.split('_')
        # self.kernel_date = datetime.strptime(datestamp, '%Y%m%d')

    def __enter__(self):
        spiceypy.furnsh(str(self.mod_meta_kernel_path))
        self.__spice_context_manager = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        spiceypy.unload(str(self.mod_meta_kernel_path))
        self.__spice_context_manager = False

    def spice_context(func):
        """
        A decorator to ensure functions are executed within the context manager.
        """
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.__spice_context_manager:
                raise NotInSpiceContext()
            return func(self, *args, **kwargs)
        return wrapper

    @staticmethod
    def _wrap_value_field(field):
        r"""
        Wrap a value field according to SPICE meta kernal spec

        Parameters
        ----------
        field : `str`
            The value to be wrapped

        Returns
        -------
        The wrapped values
        """
        parts = wrap(field, width=78)
        wrapped = "'"
        for part in parts[:-1]:
            wrapped = wrapped + part + "+'\n'"
        wrapped = wrapped + f"{parts[-1]}'"
        return wrapped


class NotInSpiceContext(Exception):
    """
    Raised when a function decorated with `spice_context` decorator is called out side a
    `SpiceManager` context.
    """


class Time(SpiceManager):
    """
    Convert between spacecraft elapsed times (SCET), UTC strings and datetime objects

    Examples
    --------
    >>> from stixcore.ephemeris.manager import Time
    >>> import stixcore.data.test
    >>> with Time(meta_kernel_path=stixcore.data.test.test_data.ephemeris.META_KERNEL_TIME) as time:
    ...     converted = time.scet_to_datetime('625237315:44104')
    >>> str(converted)
    '2019-10-24 13:01:50.672974+00:00'

    """
    spice_context = SpiceManager.spice_context

    @spice_context
    def scet_to_utc(self, scet):
        """
        Convert SCET to UTC time string in ISO format.

        Parameters
        ----------
        scet : `str`, `int`, `float`
            SCET time as a number or spacecraft clock string e.g. `1.0` or `625237315:44104`

        Returns
        -------
        `str`
            UTC time string in ISO format
        """
        # Obt to Ephemeris time (seconds past J2000)
        ephemeris_time = None
        if isinstance(scet, (float, int)):
            ephemeris_time = spiceypy.sct2e(SOLAR_ORBITER_ID, scet)
        elif isinstance(scet, str):
            ephemeris_time = spiceypy.scs2e(SOLAR_ORBITER_ID, scet)
        # Ephemeris time to Utc
        # Format of output epoch: ISOC (ISO Calendar format, UTC)
        # Digits of precision in fractional seconds: 6
        return spiceypy.et2utc(ephemeris_time, "ISOC", 3)

    @spice_context
    def utc_to_scet(self, utc):
        """
        Convert UTC ISO format to SCET time strings.

        Parameters
        ----------
        utc : `str`
            UTC time string in ISO format e.g. '2019-10-24T13:06:46.682758'

        Returns
        -------
        `str`
            SCET time string
        """
        # Utc to Ephemeris time (seconds past J2000)
        ephemeris_time = spiceypy.utc2et(utc)
        # Ephemeris time to Obt
        return spiceypy.sce2s(SOLAR_ORBITER_ID, ephemeris_time)

    @spice_context
    def scet_to_datetime(self, scet):
        """
        Convert SCET to datetime.

        Parameters
        ----------
        scet : `str`, `int`, `float`
            SCET time as number or spacecraft clock string e.g. `1.0` or `'625237315:44104'`

        Returns
        -------
        `datetime.datetime`
            Datetime of SCET

        """
        ephemeris_time = None
        if isinstance(scet, (float, int)):
            ephemeris_time = spiceypy.sct2e(SOLAR_ORBITER_ID, scet)
        elif isinstance(scet, str):
            ephemeris_time = spiceypy.scs2e(SOLAR_ORBITER_ID, scet)
        return spiceypy.et2datetime(ephemeris_time)

    @spice_context
    def datetime_to_scet(self, adatetime):
        """
        Convert datetime to SCET.

        Parameters
        ----------
        adatetime : `datetime.datetime` or `astropy.time.Time`
            Time to convert to SCET

        Returns
        -------
        `str`
            SCET of datetime encoded as spacecraft clock string
        """
        if isinstance(adatetime, ApTime):
            adatetime = adatetime.to_datetime()
        et = spiceypy.datetime2et(adatetime)
        scet = spiceypy.sce2s(SOLAR_ORBITER_ID, et)
        return scet


class Position(SpiceManager):
    """
    Obtain spacecraft position and orientation, convert to and from instrument coordinate system

    Examples
    --------
    >>> from datetime import datetime
    >>> from stixcore.ephemeris.manager import Position
    >>> import stixcore.data.test
    >>> with Position(
    ...     meta_kernel_path=stixcore.data.test.test_data.ephemeris.META_KERNEL_POS) as pos:
    ...     x, y, z = pos.get_position(date=datetime(2020, 10, 1), frame='SOLO_HEE')
    >>> x, y, z
    (<Quantity -92089164.00717261 km>,
    <Quantity 1.05385302e+08 km>,
    <Quantity 44917232.72028707 km>)
    """
    spice_context = SpiceManager.spice_context

    @spice_context
    def get_position(self, *, date, frame):
        """
        Get the position of SolarOrbiter at the given date in the given coordinate frame.

        Parameters
        ----------
        date : `datetime.datetime`
            Date at which to obtain position
        frame : `str`
            Name of the coordinate frame ('IAU_SUN', 'SOLO_HEE')

        Returns
        -------
        tuple
            The x, y, and z components of the spacecraft position
        """
        et = spiceypy.datetime2et(date)
        (x, y, z), lt = spiceypy.spkpos('SOLO', et, frame, 'None', 'SUN')
        return x*u.km, y*u.km, z*u.km

    @spice_context
    def get_orientation(self, date, frame):
        """
        Get the orientation or roll, pith and yaw of STIX (ILS or OPT).

        Parameters
        ----------
        date : `datetime.datetime`
            Date at which to obtain orientation information
        frame : `str`
            Name of the coordinate frame
        Returns
        -------
        tuple
            Roll, pitch and yaw of the spacecraft
        """
        et = spiceypy.datetime2et(date)
        sc = spiceypy.sce2c(SOLAR_ORBITER_ID, et)
        cmat, sc = spiceypy.ckgp(SOLAR_ORBITER_SRF_FRAME_ID, sc, 0, frame)
        vec = cmat @ np.eye(3)
        roll, pitch, yaw = spiceypy.m2eul(vec, 1, 2, 3)
        roll, pitch, yaw = np.rad2deg([roll, pitch, yaw])*u.deg
        return roll, pitch, yaw

    @spice_context
    def convert_to_inst(self, coords):
        """
        Convert the given coordinates to the instrument frame

        Parameters
        ----------
        coords : `astropy.coordinate.SkyCoord`
            The coordinates to transform to the instrument frame
        frame : `str`, optional
            The instrument coordinate frame (ILS or OPT)

        Returns
        -------
        tuple
            x and y coordinates
        """
        icrs_coords = coords.transform_to('icrs')
        cart_coords = icrs_coords.represent_as('cartesian')

        et = spiceypy.datetime2et(coords.obstime.to_datetime())
        sc = spiceypy.sce2c(SOLAR_ORBITER_ID, et)
        cmat, sc = spiceypy.ckgp(SOLAR_ORBITER_STIX_ILS_FRAME_ID, sc, 0, 'J2000')
        vec = cmat @ cart_coords.xyz.value
        # Rotate about z so +x towards the Sun
        # vec = np.array([[-1, 0, 0], [0, -1, 0], [0, 0, 1]]) @ vec
        distance, latitude, longitude = spiceypy.reclat(vec.reshape(3))

        y = (latitude + np.pi) * u.rad
        x = (np.pi/2 - longitude) * u.rad

        return x, y

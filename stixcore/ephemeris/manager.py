from datetime import datetime
from pathlib import Path

import spiceypy as spice
from astropy.time.core import Time

SOLAR_ORBITER_NAIF_ID = -144

__all__ = ['SpiceManager']


class SpiceManager:
    """
    Load the SolarOrbiter kernels as provided in the given meta kernel
    """
    def __init__(self, meta_kernel_path):
        """
        Load the spice kernel or kernels referenced in the give kernel file

        Parameters
        ----------
        meta_kernel_path : `str` or `pathlib.Path`
            Path to the meta kernel

        """
        self.mk_path = Path(meta_kernel_path)
        *_, datestamp, version = self.mk_path.name.split('_')
        self.kernel_date = datetime.strptime(datestamp, '%Y%m%d')
        spice.furnsh(str(meta_kernel_path))

    @staticmethod
    def scet_to_utc(scet):
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
        if isinstance(scet, (float, int)):
            ephemeris_time = spice.sct2e(SOLAR_ORBITER_NAIF_ID, scet)
        elif isinstance(scet, str):
            ephemeris_time = spice.scs2e(SOLAR_ORBITER_NAIF_ID, scet)
        # Ephemeris time to Utc
        # Format of output epoch: ISOC (ISO Calendar format, UTC)
        # Digits of precision in fractional seconds: 6
        return spice.et2utc(ephemeris_time, "ISOC", 3)

    @staticmethod
    def utc_to_scet(utc):
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
        ephemeris_time = spice.utc2et(utc)
        # Ephemeris time to Obt
        return spice.sce2s(SOLAR_ORBITER_NAIF_ID, ephemeris_time)

    @staticmethod
    def scet_to_datetime(scet):
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
        if isinstance(scet, (float, int)):
            ephemeris_time = spice.sct2e(SOLAR_ORBITER_NAIF_ID, scet)
        elif isinstance(scet, str):
            ephemeris_time = spice.scs2e(SOLAR_ORBITER_NAIF_ID, scet)
        return spice.et2datetime(ephemeris_time)

    @staticmethod
    def datetime_to_scet(datetime):
        """
        Convert datetime to SCET.

        Parameters
        ----------
        datetime : `datetime.datetime` or `astropy.time.Time`
            Time to convert to SCET

        Returns
        -------
        `str`
            SCET of datetime encoded as spacecraft clock string
        """
        if isinstance(datetime, Time):
            datetime = datetime.to_datetime()
        et = spice.datetime2et(datetime)
        scet = spice.sce2s(SOLAR_ORBITER_NAIF_ID, et)
        return scet

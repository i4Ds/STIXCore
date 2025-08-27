import re
import sys
import logging
from enum import Enum
from pathlib import Path
from datetime import datetime
from textwrap import wrap
from collections import defaultdict

import numpy as np
import spiceypy
from spiceypy.utils.exceptions import (
    NotFoundError,
    SpiceBADPARTNUMBER,
    SpiceINVALIDSCLKSTRING,
    SpiceyError,
)
from sunpy.coordinates import HeliographicCarrington, HeliographicStonyhurst

import astropy.units as u
from astropy.time.core import Time as ApTime

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

SOLAR_ORBITER_ID = -144
SOLAR_ORBITER_SRF_FRAME_ID = -144000
SOLAR_ORBITER_STIX_ILS_FRAME_ID = -144851
SOLAR_ORBITER_STIX_OPT_FRAME_D = -144852

__all__ = ["SpiceKernelLoader", "Spice", "SpiceKernelManager", "SpiceKernelType"]

logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)


class SpiceKernelType(Enum):
    """Different Spice Kernel types."""

    CK = "ck", "*.*"
    """Kernels that contain orientation for the spacecraft and some of its
    structures, (solar arrays, for instance)"""

    FK = "fk", "*.*"
    """Kernels that define reference frames needed for the Mission."""

    IK = "ik", "*.*"
    """Kernels for the instruments on board the spacecraft."""

    LSK = "lsk", "naif*.tls"
    """Leapseconds kernel."""

    MK = "mk", "solo_ANC_soc-flown-mk_*.tm"
    """Meta-kernel files (a.k.a "furnsh" files) that provide lists of kernels
    suitable for a given mission period."""

    MK_PRED = "mk", "solo_ANC_soc-pred-mk_*.tm"
    """Predicted Meta-kernel files (a.k.a "furnsh" files) that provide lists of kernels
    suitable for a given mission period "in the future"."""

    PCK = "pck", "*.*"
    """Kernels that define planetary constants."""

    SCLK = "sclk", "solo_ANC_soc-sclk_*.tsc"
    """Spacecraft clock coefficients kernels."""

    SPK = "spk", "*.*"
    """Orbit kernels, for the spacecraft and other solar system bodies."""


class SpiceKernelManager:
    """A class to manage Spice kernels in the local file system."""

    MK_DATE_PATTERN = re.compile(r"solo_ANC_soc.*_(\d{4})(\d{2})(\d{2})_.*.tm")
    OLDEST_MK_DATE = datetime.fromisoformat("2019-01-01T00:00:00.000")

    def __init__(self, path):
        """Creates a new SpiceKernelManager

        Parameters
        ----------
        path : `str` | `path`
            path to the base directory with all available Spice kernels

        Raises
        ------
        ValueError
            Path does not exists
        """
        path = Path(path)
        if not path.exists():
            raise ValueError(f"path not found: {path}")
        self.path = path

    def _get_latest(self, kerneltype, *, top_n=1):
        subdir, filter = kerneltype.value
        path = self.path / str(subdir)
        files = sorted(list(path.glob(filter)), key=lambda x: x.name.lower(), reverse=True)

        if len(files) == 0:
            raise ValueError(f"No current kernel found at: {path}")

        top_n = min(len(files), top_n)

        return files[0:top_n] if top_n > 1 else [files[0]]

    def get_latest_mk(self, *, top_n=1):
        return [SpiceKernelManager.get_mk_meta(mkp) for mkp in self._get_latest(SpiceKernelType.MK, top_n=top_n)]

    def get_latest_mk_pred(self, *, top_n=1):
        return [SpiceKernelManager.get_mk_meta(mkp) for mkp in self._get_latest(SpiceKernelType.MK_PRED, top_n=top_n)]

    def get_latest_mk_and_pred(self, *, top_n=1):
        mks = self.get_latest_mk_pred(top_n=top_n)
        mks.extend(self.get_latest_mk(top_n=top_n))
        return mks

    @classmethod
    def get_mk_date(cls, path: Path):
        try:
            ds = SpiceKernelManager.MK_DATE_PATTERN.findall(path.name)[0]
            return datetime.fromisoformat(f"{ds[0]}-{ds[1]}-{ds[2]}T00:00:00.000")
        except Exception:
            # return a date before the start so that comparisons will fail
            # TODO check if this is OK
            return SpiceKernelManager.OLDEST_MK_DATE
        return None

    @classmethod
    def get_mk_meta(cls, path: Path):
        date = cls.get_mk_date(path)
        type = "flown" if "-flown-" in path.name else "pred"
        return (path, type, date)

    def get_latest(self, kerneltype=SpiceKernelType.MK, *, top_n=1):
        """Finds the latest version of the spice kernel.

        Parameters
        ----------
        kerneltype : `SpiceKernelType`, optional
            the spice kernel type to looking for, by default SpiceKernelType.MK

        Returns
        -------
        `Path`
            Path to the latest found spice kernel file of the given type.
        """
        return self._get_latest(kerneltype)


class SpiceKernelLoader:
    """A context manager to ensure kernels are loaded and unloaded properly before and after use."""

    def __init__(self, meta_kernel_pathes):
        """Create an instance loading the spice kernel in the given meta kernel file.

        Parameters
        ----------
        meta_kernel_path : list of `str` or `pathlib.Path`
            Path to the meta kernel

        """
        self.meta_kernel_path = list()
        # unload all old kernels
        spiceypy.kclear()

        self.mk_dates = defaultdict(lambda: SpiceKernelManager.OLDEST_MK_DATE)

        for meta_kernel_path, mk_type, mk_date in np.atleast_1d(meta_kernel_pathes):
            try:
                meta_kernel_path = Path(meta_kernel_path)
                if not meta_kernel_path.exists():
                    raise ValueError(f"Meta kernel not found: {meta_kernel_path}")

                # look for a twin file *.abs where the path definition is absolute
                # if not existing create it on the fly and store it in same location for later reuse

                abs_file = meta_kernel_path.parent / (meta_kernel_path.name + ".abs")

                if not abs_file.exists():
                    with meta_kernel_path.open("r") as mk:
                        original_mk = mk.read()
                        kernel_dir = str(meta_kernel_path.parent.parent.resolve())
                        kernel_dir = kernel_dir.replace("\\", "\\\\")
                        # spice meta kernel seems to have a max variable length of 80 characters
                        # https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/kernel.html#Additional%20Meta-kernel%20Specifications # noqa
                        wrapped = SpiceKernelLoader._wrap_value_field(kernel_dir)
                        path_value = f"PATH_VALUES       = ( {wrapped} )"
                        logger.debug(f"Kernel directory {kernel_dir}")
                        new_mk = re.sub(r"PATH_VALUES\s*= \( '.*' \)", path_value, original_mk)

                        with abs_file.open("w") as f:
                            f.write(new_mk)
                # load the meta kernel
                spiceypy.furnsh(str(abs_file))
                logger.info(f"LOADING NEW META KERNEL: {meta_kernel_path}")
                self.meta_kernel_path.append((meta_kernel_path, mk_type, mk_date))
                self.mk_dates[mk_type] = max(self.mk_dates[mk_type], mk_date)
            except Exception as e:
                logger.warning(f"Failed LOADING NEW META KERNEL: {meta_kernel_path}\n{e}")

        if len(self.meta_kernel_path) == 0:
            raise ValueError(f"Failed to load any NEW META KERNEL: {meta_kernel_pathes}")

    def get_mk_date(self, meta_kernel_type="flown"):
        return self.mk_dates[meta_kernel_type]

    @staticmethod
    def _wrap_value_field(field):
        r"""
        Wrap a value field according to SPICE meta kernel spec
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


class Spice(SpiceKernelLoader, metaclass=Singleton):
    """Wrapper to spice functions.

       Convert between spacecraft elapsed times (SCET), UTC strings and datetime objects.
       Obtain spacecraft position and orientation, convert to and from instrument coordinate system

    Examples
    --------
    >>> from stixcore.ephemeris.manager import Spice
    >>> import stixcore.data.test
    >>> converted = Spice.instance.scet_to_datetime('625237315:44104')
    >>> str(converted)
    '2019-10-24 13:01:50.672974+00:00'

    >>> from datetime import datetime
    >>> from stixcore.ephemeris.manager import Spice
    >>> import stixcore.data.test
    >>> x, y, z = Spice.instance.get_position(date=datetime(2020, 10, 1), frame='SOLO_HEE')
    >>> x, y, z
    (<Quantity -92089164.00717261 km>,
    <Quantity 1.05385302e+08 km>,
    <Quantity 44917232.72028707 km>)
    """

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

    def get_auxiliary_positional_data(self, *, date):
        et = spiceypy.scs2e(SOLAR_ORBITER_ID, str(date))
        sc = spiceypy.sce2c(SOLAR_ORBITER_ID, et)

        try:
            cmat, *_ = spiceypy.ckgp(SOLAR_ORBITER_STIX_ILS_FRAME_ID, sc, 1.0, "SOLO_SUN_RTN")
            vec = cmat @ np.eye(3)
            roll, pitch, yaw = spiceypy.m2eul(vec, 1, 2, 3)
        except (SpiceyError, NotFoundError) as e:
            logger.error("Spice error obtaining SO pointing", exc_info=e)
            roll, pitch, yaw = np.full(3, np.nan)

        # HeliographicStonyhurst
        try:
            solo_sun_hg, _ = spiceypy.spkezr("SOLO", et, "SUN_EARTH_CEQU", "None", "Sun")
        except SpiceyError as e:
            logger.error("Spice error obtaining SO position", exc_info=e)
            solo_sun_hg = np.full(6, np.nan)

        # Convert to spherical and add units
        hg_rad, hg_lon, hg_lat = spiceypy.reclat(solo_sun_hg[:3])
        hg_rad = hg_rad * u.km
        hg_lat, hg_lon = (hg_lat * u.rad).to("deg"), (hg_lon * u.rad).to("deg")
        # Calculate radial velocity add units
        rad_vel, *_ = spiceypy.reclat(solo_sun_hg[3:])
        rad_vel = rad_vel * (u.km / u.s)

        solo_sun_heeq, _ = spiceypy.spkezr("SOLO", et, "SOLO_HEEQ", "None", "Sun")
        return (
            np.rad2deg([roll, pitch, yaw]) * u.deg,
            hg_rad,
            [hg_lon.to_value("deg"), hg_lat.to_value("deg")] * u.deg,
            solo_sun_heeq[0:3] * u.km,
        )

    def get_sun_disc_size(self, *, date):
        # if hasattr(date, "size") and date.size > 0:
        #    et = [spiceypy.scs2e(SOLAR_ORBITER_ID, str(d)) for d in date]
        # else:
        et = spiceypy.scs2e(SOLAR_ORBITER_ID, str(date))

        # HeliographicStonyhurst
        solo_sun_hg, sun_solo_lt = spiceypy.spkezr("SOLO", et, "SUN_EARTH_CEQU", "None", "Sun")

        # Convert to spherical and add units
        hg_rad, hg_lon, hg_lat = spiceypy.reclat(solo_sun_hg[:3])
        hg_rad = hg_rad * u.km

        rsun_arc = np.arcsin((1 * u.R_sun) / hg_rad).decompose().to("arcsec")

        return rsun_arc

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
        (x, y, z), lt = spiceypy.spkpos("SOLO", et, frame, "None", "SUN")
        return x * u.km, y * u.km, z * u.km

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
        cmat, sc = spiceypy.ckgp(SOLAR_ORBITER_SRF_FRAME_ID, sc, 1.0, frame)
        vec = cmat @ np.eye(3)
        roll, pitch, yaw = spiceypy.m2eul(vec, 1, 2, 3)
        roll, pitch, yaw = np.rad2deg([roll, pitch, yaw]) * u.deg
        return roll, pitch, yaw

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
        icrs_coords = coords.transform_to("icrs")
        cart_coords = icrs_coords.represent_as("cartesian")

        et = spiceypy.datetime2et(coords.obstime.to_datetime())
        sc = spiceypy.sce2c(SOLAR_ORBITER_ID, et)
        cmat, sc = spiceypy.ckgp(SOLAR_ORBITER_STIX_ILS_FRAME_ID, sc, 0, "J2000")
        vec = cmat @ cart_coords.xyz.value
        # Rotate about z so +x towards the Sun
        # vec = np.array([[-1, 0, 0], [0, -1, 0], [0, 0, 1]]) @ vec
        distance, latitude, longitude = spiceypy.reclat(vec.reshape(3))

        y = (latitude + np.pi) * u.rad
        x = (np.pi / 2 - longitude) * u.rad

        return x, y

    def get_fits_headers(self, *, start_time, average_time):
        try:
            et = spiceypy.scs2e(SOLAR_ORBITER_ID, str(average_time))
        except (SpiceBADPARTNUMBER, SpiceINVALIDSCLKSTRING):
            et = spiceypy.utc2et(average_time.isot)

        headers = (
            ("SPICE_MK", ", ".join([mkp.name for mkp, mkt, mkd in self.meta_kernel_path]), "SPICE meta kernel file"),
        )

        header_results = defaultdict(lambda: "")
        try:
            # HeliographicStonyhurst
            solo_sun_hg, sun_solo_lt = spiceypy.spkezr("SOLO", et, "SUN_EARTH_CEQU", "None", "Sun")
            # Convert to spherical and add units
            hg_rad, hg_lon, hg_lat = spiceypy.reclat(solo_sun_hg[:3])
            hg_rad = hg_rad * u.km
            hg_lat, hg_lon = (hg_lat * u.rad).to("deg"), (hg_lon * u.rad).to("deg")
            # Calculate radial velocity add units
            rad_vel, *_ = spiceypy.reclat(solo_sun_hg[3:])
            rad_vel = rad_vel * (u.km / u.s)

            hgs = HeliographicStonyhurst(hg_lon, hg_lat, hg_rad, obstime=average_time.to_datetime())
            hgc = hgs.transform_to(HeliographicCarrington(obstime=hgs.obstime, observer="Earth"))

            rsun_arc = np.arcsin((1 * u.R_sun) / hg_rad).decompose().to("arcsec")

            solo_sun_hee, _ = spiceypy.spkezr("SOLO", et, "SOLO_HEE", "None", "Sun")
            solo_sun_hci, _ = spiceypy.spkezr("SOLO", et, "SOLO_HCI", "None", "Sun")
            solo_sun_hae, _ = spiceypy.spkezr("SOLO", et, "SUN_ARIES_ECL", "None", "Sun")
            solo_sun_heeq, _ = spiceypy.spkezr("SOLO", et, "SOLO_HEEQ", "None", "Sun")
            solo_sun_gse, earth_solo_lt = spiceypy.spkezr("SOLO", et, "EARTH_SUN_ECL", "None", "Earth")
            sun_earth_hee, sun_earth_lt = spiceypy.spkezr("Earth", et, "SOLO_HEE", "None", "Sun")

            header_results["RSUN_ARC"] = rsun_arc.to_value("arcsec")
            header_results["HGLT_OBS"] = hg_lat.to_value("deg")
            header_results["HGLN_OBS"] = hg_lon.to_value("deg")
            header_results["CRLT_OBS"] = hgc.lat.to_value("deg")
            header_results["CRLN_OBS"] = hgc.lon.to_value("deg")
            header_results["DSUN_OBS"] = hg_rad.to_value("m")
            header_results["HEEX_OBS"] = (solo_sun_hee[0] * u.km).to_value("m")
            header_results["HEEY_OBS"] = (solo_sun_hee[1] * u.km).to_value("m")
            header_results["HEEZ_OBS"] = (solo_sun_hee[2] * u.km).to_value("m")
            header_results["HCIX_OBS"] = (solo_sun_hci[0] * u.km).to_value("m")
            header_results["HCIY_OBS"] = (solo_sun_hci[1] * u.km).to_value("m")
            header_results["HCIZ_OBS"] = (solo_sun_hci[2] * u.km).to_value("m")
            header_results["HCIX_VOB"] = (solo_sun_hci[3] * (u.km / u.s)).to_value("m/s")
            header_results["HCIY_VOB"] = (solo_sun_hci[4] * (u.km / u.s)).to_value("m/s")
            header_results["HCIZ_VOB"] = (solo_sun_hci[5] * (u.km / u.s)).to_value("m/s")
            header_results["HAEX_OBS"] = (solo_sun_hae[0] * u.km).to_value("m")
            header_results["HAEY_OBS"] = (solo_sun_hae[1] * u.km).to_value("m")
            header_results["HAEZ_OBS"] = (solo_sun_hae[0] * u.km).to_value("m")
            header_results["HEQX_OBS"] = (solo_sun_heeq[0] * u.km).to_value("m")
            header_results["HEQY_OBS"] = (solo_sun_heeq[1] * u.km).to_value("m")
            header_results["HEQZ_OBS"] = (solo_sun_heeq[2] * u.km).to_value("m")
            header_results["GSEX_OBS"] = (solo_sun_gse[0] * u.km).to_value("m")
            header_results["GSEY_OBS"] = (solo_sun_gse[1] * u.km).to_value("m")
            header_results["GSEZ_OBS"] = (solo_sun_gse[2] * u.km).to_value("m")
            header_results["OBS_VR"] = rad_vel.to_value("m/s")
            header_results["EAR_TDEL"] = sun_earth_lt - sun_solo_lt
            header_results["SUN_TIME"] = sun_solo_lt
            header_results["DATE_EAR"] = (start_time + (sun_earth_lt - sun_solo_lt) * u.s).fits
            header_results["DATE_SUN"] = (start_time - sun_solo_lt * u.s).fits

        except Exception:
            headers = headers + (("SPICE_ER", "1", "Pointing Data might be corrupt due to SPICE / time issues"),)

        headers = headers + (
            ("RSUN_ARC", header_results["RSUN_ARC"], "[arcsec] Apparent photospheric solar radius"),
            # ('CAR_ROT', ,), Doesn't make sense as we don't have a crpix
            ("HGLT_OBS", header_results["HGLT_OBS"], "[deg] s/c heliographic latitude (B0 angle)"),
            ("HGLN_OBS", header_results["HGLN_OBS"], "[deg] s/c heliographic longitude"),
            # Not mistake same values know by different terms
            ("CRLT_OBS", header_results["CRLT_OBS"], "[deg] s/c Carrington latitude (B0 angle)"),
            ("CRLN_OBS", header_results["CRLN_OBS"], "[deg] s/c Carrington longitude (L0 angle)"),
            ("DSUN_OBS", header_results["DSUN_OBS"], "[m] s/c distance from Sun"),
            ("HEEX_OBS", header_results["HEEX_OBS"], "[m] s/c Heliocentric Earth Ecliptic X"),
            ("HEEY_OBS", header_results["HEEY_OBS"], "[m] s/c Heliocentric Earth Ecliptic Y"),
            ("HEEZ_OBS", header_results["HEEZ_OBS"], "[m] s/c Heliocentric Earth Ecliptic Z"),
            ("HCIX_OBS", header_results["HCIX_OBS"], "[m] s/c Heliocentric Inertial X"),
            ("HCIY_OBS", header_results["HCIY_OBS"], "[m] s/c Heliocentric Inertial Y"),
            ("HCIZ_OBS", header_results["HCIZ_OBS"], "[m] s/c Heliocentric Inertial Z"),
            ("HCIX_VOB", header_results["HCIX_VOB"], "[m/s] s/c Heliocentric Inertial X Velocity"),
            ("HCIY_VOB", header_results["HCIY_VOB"], "[m/s] s/c Heliocentric Inertial Y Velocity"),
            ("HCIZ_VOB", header_results["HCIZ_VOB"], "[m/s] s/c Heliocentric Inertial Z Velocity"),
            ("HAEX_OBS", header_results["HAEX_OBS"], "[m] s/c Heliocentric Aries Ecliptic X"),
            ("HAEY_OBS", header_results["HAEY_OBS"], "[m] s/c Heliocentric Aries Ecliptic Y"),
            ("HAEZ_OBS", header_results["HAEZ_OBS"], "[m] s/c Heliocentric Aries Ecliptic Z"),
            ("HEQX_OBS", header_results["HEQX_OBS"], "[m] s/c Heliocentric Earth Equatorial X"),
            ("HEQY_OBS", header_results["HEQY_OBS"], "[m] s/c Heliocentric Earth Equatorial Y"),
            ("HEQZ_OBS", header_results["HEQZ_OBS"], "[m] s/c Heliocentric Earth Equatorial Z"),
            ("GSEX_OBS", header_results["GSEX_OBS"], "[m] s/c Geocentric Solar Ecliptic X"),
            ("GSEY_OBS", header_results["GSEY_OBS"], "[m] s/c Geocentric Solar Ecliptic Y"),
            ("GSEZ_OBS", header_results["GSEZ_OBS"], "[m] s/c Geocentric Solar Ecliptic Y"),
            ("OBS_VR", header_results["OBS_VR"], "[m/s] Radial velocity of spacecraft relative to Sun"),
            ("EAR_TDEL", header_results["EAR_TDEL"], "[s] Time(Sun to Earth) - Time(Sun to S/C)"),
            ("SUN_TIME", header_results["SUN_TIME"], "[s] Time(Sun to s/c)"),
            ("DATE_EAR", header_results["DATE_EAR"], "Start time of observation, corrected to Earth"),
            ("DATE_SUN", header_results["DATE_SUN"], "Start time of observation, corrected to Sun"),
        )

        return headers


if "pytest" in sys.modules:
    # only set the global in test scenario
    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(_spm.get_latest_mk_and_pred())

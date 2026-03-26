from datetime import date

import numpy as np
import pytest
from sunpy.coordinates import HeliographicStonyhurst

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import QTable
from astropy.tests.helper import assert_quantity_allclose
from astropy.time import Time

from stixcore.io.product_processors.fits.processors import FitsL3Processor
from stixcore.products.level3.flarelist import FlarelistSDCLoc
from stixcore.products.product import Product

N = 10


@pytest.fixture
def flare_data():
    peak_times = Time("2022-01-01T12:00:00") + np.arange(N) * 600 * u.s

    lon = np.linspace(0, 30, N)
    lat = np.linspace(-5, 5, N)
    # mark same rows fully NaN so the entire SkyCoord row is invalid
    lon[2] = lat[2] = np.nan
    lon[7] = lat[7] = np.nan

    hgs_coords = SkyCoord(
        lon=lon * u.deg,
        lat=lat * u.deg,
        radius=np.ones(N) * 1.0 * u.AU,
        frame=HeliographicStonyhurst(obstime=peak_times),
    )

    data = QTable()
    data["peak_UTC"] = peak_times
    data["start_UTC"] = peak_times - 60 * u.s
    data["end_UTC"] = peak_times + 60 * u.s
    data["duration"] = np.ones(N) * 120 * u.s
    data["lc_peak"] = np.ones((N, 5)) * u.ct / u.s
    data["location_hgs"] = hgs_coords

    return data


@pytest.fixture
def written_fits(flare_data, tmp_path):
    prod = FlarelistSDCLoc(
        data=flare_data,
        month=date(2022, 1, 1),
        control=QTable(),
    )

    # minimal header bypasses the Spice-dependent header generation chain
    header = fits.Header()
    header["LEVEL"] = "L3"
    header["STYPE"] = 0
    header["SSTYPE"] = 0
    header["SSID"] = 3
    header["DATE-BEG"] = "2022-01-01T00:00:00"
    prod.fits_header = header
    prod.energy = None
    prod._additional_header_keywords = []

    writer = FitsL3Processor(tmp_path)
    written = writer.write_fits(prod)
    assert len(written) == 1

    return prod, written[0]


def test_flarelist_sdcloc_location_roundtrip(written_fits):
    prod, fits_path = written_fits
    orig_hgs_lon = prod.data["location_hgs"].lon.copy()
    orig_hgs_lat = prod.data["location_hgs"].lat.copy()

    # read back via Product factory — calls on_deserialize internally
    recovered = Product(fits_path)

    assert isinstance(recovered, FlarelistSDCLoc)
    assert_quantity_allclose(recovered.data["location_hgs"].lon, orig_hgs_lon, atol=1e-6 * u.deg, equal_nan=True)
    assert_quantity_allclose(recovered.data["location_hgs"].lat, orig_hgs_lat, atol=1e-6 * u.deg, equal_nan=True)


def test_flarelist_sdcloc_fits_stores_icrs(written_fits):
    prod, fits_path = written_fits
    orig_hgs_lon = prod.data["location_hgs"].lon.copy()
    orig_hgs_lat = prod.data["location_hgs"].lat.copy()

    # read the DATA extension directly — no on_deserialize, raw FITS content
    raw = QTable.read(fits_path, hdu="DATA", astropy_native=True)

    assert "location_hgs" not in raw.colnames, "HGS column should not be stored in FITS"
    assert "location_icrs" in raw.colnames, "ICRS column should be present in FITS"

    # manually transform ICRS back to HGS and compare with original
    obstime = Time(raw["peak_UTC"])
    hgs = raw["location_icrs"].transform_to(HeliographicStonyhurst(obstime=obstime))
    assert_quantity_allclose(hgs.lon, orig_hgs_lon, atol=1e-6 * u.deg, equal_nan=True)
    assert_quantity_allclose(hgs.lat, orig_hgs_lat, atol=1e-6 * u.deg, equal_nan=True)

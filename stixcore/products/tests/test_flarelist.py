from datetime import date

import numpy as np
import pytest
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import QTable
from astropy.time import Time

from stixcore.io.product_processors.fits.processors import FitsL3Processor
from stixcore.products.level3.flarelist import FlarelistSDCLoc
from stixcore.products.product import Product

N = 10


@pytest.fixture
def flare_data():
    peak_times = Time("2022-01-01T12:00:00") + np.arange(N) * 600 * u.s

    hgs_coords = SkyCoord(
        lon=np.linspace(0, 30, N) * u.deg,
        lat=np.linspace(-5, 5, N) * u.deg,
        radius=np.ones(N) * 1.0 * u.AU,
        frame=HeliographicStonyhurst(obstime=peak_times),
    )

    hp_coords = SkyCoord(
        Tx=np.linspace(-300, 300, N) * u.arcsec,
        Ty=np.linspace(-200, 200, N) * u.arcsec,
        frame=Helioprojective(obstime=peak_times, observer=hgs_coords),
    )

    data = QTable()
    data["peak_UTC"] = peak_times
    data["start_UTC"] = peak_times - 60 * u.s
    data["end_UTC"] = peak_times + 60 * u.s
    data["duration"] = np.ones(N) * 120 * u.s
    data["lc_peak"] = np.ones((N, 5)) * u.ct / u.s
    data["location_hgs"] = hgs_coords
    data["location_hp"] = hp_coords

    return data


def test_flarelist_sdcloc_location_roundtrip(flare_data, tmp_path):
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

    # energy/additional_header_keywords are not set for freshly created products
    prod.energy = None
    prod._additional_header_keywords = []

    orig_hgs_lon = prod.data["location_hgs"].lon.copy()
    orig_hgs_lat = prod.data["location_hgs"].lat.copy()
    orig_hp_tx = prod.data["location_hp"].Tx.copy()
    orig_hp_ty = prod.data["location_hp"].Ty.copy()

    # write via FitsL3Processor — calls on_serialize internally, prod.data unchanged
    writer = FitsL3Processor(tmp_path)
    written_file_name = writer.write_fits(prod)
    assert len(written_file_name) == 1

    # read back via Product factory — calls on_deserialize internally
    recovered = Product(written_file_name[0])

    assert isinstance(recovered, FlarelistSDCLoc)
    assert u.allclose(recovered.data["location_hgs"].lon, orig_hgs_lon, atol=1e-6 * u.deg)
    assert u.allclose(recovered.data["location_hgs"].lat, orig_hgs_lat, atol=1e-6 * u.deg)
    assert u.allclose(recovered.data["location_hp"].Tx, orig_hp_tx, atol=1e-3 * u.arcsec)
    assert u.allclose(recovered.data["location_hp"].Ty, orig_hp_ty, atol=1e-3 * u.arcsec)

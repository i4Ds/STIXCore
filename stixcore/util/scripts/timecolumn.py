from pathlib import Path
from datetime import datetime

from stixpy.coordinates.transforms import STIXImaging, get_hpc_info
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.table.table import QTable
from astropy.time import Time

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.product_processors.fits.processors import FitsL3Processor
from stixcore.products.level3.flarelist import FlarelistSDC
from stixcore.products.product import Product

if __name__ == "__main__":
    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    spicemeta = _spm.get_latest_mk_and_pred()

    Spice.instance = Spice(spicemeta)

    l3_fits_writer = FitsL3Processor(Path("."))

    data = QTable()
    start_utc = Time(datetime.now()) - 100 * u.d
    peak_utc = Time(datetime.now()) - 98 * u.d
    end_utc = Time(datetime.now()) - 96 * u.d

    start_utc_2 = Time(datetime.now()) - 10 * u.d
    peak_utc_2 = Time(datetime.now()) - 9 * u.d
    end_utc_2 = Time(datetime.now()) - 9 * u.d

    data["start_UTC"] = [start_utc, start_utc_2]
    data["peak_UTC"] = [peak_utc, peak_utc_2]
    data["end_UTC"] = [end_utc, end_utc_2]
    data["counts"] = 100 * u.s

    data["duration"] = 10 * u.s
    data["lc_peak"] = [u.Quantity(range(1, 10))]

    control = QTable()
    month = datetime(2025, 1, 1, 0, 0, 0, 0)
    p_new = FlarelistSDC(data=data, month=month, control=control, energy=None)

    roll, solo_xyz, pointing = get_hpc_info(start_utc, end_utc)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=peak_utc, representation_type="cartesian")
    center_hpc = SkyCoord(0 * u.deg, 0 * u.deg, frame=Helioprojective(obstime=peak_utc, observer=solo))
    coord_stix = center_hpc.transform_to(STIXImaging(obstime=start_utc, obstime_end=end_utc, observer=solo))

    coord_hp = coord_stix.transform_to(Helioprojective(observer=solo))

    roll, solo_xyz, pointing = get_hpc_info(start_utc_2, end_utc_2)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=peak_utc_2, representation_type="cartesian")
    center_hpc = SkyCoord(0 * u.deg, 0 * u.deg, frame=Helioprojective(obstime=peak_utc_2, observer=solo))
    coord_stix_2 = center_hpc.transform_to(STIXImaging(obstime=start_utc_2, obstime_end=end_utc_2, observer=solo))

    coord_hp_2 = coord_stix_2.transform_to(Helioprojective(observer=solo))

    data["flare_location_stix"] = [coord_stix, coord_stix]
    data["flare_location_hp"] = [coord_hp, coord_hp_2]

    f = l3_fits_writer.write_fits(p_new)

    p_in = Product(f[0])

    print("all done")

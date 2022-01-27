import tempfile
from pathlib import Path
from collections import defaultdict

from astropy.io import fits

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.products.product import Product

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")

collector = defaultdict(lambda: defaultdict(list))
files = [
      # L0
           # science
    "L0/21/6/20/solo_L0_stix-sci-xray-rpd_0678187309f00000-0678187429f00000_V01_2106280011-54760.fits", # noqa
    "L0/21/6/21/solo_L0_stix-sci-xray-cpd_0688454771f32768-0688455372f13108_V01_2110250007-65280.fits", # noqa
    "L0/21/6/22/solo_L0_stix-sci-xray-spd_0678187309f00000-0678187429f58982_V01_2106280006-54720.fits", # noqa
    "L0/21/6/23/solo_L0_stix-sci-xray-vis_0678187308f65535-0678187429f58982_V01_2106280004-54716.fits", # noqa
    "L0/21/6/42/solo_L0_stix-sci-aspect-burst_0687412111f52953-0687412634f03145_V01_0.fits", # noqa
    "L0/21/6/24/solo_L0_stix-sci-xray-spec_0689786926f13107-0689801914f19660_V01_2111090002-50819.fits", # noqa
    # QL
    "L0/21/6/31/solo_L0_stix-ql-background_0668822400_V01.fits",
    "L0/21/6/34/solo_L0_stix-ql-flareflag_0684547200_V01.fits",
    "L0/21/6/30/solo_L0_stix-ql-lightcurve_0684892800_V01.fits",
    "L0/21/6/33/solo_L0_stix-ql-variance_0687484800_V01.fits",
    "L0/21/6/32/solo_L0_stix-ql-spectra_0680400000_V01.fits",
    "L0/21/6/41/solo_L0_stix-cal-energy_0683856000_V01.fits",
    # HK
    "L0/3/25/2/solo_L0_stix-hk-maxi_0647913600_V01.fits",
    "L0/3/25/1/solo_L0_stix-hk-mini_0643507200_V01.fits"]

remote = ["http://pub099.cs.technik.fhnw.ch/data/fits_test/" + x for x in files]
# files = ["/home/shane/fits_test/" + x for x in files]
files = [("/home/shane/fits_test_latest/" + x, remote[i]) for i, x in enumerate(files)]

with tempfile.TemporaryDirectory() as tempdir:
    temppath = Path(tempdir)
    for f, rm in files:
        try:
            pr = Product(f)
            prLB = Product(pr.parent)
            prlb = Product(Path("/home/shane/fits_test_latest/LB/21/6/20/"))
            # / header.get("parent"))

            header = fits.getheader(f)
            collector[pr.level][pr.type].append((pr, header))
        except Exception as e:
            print(e)

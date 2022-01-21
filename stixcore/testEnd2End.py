import tempfile
from pathlib import Path
from collections import defaultdict

from stixcore.data.test import test_data
from stixcore.idb.manager import IDBManager
from stixcore.products.product import Product

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")

collector = defaultdict(lambda: defaultdict(list))
files = [
    # L1
    # science
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-rpd_20210628T092301_20210628T092501_V01_2106280010-54759.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-cpd_20210628T190505_20210628T191500_V01_2106280009-54755.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-scpd_20210628T092301_20210628T092502_V01_2106280006-54720.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-vis_20210628T092301_20210628T092502_V01_2106280004-54716.fits", # noqa
    "L1/2021/10/13/SCI/solo_L1_stix-sci-aspect-burst_20211013T034959_20211013T035842_V01_0.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-spec_20210628T230112_20210628T234143_V01_2106280041-54988.fits", # noqa
    # QL
    "L1/2020/06/16/QL/solo_L1_stix-ql-background_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-flareflag_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-lightcurve_20200616_V01.fits",
    "L1/2020/06/16/QL/solo_L1_stix-ql-variance_20200616_V01.fits",
    "L1/2021/11/16/QL/solo_L1_stix-ql-spectra_20211116_V01.fits",
    "L1/2021/11/16/CAL/solo_L1_stix-cal-energy_20211116_V01.fits",
    # HK
    "L1/2020/06/16/HK/solo_L1_stix-hk-maxi_20200616_V01.fits",
    "L1/2021/09/20/HK/solo_L1_stix-hk-mini_20210920_V01.fits"]

remote = ["http://pub099.cs.technik.fhnw.ch/data/fits_test/" + x for x in files]
# files = ["/home/shane/fits_test/" + x for x in files]
files = [("/home/shane/fits_test_latest/" + x, remote[i]) for i, x in enumerate(files)]

with tempfile.TemporaryDirectory() as tempdir:
    temppath = Path(tempdir)
    for f, rm in files:
        try:
            pr = Product(f)
            collector[pr.level][pr.type].append(pr)
        except Exception as e:
            print(e)

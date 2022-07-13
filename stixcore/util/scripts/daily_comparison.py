import os
import pprint
from pathlib import Path

import numpy as np

from astropy.io import ascii
from astropy.table import QTable

from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.products.product import Product
from stixcore.time.datetime import SCETime

if __name__ == '__main__':
    pathL0 = Path('/data/stix/out/test/bsdfull/L0')
    pathLB = Path('/data/stix/out/test/bsdfull/LB')

    for f in pathL0.rglob("solo*.*"):
        name = f.name
        name = name.replace('solo_L0_', 'solo_LB_')
        parts = name.split("_")
        parts[2] = "*"
        if "-" in parts[3]:
            parts[3] = "*"
        name = "_".join(parts)

        target = pathLB
        parent = f.parent
        subd = []
        while parent != pathL0:
            subd.insert(0, parent.name)
            parent = parent.parent
        for s in subd:
            target = target / s
        found = len(list(target.rglob(name)))
        if found == 0:
            print(f"{f} >> {target} .. {name}")

if __name__ == '__main__2':

    _spm = SpiceKernelManager(Path("/data/stix/spice/git/solar-orbiter/kernels"))
    spicemeta = _spm.get_latest_mk(top_n=30)

    Spice.instance = Spice(spicemeta)
    print(f"Spice kernel @: {Spice.instance.meta_kernel_path}")

    files = [Path('/data/stix/out/test/scetime/L0/21/6/21/solo_L0_stix-sci-xray-cpd_0701954970-0701955082_V01_2203303955-64100.fits'),  # noqa
             Path('/data/stix/out/test/scetime/L0/21/6/21/solo_L0_stix-sci-xray-cpd_0701954970-0701955082_V01_2203303955-64537.fits'),  # noqa
             Path('/data/stix/out/test/scetime/L0/21/6/21/solo_L0_stix-sci-xray-cpd_0701954970-0701955082_V01_2203303955-64713.fits')]  # noqa

    for f in files:
        pprint.pprint(str(f))
        p0 = Product(f)
        pprint.pprint(p0.find_parent_files('/data/stix/out/test/scetime/'))
        for pp in p0.find_parent_products('/data/stix/out/test/scetime/'):
            pprint.pprint(pp.parent)

    pn = Product(Path('/data/stix/out/test/scetime/L0/21/6/21/solo_L0_stix-sci-xray-cpd_0701954970-0701955082_V01_2203303955-64100.fits'))   # noqa

    delta = (pn.data["time"][1:] - pn.data["time"][:-1]).as_float().value
    pprint.pprint(delta)
    print(delta.mean())
    print(delta[delta > 4])
    print(delta[delta < 4])

    (delta[delta > 4] - 4).sum() - (4-delta[delta < 4]).sum()

    pn.control.add_index('index')
    pn.control.loc[pn.data[:-1][delta < 63]["control_index"]].pprint_all()
    pn.find_parent_products('/data/stix/out/test/scetime/')[0].parent

    print("all done")


if __name__ == '__main__2':

    dir = Path('/data/stix/out/test/scetime/L0')
    products = ['ql-variance', 'ql-spectra', 'ql-background', 'ql-lightcurve', 'ql-flareflag',
                'cal-energy', 'tmstatusflarelist', 'stix-hk-maxi']

    time = SCETime(coarse=0)
    data = QTable(np.array([time, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, 0, ""]),
                  names=("time", "data_start", "data_start_fine", "data_end", "data_end_fine",
                         "duration", "day", "fcoarse", "fday", "product", "size", "product_entry",
                         "control_entry", "filename"))

    for product in products:
        for f in dir.rglob(f"*{product}*.fits"):
            print(f"{len(data)}: {f}")
            file_stats = os.stat(f)
            p = Product(f)
            c = int(f.name.split("_")[3])
            time = SCETime(coarse=c)
            data.add_row([p.scet_timerange.start, int(p.scet_timerange.start.coarse),
                          int(p.scet_timerange.start.fine),
                          int(p.scet_timerange.end.coarse), int(p.scet_timerange.end.fine),
                          p.scet_timerange.duration().value,
                          p.scet_timerange.start.get_scedays(), c, time.get_scedays(),
                          product, file_stats.st_size, len(p.data), len(p.control), str(f)])

    del data[0]
    data["new"] = 1
    ascii.write(data, 'data.new.csv', format='csv', overwrite=True, fast_writer=False)

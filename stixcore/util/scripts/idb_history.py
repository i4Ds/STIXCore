"""Script to read the IDB/ASW from the HK tm data and format it
into a json string that can be used to update the idbVersionHistory.json file.

Returns
-------
None: needs manual copy of the output
"""

import glob
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.table.table import QTable

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.time import SCETime

if __name__ == '__main__':
    idbs = {
        167:  "2.26.31",
        174: "2.26.32",
        178: "2.26.33",
        179: "2.26.34",
        181: "2.26.35",
        183: "2.26.36"
    }

    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(_spm.get_latest_mk())

    fitsdir = Path("/data/stix/out/test/bsdfull3")  # Path(CONFIG.get('Paths', 'fits_archive')

    def extract_time_swver(file):
        print(file)
        hdul = fits.open(file)
        times = (SCETime.from_float(hdul[0].header.get('OBT_BEG') * u.s)
                 + (hdul[2].data['time'] * u.cs))
        sw_ver = hdul[2].data['sw_version_number']
        idpu_identifier = [0 if idpu == "Nominal" else 1
                           for idpu in hdul[2].data['idpu_identifier']]
        return times, sw_ver, idpu_identifier

    files = [Path(f) for f in glob.glob(str(fitsdir / "L1/**/solo_L1_stix-hk-maxi*.fits"),
                                        recursive=True)]

    with ProcessPoolExecutor() as exec:
        res = exec.map(extract_time_swver, files)
    times, ver, idpu = list(zip(*res))

    c = []
    f = []
    for t in times:
        c.extend(t.coarse)
        f.extend(t.fine)

    data = np.column_stack((c, f, np.hstack(ver), np.hstack(idpu)))

    table = QTable(data, names=['coarse', 'fine', 'version', 'idpu'])

    table = table[table['idpu'] == 0]
    table.sort('coarse')

    change_indices = np.argwhere(table['version'][1:] != table['version'][:-1])

    # plt.clf()
    # plt.plot(table['coarse'], table['version'],  label='SW Versions')
    # plt.savefig("idb_asw.out.png")
    table = table[change_indices+1]

    periods = []

    start_time = SCETime.min_time()

    for i, row in enumerate(table):
        if i == 0:
            continue
        last = table[i-1]
        p = {}
        p["version"] = idbs[last['version'][0]]
        p["aswVersion"] = int(last['version'][0])
        end_time = SCETime(row['coarse'][0]-1, 0)
        p["validityPeriodUTC"] = [start_time.to_datetime().isoformat(timespec='milliseconds'),
                                  end_time.to_datetime().isoformat(timespec='milliseconds')],
        p["validityPeriodOBT"] = [{"coarse": int(start_time.coarse), "fine": int(start_time.fine)},
                                  {"coarse": int(end_time.coarse), "fine": int(0)}]
        periods.append(p)
        start_time = end_time

    p = {}
    p["version"] = idbs[row['version'][0]]
    p["aswVersion"] = int(row['version'][0])
    end_time = SCETime.max_time()
    p["validityPeriodUTC"] = [start_time.to_datetime().isoformat(timespec='milliseconds'),
                              "2136-02-07T08:11:44.778+00:00"],
    p["validityPeriodOBT"] = [{"coarse": int(start_time.coarse), "fine": int(start_time.fine)},
                              {"coarse": int(end_time.coarse), "fine": int(0)}]
    periods.append(p)

    # copy that output into idbVersionHistory.json
    print(json.dumps(periods, indent=4))

from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt

from astropy.table.operations import vstack

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.products.product import Product
from stixcore.time.datetime import SCETime


def rand_jitter(arr):
    stdev = 0.01
    return arr + np.random.randn(len(arr)) * stdev


_spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
Spice.instance = Spice(_spm.get_latest_mk())
print(f"Spice kernel @: {Spice.instance.meta_kernel_path}")

dir = Path("/data/stix/out/test/nicky/LB/21/6/42/")

files = dir.rglob("*.fits")

all = None

for file in files:
    lb = Product(file)
    idx = (lb.control["data_length"] < 4000) & (lb.control["sequence_flag"] < 2)
    lb.control["time"] = SCETime(lb.control["scet_coarse"], lb.control["scet_fine"]).to_datetime()
    t = lb.control[["data_length", "sequence_flag", "time", "index"]][idx]
    # t["request"] = 0

    # packets, _ = GenericProduct.getLeveL0Packets(lb)

    # for row in t:
    #    i = np.where(lb.data['control_index'] == row['index'])

    if not all:
        all = t
    else:
        all = vstack((all, t))

all["event"] = 1
all["event"] = rand_jitter(all["event"])
all.pprint_all()

plt.plot(all["time"], all["event"], "ro", label="Aspect Gap Events")
plt.xticks(rotation=45)
plt.savefig("aspect_gaps.png")

print("all done")

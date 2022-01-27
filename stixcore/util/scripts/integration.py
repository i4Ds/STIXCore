from pathlib import Path
from collections import defaultdict

from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCManager
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.tmtc.packets import TMTC

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")

if __name__ == '__main__':

    spm = SpiceKernelManager(Path("D:/Projekte/Stix/STIXCore/" +
                                  "stixcore/data/test/ephemeris/spice/kernels"))
    Spice.instance = Spice(spm.get_latest_mk())

    socm = SOCManager(Path("d:/tm_parsing"))
    out_dir = Path("d:/tm_parsing/out")
    out_dir.mkdir(parents=True, exist_ok=True)

    l0 = Level0(out_dir / 'LB', out_dir)

    files_to_process = list(socm.get_files(TMTC.TM))
    lbfiles = process_tmtc_to_levelbinary(files_to_process, out_dir)
    l0files = l0.process_fits_files(files=lbfiles)

    print("DONE")

from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np

import stixcore.processing.decompression as decompression
import stixcore.processing.engineering as engineering
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCManager
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products.product import Product
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import TMTC, GenericTMPacket, PacketSequence

name_counter = defaultdict(int)

IDB = IDBManager(test_data.idb.DIR).get_idb("2.26.34")

if __name__ == '__main__':

    plt.ion()
    # spm = SpiceKernelManager(Path("d:/tm_parsing/spice/kernels"))
    spm = SpiceKernelManager(test_data.ephemeris.KERNELS_DIR)
    Spice.instance = Spice(spm.get_latest_mk())

    data = '0da4c0090066100319000000000000000212000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b788014000000000ffffffff000000000000000000000000000000000000000000000000000000001f114cffffff' # noqa:
    packet = GenericTMPacket('0x' + data)
    # packet.export(descr=True)

    lb_cpd = Product(Path("d:/tm_parsing/solo_LB_stix-21-6-21_0694742400_V01.fits"))
    l0_cpd = Product(Path("d:/tm_parsing/solo_L0_stix-sci-xray-cpd_0691989251f32768-0691990834f32767_V01_2112050018-51807.fits"))  # noqa:
    l1_cpd = Product(Path("d:/tm_parsing/solo_L1_stix-sci-xray-cpd_20211205T031547-20211205T034210_V01_2112050018-51807.fits"))  # noqa:

    # lb_sp = Product(Path("d:/tm_parsing/solo_LB_stix-21-6-24_0693360000_V01.fits"))
    # l0_sp = Product(Path("d:/tm_parsing/solo_L0_stix-sci-xray-spec_0691988051f32768-0691993421f00000_V01_2112050008-51555.fits"))  # noqa:
    # l1_sp = Product(Path("d:/tm_parsing/solo_L1_stix-sci-xray-spec_20211205T025547-20211205T042516_V01_2112050008-51555.fits"))  # noqa:

    next_o = l0_cpd.data['time'] + l0_cpd.data['timedel']
    dis = next_o[0:-1] - l0_cpd.data['time'][1:]
    plt.step(range(0, len(l0_cpd.data['time'])-1), dis.as_float())
    plt.draw()

    packets = [Packet(d) for d in lb_cpd.data[48:51]['data']]

    for i, packet in enumerate(packets):
        decompression.decompress(packet)
        engineering.raw_to_engineering(packet)

    # packets = [p for p in packets if p.data.NIX00037.value == 2112050018]

    packets_s = PacketSequence(packets)

    packets[2].export(descr=True)

    start_raw = packets_s.get_value("NIX00404", attr="value")
    duration_raw = packets_s.get_value("NIX00405", attr="value")

    start_eng = packets_s.get_value("NIX00404")
    duration_eng = packets_s.get_value("NIX00405")

    u, idx = np.unique(start_raw, return_index=True)

    start_raw = start_raw[idx]
    duration_raw = duration_raw[idx]
    start_eng = start_eng[idx]
    duration_eng = duration_eng[idx]

    offsets_eng = ((start_eng + duration_eng)[0:-1] - start_eng[1:]).value
    offsets_raw = ((start_raw + duration_raw)[0:-1] - start_raw[1:])

    total_eng = abs(offsets_eng).sum()
    total_raw = abs(offsets_raw).sum()
    outdir = Path("d:/tm_parsing")
    l0 = Level0(outdir / 'LB', outdir)
    res = l0.process_fits_files(files=[Path("d:/tm_parsing/solo_LB_stix-21-6-21_0694742400_V01.fits")])  # noqa:

    socm = SOCManager(Path("d:/tm_parsing"))
    out_dir = Path("d:/tm_parsing/out")
    out_dir.mkdir(parents=True, exist_ok=True)

    l0 = Level0(out_dir / 'LB', out_dir)

    files_to_process = list(socm.get_files(TMTC.TM))
    lbfiles = process_tmtc_to_levelbinary(files_to_process, out_dir)
    l0files = l0.process_fits_files(files=lbfiles)

    plt.show()
    print("DONE")

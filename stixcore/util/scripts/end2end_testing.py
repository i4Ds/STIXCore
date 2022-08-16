import os
import sys
import shutil
from pathlib import Path
from xml.etree import ElementTree as Et
from collections import defaultdict

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.io.soc.manager import SOCManager
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products.level0.scienceL0 import ScienceProduct
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.tmtc.packets import TMTC

import pytest  # noqa


# import pytest  # noqa
# this is needed to keep a reference to pytest "alive" as isort/flake8 will remove it otherwise
# but this script assumes that the test config is loaded


END_TO_END_TEST_FILES = [
    # L1
    # science
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-rpd_20210628T092300-20210628T092500_V01_2106280010-54759.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-cpd_20210628T231111-20210628T233142_V01_2106280103-60638.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-scpd_20210628T092300-20210628T092501_V01_2106280005-54719.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-vis_20210628T092300-20210628T092501_V01_2106280004-54716.fits", # noqa
    "L1/2021/09/13/SCI/solo_L1_stix-sci-aspect-burst_20210913T055758-20210914T095357_V01_0.fits", # noqa
    "L1/2021/06/28/SCI/solo_L1_stix-sci-xray-spec_20210628T230111-20210628T234142_V01_2106280041-54988.fits", # noqa
    # QL
    "L1/2021/06/28/QL/solo_L1_stix-ql-background_20210628_V01.fits",
    "L1/2021/06/28/QL/solo_L1_stix-ql-flareflag_20210628_V01.fits",
    "L1/2021/06/28/QL/solo_L1_stix-ql-lightcurve_20210628_V01.fits",
    "L1/2021/06/28/QL/solo_L1_stix-ql-ql-tmstatusflarelist_20210628_V01.fits",
    "L1/2021/06/28/QL/solo_L1_stix-ql-spectra_20210628_V01.fits",
    "L1/2021/06/28/QL/solo_L1_stix-ql-variance_20210628_V01.fits",
    "L1/2021/06/28/CAL/solo_L1_stix-cal-energy_20210628_V01.fits",
    # HK
    "L1/2021/09/20/HK/solo_L1_stix-hk-mini_20210920_V01.fits",
    "L1/2021/06/28/HK/solo_L1_stix-hk-maxi_20210628_V01.fits"]

remote = ["http://pub099.cs.technik.fhnw.ch/data/fits_test/" + x for x in END_TO_END_TEST_FILES]
# files = ["/home/shane/fits_test/" + x for x in files]
files = [("/data/stix/end2end/fits/" + x, remote[i])
         for i, x in enumerate(END_TO_END_TEST_FILES)]


def _pretty_xml_print(current, parent=None, index=-1, depth=0):
    for i, node in enumerate(current):
        _pretty_xml_print(node, current, i, depth + 1)
    if parent is not None:
        if index == 0:
            parent.text = '\n' + ('\t' * depth)
        else:
            parent[index - 1].tail = '\n' + ('\t' * depth)
        if index == len(parent) - 1:
            current.tail = '\n' + ('\t' * (depth - 1))


def rebuild_end2end(files, *, splits=3,
                    socdir=Path(CONFIG.get("Paths", "tm_archive")),
                    outdir=test_data.products.DIREND2END):
    splitfiles = list(range(0, splits))
    packetid = 0
    rootxml = None

    print(f"rebuild_end2end output dir: {outdir}")

    outdir.mkdir(parents=True, exist_ok=True)

    newroot = [Et.Element('Response') for i in splitfiles]
    newresponse = [Et.SubElement(newroot[i], 'PktRawResponse') for i in splitfiles]

    xmlfiles = defaultdict(set)

    for f, u in files:
        l1_f = Path(f)
        print(f"{l1_f}: {l1_f.exists()}")
        l1_p = Product(l1_f)
        rootdir = l1_f.parent.parent.parent.parent.parent.parent

        for l0_p in l1_p.find_parent_products(rootdir):
            if isinstance(l0_p, ScienceProduct):
                for prow in l0_p.control:
                    xmlfiles[str(prow['raw_file'][0].decode())].update(prow['packet'])
            else:
                parents = l0_p.find_parent_products(rootdir)
                for row in l0_p.control:
                    for p in parents:
                        packet = p.control[p.control['packet'] == row["packet"]]
                        for prow in packet:
                            xmlfiles[prow['raw_file']].add(row["packet"])

    print(f"{xmlfiles.keys()} TM files detected")

    for xmlfile, packet_ids in xmlfiles.items():
        rootxml = Et.parse(socdir / xmlfile).getroot()
        for packet in rootxml.iter('PktRawResponseElement'):
            orig_pid = int(packet.get("packetID"))
            if orig_pid in packet_ids:
                packetid += 1
                rpa = Et.SubElement(newresponse[packetid % splits], "PktRawResponseElement")
                rpa.set("packetID", str(packetid))
                pa = Et.SubElement(rpa, "packet")
                pa.text = list(packet)[0].text

    print(f"{packetid} packets recompiled")

    for i, root in enumerate(newroot):
        with open(outdir / f'Test{i}.PktTmRaw.xml', 'wb') as xmlf:
            _pretty_xml_print(root)
            tree = Et.ElementTree(root)
            tree.write(xmlf, encoding='utf-8')

    fitsdir = outdir / "fits"
    if fitsdir.exists():
        shutil.rmtree(str(fitsdir))
    print(f"saved: xml files {len(newroot)}")

    fitsfiles = end2end_pipeline(outdir, fitsdir)
    for f in fitsfiles:
        print(f)
    return fitsfiles


def end2end_pipeline(indir, fitsdir):
    _spm = SpiceKernelManager(test_data.ephemeris.KERNELS_DIR)
    Spice.instance = Spice(_spm.get_latest_mk())
    print(f"Spice kernel @: {Spice.instance.meta_kernel_path}")

    SOOPManager.instance = SOOPManager(test_data.soop.DIR)

    soc = SOCManager(indir)
    lb_files = process_tmtc_to_levelbinary(soc.get_files(TMTC.TM), archive_path=fitsdir)
    l0_proc = Level0(indir, fitsdir)
    l0_files = l0_proc.process_fits_files(files=lb_files)
    l1_proc = Level1(indir, fitsdir)
    l1_files = l1_proc.process_fits_files(files=l0_files)

    allfiles = list(lb_files)
    allfiles.extend(l0_files)
    allfiles.extend(l1_files)
    return allfiles


if __name__ == '__main__2':
    if len(sys.argv) > 2:
        zippath = Path(sys.argv[1])
        datapath = Path(sys.argv[2])

        rebuild_end2end(files, splits=1, outdir=datapath,
                        socdir=Path("/data/stix/SOLSOC/from_edds/tm/"))

        if zippath.parent.exists() and datapath.exists():
            zipcmd = f"zip -FSrj {str(zippath)} {str(datapath)}"
            print(zipcmd)
            copyout = os.popen(zipcmd).read()
            print(copyout)
            shutil.rmtree(datapath)
            exit()
    print("No zip")

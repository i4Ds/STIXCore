import io
import re
import glob
from pathlib import Path
from binascii import unhexlify
from unittest.mock import patch

import numpy as np
import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice
from stixcore.ephemeris.manager import SpiceKernelManager
from stixcore.idb.idb import IDBPolynomialCalibration
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCManager
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.L1toL2 import Level2
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products.level0.quicklookL0 import LightCurve
from stixcore.products.product import Product
from stixcore.tmtc.packets import TMTC, GenericTMPacket
from stixcore.soop.manager import SOOPManager
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


@pytest.fixture
def soc_manager():
    return SOCManager(Path(__file__).parent.parent.parent / 'data' / 'test' / 'io' / 'soc')


@pytest.fixture
def spicekernelmanager():
    return SpiceKernelManager(test_data.ephemeris.KERNELS_DIR)


@pytest.fixture
def idb():
    return IDBManager(test_data.idb.DIR).get_idb("2.26.34")


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path


@pytest.fixture
def packet():
    data = '0da4c0090066100319000000000000000212000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b788014000000000ffffffff000000000000000000000000000000000000000000000000000000001f114cffffff' # noqa:
    packet = GenericTMPacket('0x' + data)
    return packet


@pytest.mark.skip(reason="will be replaces with end2end test soon")
def test_level_b(soc_manager, out_dir):
    files_to_process = list(soc_manager.get_files(TMTC.TM))
    res = process_tmtc_to_levelbinary(files_to_process=files_to_process[0:1], archive_path=out_dir)
    assert len(res) == 1
    fits = res.pop()
    diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                    ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
    if not diff.identical:
        print(diff.report())
    assert diff.identical


@pytest.mark.skip(reason="will be replaces with end2end test soon")
def test_level_0(out_dir):
    lb = test_data.products.LB_21_6_30_fits
    l0 = Level0(out_dir / 'LB', out_dir)
    res = l0.process_fits_files(files=[lb])
    assert len(res) == 2
    for fits in res:
        diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
        if not diff.identical:
            print(diff.report())
        assert diff.identical


@pytest.mark.skip(reason="will be replaces with end2end test soon")
def test_level_1(out_dir):
    SOOPManager.instance = SOOPManager(Path(__file__).parent.parent.parent
                                       / 'data' / 'test' / 'soop')

    l0 = test_data.products.L0_LightCurve_fits
    l1 = Level1(out_dir / 'LB', out_dir)
    res = sorted(l1.process_fits_files(files=l0))
    assert len(res) == 2

    # test for https://github.com/i4Ds/STIXCore/issues/180
    # TODO remove when solved
    lc1 = Product(res[0])
    lc2 = Product(res[1])
    t = np.hstack((np.array(lc1.data['time']), (np.array(lc2.data['time']))))
    td = np.hstack((np.array(lc1.data['timedel']), (np.array(lc2.data['timedel']))))
    range(len(lc1.data['time'])-3, len(lc1.data['time'])+3)
    assert np.all((t[1:] - t[0:-1]) == td[0:-1])
    # end test for https://github.com/i4Ds/STIXCore/issues/180

    for fits in res:
        diff = FITSDiff(test_data.products.DIR / fits.name, fits,
                        ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
        if not diff.identical:
            print(diff.report())
        assert diff.identical


@pytest.mark.skip(reason="needs proper spize pointing kernels")
def test_level_2(out_dir, spicekernelmanager):
    SOOPManager.instance = SOOPManager(Path(__file__).parent.parent.parent
                                       / 'data' / 'test' / 'soop')

    idlfiles = 4 if CONFIG.getboolean("IDLBridge", "enabled", fallback=False) else 0

    l1 = test_data.products.L1_fits
    l2 = Level2(out_dir / 'L1', out_dir)
    res = l2.process_fits_files(files=l1)
    assert len(res) == len(test_data.products.L1_fits) + idlfiles
    input_names = [f.name for f in l1]
    for ffile in res:
        pl2 = Product(ffile)
        assert pl2.level == 'L2'
        assert pl2.parent[0] in input_names


@pytest.mark.skip(reason="needs proper spize pointing kernels")
def test_level_2_auxiliary(out_dir, spicekernelmanager):
    SOOPManager.instance = SOOPManager(Path(__file__).parent.parent.parent
                                       / 'data' / 'test' / 'soop')
    l1 = [p for p in test_data.products.L1_fits if p.name.startswith('solo_L1_stix-hk-maxi')]

    l2 = Level2(out_dir / 'L1', out_dir)
    res = l2.process_fits_files(files=l1)
    print(res)
    assert len(res) == len(l1) * (2 if CONFIG.getboolean("IDLBridge", "enabled", fallback=False)
                                  else 1)


def test_get_calibration_polynomial(idb):
    poly = idb.get_calibration_polynomial('CIX00036TM')
    assert isinstance(poly, IDBPolynomialCalibration)
    assert poly(1) == poly.A[1]
    assert poly.valid is True

    assert (poly(np.array([1, 2, 3])) == np.array([poly.A[1], poly.A[1] * 2, poly.A[1] * 3])).all()
    assert poly([1, 2, 3]) == [poly.A[1], poly.A[1] * 2, poly.A[1] * 3]


@patch('stixcore.io.soc.manager.SOCPacketFile')
def test_pipeline(socpacketfile, out_dir):

    l0_proc = Level0(out_dir / 'LB', out_dir)
    l1_proc = Level1(out_dir / 'LB', out_dir)

    all = True
    report = dict()

    exclude = ['__doc__', 'TM_DIR',
               # the following TMs have invalid times: year 2086
               'TM_1_2_48000', 'TM_236_19', 'TM_237_12',
               'TM_239_14', 'TM_5_4_54304', 'TM_6_6_53250']
    # singletest = ['TM_21_6_42_complete']

    for pid, fkey in enumerate([k for k in test_data.tmtc.__dict__.keys()
                                if ((k not in exclude)
                                    and not (k.startswith('TM_21_6_')
                                             and not k.endswith('_complete')))]):
        # for pid, fkey in enumerate([k for k in test_data.tmtc.__dict__.keys()
        #                            if k in singletest]):
        hex_file = test_data.tmtc.__dict__[fkey]

        try:
            with hex_file.open('r') as file:
                hex = file.readlines()

            socpacketfile.get_packet_binaries.return_value = list(
                [(pid*1000 + i, unhexlify(re.sub(r"\s+", "", h))) for i, h in enumerate(hex)])
            socpacketfile.file = hex_file

            lb_files = process_tmtc_to_levelbinary([socpacketfile], archive_path=out_dir)
            assert len(lb_files) > 0

            l0_files = l0_proc.process_fits_files(files=lb_files)
            assert len(l0_files) > 0

            l1_files = l1_proc.process_fits_files(files=l0_files)
            assert len(l1_files) > 0

            print(f"OK {fkey}: {l1_files}")
        except Exception as e:
            report[fkey] = e
            all = False

    if not all:
        for key, error in report.items():
            logger.error(f"Error while processing TM file: {key}")
            logger.error(error)
        raise ValueError("Pipline Test went wrong")


def test_export_single(packet):
    p = packet.export(descr=True)
    assert isinstance(p, dict)
    assert p['spice_kernel'] == Spice.instance.meta_kernel_path.name


def test_export_all():
    lb = Product(test_data.products.LB_21_6_30_fits)
    l0 = LightCurve.from_levelb(lb, parent="raw.xml")

    assert hasattr(l0, "packets")
    assert len(l0.packets.packets) > 0
    for packet in l0.packets.packets:
        p = packet.export(descr=True)
        assert isinstance(p, dict)
        assert p['spice_kernel'] == Spice.instance.meta_kernel_path.name


def test_print(packet):
    ms = io.StringIO()
    packet.print(descr=True, stream=ms)
    ms.seek(0)
    assert len(ms.read()) > 100


if __name__ == '__main__':
    '''TO BE REMOVED

    currently used to start the AUX processing on an existing L1 fits dir
    '''
    _spm = SpiceKernelManager(Path("/data/stix/spice/kernels/"))
    Spice.instance = Spice(_spm.get_latest_mk())
    print(Spice.instance.meta_kernel_path)

    out_dir_main = Path("/home/nicky/fits_20220510")

    SOOPManager.instance = SOOPManager(Path("/data/stix/SOLSOC/from_soc"))

    l1 = [Path('/home/shane/fits_20220321/L1/2020/06/07/HK/solo_L1_stix-hk-maxi_20200607_V01.fits'),
          Path('/home/shane/fits_20220321/L1/2021/08/26/HK/solo_L1_stix-hk-maxi_20210826_V01.fits'),
          Path('/home/shane/fits_20220321/L1/2021/08/28/HK/solo_L1_stix-hk-maxi_20210828_V01.fits'),
          Path('/home/shane/fits_20220321/L1/2021/09/23/HK/solo_L1_stix-hk-maxi_20210923_V01.fits'),
          Path('/home/shane/fits_20220321/L1/2021/10/09/HK/solo_L1_stix-hk-maxi_20211009_V01.fits')
          ]

    # glob.glob("/home/shane/fits_20220321/L1/2022/01/0*/**/solo_L1_stix-hk-maxi*.fits", recursive=True)]  # noqa
    # glob.glob("/home/shane/fits_20220321/L1/**/solo_L1_stix-hk-maxi*.fits", recursive=True)]

    l1 = [Path(f) for f in
          glob.glob("/home/shane/fits_20220321/L1/**/solo_L1_stix-hk-maxi*.fits", recursive=True)]

    l2 = Level2(out_dir / 'L1', out_dir_main)
    res = l2.process_fits_files(files=l1)

    print("DONE")

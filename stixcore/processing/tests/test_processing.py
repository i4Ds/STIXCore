import io
import re
import glob
import smtplib
from pathlib import Path
from binascii import unhexlify
from unittest.mock import patch

import numpy as np
import pytest

from astropy.io.fits.diff import FITSDiff

from stixcore.config.config import CONFIG
from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.idb.idb import IDBPolynomialCalibration
from stixcore.idb.manager import IDBManager
from stixcore.io.soc.manager import SOCManager, SOCPacketFile
from stixcore.processing.L0toL1 import Level1
from stixcore.processing.L1toL2 import Level2
from stixcore.processing.LBtoL0 import Level0
from stixcore.processing.pipeline import PipelineStatus, process_tm
from stixcore.processing.TMTCtoLB import process_tmtc_to_levelbinary
from stixcore.products.level0.quicklookL0 import LightCurve
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager
from stixcore.tmtc.packets import TMTC, GenericTMPacket
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
    res = l1.process_fits_files(files=l0)
    assert len(res) == 1
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

    exclude = ['__doc__', 'TM_DIR', 'XML_TM',
               # the following TMs have invalid times: year 2086
               'TM_1_2_48000', 'TM_236_19', 'TM_237_12',
               'TM_239_14', 'TM_5_4_54304', 'TM_6_6_53250']
    # TODO go on here
    # singletest = ['TM_21_6_42']

    for pid, fkey in enumerate([k for k in test_data.tmtc.__dict__.keys()
                                if ((k not in exclude)
                                    and not (k.startswith('TM_21_6_')
                                    and not k.endswith('_complete'))
                                    )]):
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
            # assert len(l0_files) > 0

            l1_files = l1_proc.process_fits_files(files=l0_files)
            # assert len(l1_files) > 0

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


def test_single_vs_batch(out_dir):
    CONTINUE_ON_ERROR = CONFIG.getboolean('Logging', 'stop_on_error', fallback=False)
    try:
        CONFIG.set('Logging', 'stop_on_error', str(False))

        tm_files = test_data.tmtc.XML_TM

        tm_files = [SOCPacketFile(f) for f in tm_files]

        # run all as batch
        oud_batch = out_dir / "batch"
        l0_proc_b = Level0(oud_batch, oud_batch)
        l1_proc_b = Level1(oud_batch, oud_batch)
        lb_files_b = process_tmtc_to_levelbinary(tm_files, archive_path=oud_batch)
        l0_files_b = l0_proc_b.process_fits_files(files=lb_files_b)
        l1_files_b = l1_proc_b.process_fits_files(files=l0_files_b)

        files_b = list(lb_files_b)
        files_b.extend(l0_files_b)
        files_b.extend(l1_files_b)
        files_b = sorted(list(set(files_b)), key=lambda f: f.name)

        # run step by step
        oud_single = out_dir / "single"
        l0_proc_s = Level0(oud_single, oud_single)
        l1_proc_s = Level1(oud_single, oud_single)

        files_s = []

        for tm_file in tm_files:
            tmfile = [tm_file]
            lb_files_s = process_tmtc_to_levelbinary(tmfile, archive_path=oud_single)
            l0_files_s = l0_proc_s.process_fits_files(files=lb_files_s)
            l1_files_s = l1_proc_s.process_fits_files(files=l0_files_s)
            files_s.extend(lb_files_s)
            files_s.extend(l0_files_s)
            files_s.extend(l1_files_s)

        files_s = sorted(list(set(files_s)), key=lambda f: f.name)

        assert len(files_s) == len(files_b)

        for i, f_b in enumerate(files_b):
            f_s = files_s[i]
            diff = FITSDiff(f_b, f_s, ignore_keywords=['CHECKSUM', 'DATASUM', 'DATE', 'VERS_SW'])
            assert diff.identical
    finally:
        CONFIG.set('Logging', 'stop_on_error', str(CONTINUE_ON_ERROR))


def test_pipeline_logging(spicekernelmanager, out_dir):

    CONTINUE_ON_ERROR = CONFIG.getboolean('Logging', 'stop_on_error', fallback=False)
    FITS_ARCHIVE = CONFIG.get('Paths', 'fits_archive')
    LOG_LEVEL = CONFIG.get('Pipeline', 'log_level')
    LOG_DIR = CONFIG.get('Pipeline', 'log_dir')
    try:
        CONFIG.set('Logging', 'stop_on_error', str(False))
        CONFIG.set('Paths', 'fits_archive', str(out_dir / "fits"))
        CONFIG.set('Pipeline', 'log_level', str('DEBUG'))
        CONFIG.set('Pipeline', 'log_dir', str(out_dir / "logging"))

        log_dir = Path(CONFIG.get('Pipeline', 'log_dir'))
        log_dir.mkdir(parents=True, exist_ok=True)

        PipelineStatus.instance = PipelineStatus(None)

        for f in test_data.tmtc.XML_TM:
            process_tm(f, spm=spicekernelmanager)

        assert len(list(log_dir.rglob("*.log"))) == 3
        assert len(list(log_dir.rglob("*.log.err"))) == 0
        assert len(list(log_dir.rglob("*.out"))) == 3
        # TODO increase if level2 for more products is available
        assert len(list(Path(CONFIG.get('Paths', 'fits_archive')).rglob("*.fits"))) == 15

    finally:
        CONFIG.set('Logging', 'stop_on_error', str(CONTINUE_ON_ERROR))
        CONFIG.set('Paths', 'fits_archive', str(FITS_ARCHIVE))
        CONFIG.set('Pipeline', 'log_level', str(LOG_LEVEL))
        CONFIG.set('Pipeline', 'log_dir', str(LOG_DIR))


@patch("smtplib.SMTP")
def test_mail(s):
    tm_file = "tm_file"
    err_file = "err_file"
    sender = CONFIG.get('Pipeline', 'error_mail_sender')
    receivers = CONFIG.get('Pipeline', 'error_mail_receivers').split(",")
    host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
    port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
    smtp_server = smtplib.SMTP(host=host, port=port)
    message = f"""Subject: StixCore TMTC Processing Error

Error while processing {tm_file}

login to pub099.cs.technik.fhnw.ch and check:

{err_file}

StixCore

==========================

do not answer to this mail.
"""
    try:
        st = smtp_server.sendmail(sender, receivers, message)
        assert len(st.keys()) == 0
    except Exception as e:
        print(e)


if __name__ == '__main__':
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

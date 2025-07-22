from unittest.mock import patch

import numpy as np
import pytest

import astropy.units as u
from astropy.io import fits
from astropy.table import QTable

from stixcore.data.test import test_data
from stixcore.io.fits.processors import FitsL0Processor, FitsL1Processor, FitsLBProcessor
from stixcore.products.product import Product, read_qtable
from stixcore.soop.manager import SOOPManager
from stixcore.time import SCETime, SCETimeRange
from stixcore.time.datetime import SCETimeDelta


@pytest.fixture
def soop_manager():
    return SOOPManager(test_data.soop.DIR)


def test_levelb_processor_init():
    pro = FitsLBProcessor('some/path')
    assert pro.archive_path == 'some/path'


def test_levelb_processor_generate_filename_with_rid():
    with patch('stixcore.products.level0.quicklookL0.QLProduct') as product:
        processor = FitsLBProcessor('some/path')
        product.control = QTable([[(123, 45678)]], names=['request_id'])
        product.service_type = 21
        product.service_subtype = 6
        product.ssid = 20
        product.obt_avg = SCETime(43200, 0)
        product.level = 'LB'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_LB_stix-21-6-20_0000000000-9999999999_V01_0000045678-00123.fits'


def test_levelb_processor_generate_filename_without_rid():
    with patch('stixcore.products.level0.quicklookL0.QLProduct') as product:
        processor = FitsLBProcessor('some/path')
        product.control = QTable([[False]], names=['request_id'])
        product.service_type = 21
        product.service_subtype = 6
        product.ssid = 20
        product.obt_avg = SCETime(43200, 0)
        product.level = 'LB'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_LB_stix-21-6-20_0000000000_V01.fits'


@patch('stixcore.products.level0.quicklookL0.QLProduct')
@patch('stixcore.io.fits.processors.datetime')
def test_levelb_processor_generate_primary_header(datetime, product):
    processor = FitsLBProcessor('some/path')
    datetime.now().isoformat.return_value = '1234-05-07T01:02:03.346'
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)

    dummy_control_data = {"scet_coarse": [beg.coarse, end.coarse],
                          "scet_fine": [beg.fine, end.fine]}

    product.control.__getitem__.side_effect = dummy_control_data.__getitem__
    product.control.colnames = ['scet_coarse', 'scet_fine']
    product.raw = ['packet1.xml', 'packet2.xml']
    product.parent = ['packet1.xml', 'packet2.xml']
    product.level = 'LB'
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3
    product.obt_beg = beg
    product.obt_end = end
    product.date_obs = beg
    product.date_beg = beg
    product.date_end = end

    test_data = {
        'FILENAME': 'a_filename.fits',
        'DATE': '1234-05-07T01:02:03.346',
        'OBT_BEG': beg.to_string(),
        'OBT_END': end.to_string(),
        'DATE_OBS': beg.to_string(),
        'DATE_BEG': beg.to_string(),
        'DATE_END': end.to_string(),
        'STYPE': product.service_type,
        'SSTYPE': product.service_subtype,
        'SSID': product.ssid,
        'TIMESYS': "OBT",
        'LEVEL': 'LB',
        'RAW_FILE': 'packet1.xml;packet2.xml',
        'PARENT': 'packet1.xml;packet2.xml'
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]


def test_level0_processor_init():
    pro = FitsL0Processor('some/path')
    assert pro.archive_path == 'some/path'


def test_level0_processor_generate_filename():
    with patch('stixcore.products.level0.quicklookL0.QLProduct') as product:
        processor = FitsL0Processor('some/path')
        product.control.colnames = []
        product.type = 'ql'
        product.scet_timerange = SCETimeRange(start=SCETime(0, 0), end=SCETime(1234, 1234))
        product.level = 'LB'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_LB_stix-ql-a-name_0000000000_V01.fits'

    with patch('stixcore.products.level0.scienceL0.ScienceProduct') as product:
        product.type = 'sci'
        product.control.colnames = []
        product.obs_avg.coarse = 0
        product.level = 'L0'
        product.name = 'a_name'
        product.scet_timerange = SCETimeRange(start=SCETime(12345, 6789), end=SCETime(98765, 4321))
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name_0000012345-0000098765_V01.fits'

        dummy_control_data = {'request_id': [123456], 'tc_packet_seq_control': [98765]}

        product.control.__getitem__.side_effect = dummy_control_data.__getitem__
        product.control.colnames = ['request_id']
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name' \
                           '_0000012345-0000098765_V01_0000123456.fits'

        product.control.colnames = ['request_id', 'tc_packet_seq_control']
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name' \
                           '_0000012345-0000098765_V01_0000123456-98765.fits'


@patch('stixcore.products.level0.quicklookL0.QLProduct')
@patch('stixcore.io.fits.processors.datetime')
def test_level0_processor_generate_primary_header(datetime, product):
    processor = FitsL0Processor('some/path')
    datetime.now().isoformat.return_value = '1234-05-07T01:02:03.346'
    product.obs_beg = SCETime(coarse=0, fine=0)
    product.obs_avg = SCETime(coarse=0, fine=2 ** 15)
    product.obs_end = SCETime(coarse=1, fine=2 ** 15)

    product.scet_timerange = SCETimeRange(start=product.obs_beg, end=product.obs_end)
    product.raw = ['packet1.xml', 'packet2.xml']
    product.parent = ['lb1.fits', 'lb2.fts']
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3
    product.level = 'L0'

    test_data = {
        'FILENAME': 'a_filename.fits',
        'DATE': '1234-05-07T01:02:03.346',
        'OBT_BEG': 0.0,
        'OBT_END': 1.5000076295109483,
        'DATE_OBS': '0000000000:00000',
        'DATE_BEG': '0000000000:00000',
        'DATE_AVG': '0000000000:49152',
        'DATE_END': '0000000001:32768',
        'STYPE': 1,
        'SSTYPE': 2,
        'SSID': 3,
        'TIMESYS': "OBT",
        'LEVEL': 'L0',
        'RAW_FILE': 'packet1.xml;packet2.xml',
        'PARENT': 'lb1.fits;lb2.fts'
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]


@pytest.mark.parametrize('p_file', [test_data.products.L0_LightCurve_fits[0],
                                    test_data.products.L1_LightCurve_fits[0]],
                         ids=["ql_l0", "ql_l1"])
def test_count_data_mixin(p_file):
    processor = FitsL0Processor('some/path')
    p = Product(p_file)
    assert p.dmin == p.data["counts"].min().value
    assert p.dmax == p.data["counts"].max().value
    assert p.exposure == p.data["timedel"].min().as_float().to_value()
    assert p.max_exposure == p.data["timedel"].max().as_float().to_value()

    test_data = {
        "DATAMAX": p.dmax,
        "DATAMIN": p.dmin,
        "XPOSURE": p.exposure,
        "XPOMAX": p.max_exposure,
        "BUNIT": "counts"
    }

    header = processor.generate_primary_header('a_filename.fits', p)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]


def test_level1_processor_init():
    pro = FitsL1Processor('some/path')
    assert pro.archive_path == 'some/path'


@patch('stixcore.products.level1.quicklookL1.QLProduct')
def test_level1_processor_generate_filename(product):
    processor = FitsL1Processor('some/path')
    product.control.colnames = []
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)
    avg = beg + (end - beg)/2
    product.obt_beg = beg
    product.obt_avg = avg
    product.obt_end = end
    product.obs_beg = beg.to_datetime()
    product.obs_avg = avg.to_datetime()
    product.obs_end = end.to_datetime()
    product.type = 'ql'
    product.scet_timerange = SCETimeRange(start=SCETime(0, 0),
                                          end=SCETime(coarse=0, fine=2**16-1))
    product.utc_timerange = product.scet_timerange.to_timerange()
    product.level = 'L1'
    product.name = 'a_name'
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_L1_stix-ql-a-name_20000101_V01.fits'
    product.type = 'sci'
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_L1_stix-sci-a-name_20000101T000000-20000101T000001_V01.fits'


@patch('stixcore.products.level1.quicklookL1.QLProduct')
def test_level1_processor_generate_primary_header(product, soop_manager):
    SOOPManager.instance = soop_manager
    processor = FitsL1Processor('some/path')
    beg = SCETime(coarse=683769519, fine=0)
    end = SCETime(coarse=beg.coarse+24 * 60 * 60)
    beg + (end - beg)/2
    product.scet_timerange = SCETimeRange(start=beg, end=end)
    product.utc_timerange = product.scet_timerange.to_timerange()
    product.raw = ['packet1.xml', 'packet2.xml']
    product.parent = ['l01.fits', 'l02.fts']
    product.level = 'L1'
    product.type = "ql"
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3

    test_data = {
        'FILENAME': 'a_filename.fits',
        'OBT_BEG': beg.as_float().value,
        'OBT_END': end.as_float().value,
        'DATE_OBS': product.utc_timerange.start.fits,
        'DATE_BEG': product.utc_timerange.start.fits,
        'DATE_AVG': product.utc_timerange.center.fits,
        'DATE_END': product.utc_timerange.end.fits,
        'STYPE': product.service_type,
        'SSTYPE': product.service_subtype,
        'SSID': product.ssid,
        'TIMESYS': 'UTC',
        'LEVEL': 'L1',
        'OBS_ID': 'SSTX_040A_000_000_5Md2_112;SSTX_040A_000_000_vFLg_11Y',
        'OBS_TYPE': '5Md2;vFLg',
        'OBS_MODE': 'STIX_ANALYSIS;STIX_BASIC',
        'SOOPNAME': 'none',
        'SOOPTYPE': 'none',
        'TARGET': 'none',
        'RSUN_ARC': 1589.33,
        'HGLT_OBS': -0.3190007305644162,
        'HGLN_OBS': -66.521984558927,
        'RAW_FILE': 'packet1.xml;packet2.xml',
        'PARENT': 'l01.fits;l02.fts'
    }

    header, o = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in (header+o):
        if name in test_data.keys():
            if isinstance(value, float):
                assert np.allclose(test_data[name], value)
            else:
                assert value == test_data[name]


def test_write_data_with_rel_time(tmp_path):
    processor = FitsL1Processor(tmp_path)
    p = Product(test_data.products.L1_LightCurve_fits[0])

    assert p.data['time'][0] == SCETime(coarse=664156752, fine=58990)
    assert p.data['timedel'][0] == SCETimeDelta(coarse=4, fine=0)
    assert p.scet_timerange.start == SCETime(coarse=664156750, fine=58990)

    f = processor.write_fits(p)

    p2 = Product(f[0])
    assert p2.data['time'][0] == SCETime(coarse=664156752, fine=58990)
    assert p2.data['timedel'][0] == SCETimeDelta(coarse=4, fine=0)
    assert p2.data['time'][-1] == SCETime(coarse=664227380, fine=58990)
    assert p2.data['timedel'][-1] == SCETimeDelta(coarse=4, fine=0)
    assert p2.scet_timerange.start == SCETime(coarse=664156750, fine=58990)

    hdul = fits.open(f[0])
    data = read_qtable(f[0], hdu="DATA", hdul=hdul)

    assert hdul["PRIMARY"].header['OBT_BEG'] == 664156750.9001297
    assert data['time'][0] == 200 * u.cs
    assert data['timedel'][0] == 400 * u.cs

    assert data['timedel'][-1] == 400 * u.cs
    assert len(data) == 17658
    assert data['time'][-1] == (17658 * 400 - 200) * u.cs
    assert hdul["PRIMARY"].header['OBT_END'] == (hdul["PRIMARY"].header['OBT_BEG']
                                                 + (17658 * 400 * u.cs).to_value(u.s))

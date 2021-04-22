from unittest.mock import patch

from astropy.time import Time as astrotime

from stixcore.datetime.datetime import SCETime
from stixcore.io.fits.processors import FitsL0Processor, FitsL1Processor, FitsLBProcessor


def test_levelb_processor_init():
    pro = FitsLBProcessor('some/path')
    assert pro.archive_path == 'some/path'


def test_levelb_processor_generate_filename():
    with patch('stixcore.products.level0.quicklook.QLProduct') as product:
        processor = FitsLBProcessor('some/path')
        product.control.colnames = []
        product.type = 'ql'
        product.obs_beg.coarse = 0
        product.level = 'LB'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_LB_stix-ql-a-name_0000000000_V01.fits'


@patch('stixcore.products.level0.quicklook.QLProduct')
@patch('stixcore.io.fits.processors.datetime')
def test_levelb_processor_generate_primary_header(datetime, product):
    processor = FitsLBProcessor('some/path')
    datetime.now().isoformat.return_value = '1234-05-07T01:02:03.346'
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)
    product.control = {"scet_coarse": [beg.coarse, end.coarse],
                       "scet_fine": [beg.fine, end.fine]}
    product.level = 'LB'
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3

    test_data = {
        'FILENAME': 'a_filename.fits',
        'DATE': '1234-05-07T01:02:03.346',
        'OBT_BEG': str(beg.as_float().value),
        'OBT_END': str(end.as_float().value),
        'DATE_OBS': str(beg.as_float().value),
        'DATE_BEG': str(beg.as_float().value),
        'DATE_END': str(end.as_float().value),
        'STYPE': product.service_type,
        'SSTYPE': product.service_subtype,
        'SSID': product.ssid,
        'TIMESYS': "OBT",
        'LEVEL': 'LB'
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]


def test_level0_processor_init():
    pro = FitsL0Processor('some/path')
    assert pro.archive_path == 'some/path'


def test_level0_processor_generate_filename():
    with patch('stixcore.products.level0.quicklook.QLProduct') as product:
        processor = FitsL0Processor('some/path')
        product.control.colnames = []
        product.type = 'ql'
        product.obs_avg.coarse = 0
        product.level = 'L0'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-ql-a-name_0000000000_V01.fits'

    with patch('stixcore.products.level0.science.ScienceProduct') as product:
        product.type = 'sci'
        product.control.colnames = []
        product.obs_avg.coarse = 0
        product.level = 'L0'
        product.name = 'a_name'
        product.obs_beg = SCETime(12345, 6789)
        product.obs_end = SCETime(98765, 4321)
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name_0000012345-0000098765_V01.fits'

        dummy_control_data = {'request_id': [123456], 'tc_packet_seq_control': [98765]}

        product.control.__getitem__.side_effect = dummy_control_data.__getitem__
        product.control.colnames = ['request_id']
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name-123456_' \
                           '0000012345-0000098765_V01.fits'

        product.control.colnames = ['request_id', 'tc_packet_seq_control']
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L0_stix-sci-a-name-123456_' \
                           '0000012345-0000098765_V01_98765.fits'


@patch('stixcore.products.level0.quicklook.QLProduct')
@patch('stixcore.io.fits.processors.datetime')
def test_level0_processor_generate_primary_header(datetime, product):
    processor = FitsL0Processor('some/path')
    datetime.now().isoformat.return_value = '1234-05-07T01:02:03.346'
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)
    avg = (beg + end)/2
    product.obs_beg = beg
    product.obs_avg = avg
    product.obs_end = end
    product.level = 'L0'
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3

    test_data = {
        'FILENAME': 'a_filename.fits',
        'DATE': '1234-05-07T01:02:03.346',
        'OBT_BEG': str(beg.as_float().value),
        'OBT_END': str(end.as_float().value),
        'DATE_OBS': str(beg.as_float().value),

        'DATE_BEG': str(beg.as_float().value),
        'DATE_AVG': str(avg.as_float().value),
        'DATE_END': str(end.as_float().value),
        'STYPE': product.service_type,
        'SSTYPE': product.service_subtype,
        'SSID': product.ssid,
        'TIMESYS': "OBT",
        'LEVEL': 'L0'
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]


def test_level1_processor_init():
    pro = FitsL1Processor('some/path')
    assert pro.archive_path == 'some/path'


def test_level1_processor_generate_filename():
    with patch('stixcore.products.level1.quicklook.QLProduct') as product:
        processor = FitsL1Processor('some/path')
        product.control.colnames = []
        beg = SCETime(coarse=0, fine=0)
        end = SCETime(coarse=1, fine=2 ** 15)
        avg = (beg + end)/2
        product.obt_beg = beg
        product.obt_avg = avg
        product.obt_end = end
        product.obs_beg = beg.to_datetime()
        product.obs_avg = avg.to_datetime()
        product.obs_end = end.to_datetime()
        product.type = 'ql'
        product.level = 'L1'
        product.name = 'a_name'
        filename = processor.generate_filename(product, version=1)
        assert filename == 'solo_L1_stix-ql-a-name_20000101T000000_20000101T000001_V01.fits'


@patch('stixcore.products.level1.quicklook.QLProduct')
def test_level1_processor_generate_primary_header(product):
    processor = FitsL1Processor('some/path')
    beg = SCETime(coarse=0, fine=0)
    end = SCETime(coarse=1, fine=2 ** 15)
    avg = (beg + end)/2
    product.obt_beg = beg
    product.obt_avg = avg
    product.obt_end = end
    product.obs_beg = beg.to_datetime()
    product.obs_avg = avg.to_datetime()
    product.obs_end = end.to_datetime()
    product.level = 'L1'
    product.type = "ql"
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3

    test_data = {
        'FILENAME': 'a_filename.fits',
        'OBT_BEG': str(beg.as_float().value),
        'OBT_END': str(end.as_float().value),
        'DATE_OBS': astrotime(beg.to_datetime()).fits,
        'DATE_BEG': astrotime(beg.to_datetime()).fits,
        'DATE_AVG': astrotime(avg.to_datetime()).fits,
        'DATE_END': astrotime(end.to_datetime()).fits,
        'STYPE': product.service_type,
        'SSTYPE': product.service_subtype,
        'SSID': product.ssid,
        'TIMESYS': 'UTC',
        'LEVEL': 'L1',
        "OBS_TYPE": 'ql'
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]

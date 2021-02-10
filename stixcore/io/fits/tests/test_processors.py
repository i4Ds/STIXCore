from datetime import datetime
from unittest.mock import patch

from stixcore.datetime.datetime import DateTime
from stixcore.io.fits.processors import FitsL0Processor


def test_level0_processor_init():
    pro = FitsL0Processor('some/path')
    assert pro.archive_path == 'some/path'


@patch('stixcore.products.level0.quicklook.QLProduct')
def test_level0_processor_generate_filename(product):
    processor = FitsL0Processor('some/path')
    product.control.colnames = []
    product.type = 'ql'
    product.obs_avg.coarse = 0
    product.level = 'LB'
    product.name = 'a_name'
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_LB_stix-ql-a-name_0000000000_V01.fits'

    product.type = 'sci'
    product.obs_beg.to_datetime.return_value = datetime(2020, 1, 2, 3, 4, 5, 6)
    product.obs_end.to_datetime.return_value = datetime(2020, 4, 5, 6, 7, 8, 9)
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_LB_stix-sci-a-name_20200102T030405-20200405T060708_V01.fits'

    dummy_control_data = {'request_id': [123456], 'tc_packet_seq_control': [98765]}

    product.control.__getitem__.side_effect = dummy_control_data.__getitem__
    product.control.colnames = ['request_id']
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_LB_stix-sci-a-name-123456_20200102T030405-20200405T060708_V01.fits'

    product.control.colnames = ['request_id', 'tc_packet_seq_control']
    filename = processor.generate_filename(product, version=1)
    assert filename == 'solo_LB_stix-sci-a-name-123456_' \
                       '20200102T030405-20200405T060708_V01_98765.fits'


@patch('stixcore.products.level0.quicklook.QLProduct')
@patch('stixcore.io.fits.processors.datetime')
def test_level0_processor_generate_primary_header(datetime, product):
    processor = FitsL0Processor('some/path')
    datetime.now().isoformat.return_value = '1234-05-07T01:02:03.346'

    product.obs_beg = DateTime(coarse=0, fine=0)
    product.obs_avg = DateTime(coarse=0, fine=2**15)
    product.obs_end = DateTime(coarse=1, fine=2**15)
    product.service_type = 1
    product.service_subtype = 2
    product.ssid = 3

    test_data = {
        'FILENAME': 'a_filename.fits',
        'DATE': '1234-05-07T01:02:03.346',
        'OBT_BEG': '0000000000:00000',
        'OBT_END': '0000000001:32768',
        'DATE_OBS': '0000000000:00000',
        'DATE_BEG': '0000000000:00000',
        'DATE_AVG': '0000000000:32768',
        'DATE_END': '0000000001:32768',
        'STYPE': 1,
        'SSTYPE': 2,
        'SSID': 3,
    }

    header = processor.generate_primary_header('a_filename.fits', product)
    for name, value, *comment in header:
        if name in test_data.keys():
            assert value == test_data[name]

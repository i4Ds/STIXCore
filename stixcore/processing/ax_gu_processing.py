from binascii import unhexlify
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as Et

import numpy as np
from astropy.table.table import Table
from bitstring import ConstBitStream, ReadError

from stixcore.idb.manager import IDBManager
from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.levelb.binary import LevelB
from stixcore.products.product import Product
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import SourcePacketHeader, TMPacket, GenericTMPacket

idbm = IDBManager(Path( '/Users/shane/Projects/STIX/STIXCore/stixcore/data/idb'), force_version='2.26.35')
# idbm.download_version("2.26.35", force=True)
idb = idbm.get_idb('2.26.35')

# bdata = b''
# tree = Et.parse('/Users/shane/Projects/STIX/dataview/data/tm_test/LTP03_morning_req_BatchRequest.PktTmRaw.SOL.0.2021.071.15.31.29.346.FelM@2021.083.06.00.01.912.1.xml')
# root = tree.getroot()
# res = defaultdict(list)
# for i, node in enumerate(root.iter('Packet')):
#     packet_binary = unhexlify(node.text)
#     # Not sure why guess and extra moc header
#     #packet_data.append(packet_binary[76:])
#     bdata += packet_binary[76:]
#     # res[(p.data_header.service_type, p.data_header.service_subtype, p.pi1_val)].append(p)

with open('/Users/shane/Downloads/ASV_v181_data_requests.ascii') as f:
# with open('/Users/shane/Downloads/20210406-1725.bsd_data_export.asc') as f:
    hex_data = f.readlines()

bdata = [unhexlify(h.split()[-1].strip()) for h in hex_data]


# # with open('/Users/shane/Downloads/BSD_Test_New_238-1_Sizing.bin', 'rb') as f:
# with open('/Users/shane/Dropbox/STIX-timing/05_26_s3_r1/ax_tmtc.bin', 'rb') as f:
#     bdata = f.read()

# bsdata = ConstBitStream(bdata)

res = defaultdict(list)
error = []
for i, bd in enumerate(bdata):
    try:
        tm = TMPacket(ConstBitStream(bd), idb=idb)
        res[tm.key].append(tm)
        print('Parsed line ', i)
    except Exception:
        print('Error with line ', i)
        error.append(i)
        continue




# start = 0
# i = 0
# while True:
#     sph = SourcePacketHeader(bsdata[start:])
#     end = start + (sph.data_length + 7) * 8
#     print(i, start, end)
#
#     try:
#         tm = TMPacket(bsdata[start:end].bytes, idb=idb)
#         res[tm.pi1_val].append(tm)
#     except Exception:
#         break
#
#     if end == bsdata.len:
#         break
#
#     i += 1
#     start = end

# print(bsdata.len)

fits_writer = FitsL0Processor(Path('/Users/shane/Library/Application Support/JetBrains/PyCharm2021.1/scratches/fsw-v181'))



for ssid, packets in res.items():
    t, st, ssid = ssid
    if t == 21 and ssid in {20, 21, 22, 23, 24, 30, 31, 32, 33, 34, 41, 42}:
    # if ssid in {30, 20, 21, 22, 23, 24}:
        headers = []
        hex_data = []
        for packet in packets:
            sh = vars(packet.source_packet_header)
            bs = sh.pop('bitstream')
            hex_data.append(bs.hex)
            dh = vars(packet.data_header)
            dh.pop('datetime')
            headers.append({**sh, **dh})

        control = Table(headers)
        control['index'] = np.arange(len(control), dtype=np.int64)

        data = Table()
        data['control_index'] = np.array(control['index'], dtype=np.int64)
        data['data'] = hex_data
        service_type, service_subtype = 21, 6
        product = LevelB(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data)

        cur_complete, cur_incomplete = product.extract_sequences()

        tmp = Product._check_registered_widget(
            level='L0', service_type=product.service_type,
            service_subtype=product.service_subtype, ssid=product.ssid, data=None,
            control=None)

        for c in cur_complete:
            level0 = tmp.from_levelb(c)
            fits_writer.write_fits(level0)

        for c in cur_incomplete:
            level0 = tmp.from_levelb(c)
            fits_writer.write_fits(level0)

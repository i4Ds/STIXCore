import os
import logging
import xml.etree.ElementTree as Et
from time import perf_counter
from pathlib import Path
from binascii import unhexlify
from collections import defaultdict

import numpy as np

from astropy.table import unique, vstack
from astropy.table.table import QTable, Table

from stixcore.io.fits.processors import FitsLBProcessor
from stixcore.tmtc.packets import TMPacket
from stixcore.util.logging import get_logger

SECONDS_IN_DAY = 24 * 60 * 60

logger = get_logger(__name__, level=logging.DEBUG)


class LevelB:
    """
    Class representing level binary data.
    """
    def __init__(self, control, data):
        self.level = 'LB'
        self.type = f"{control['service_type'][0]}-{control['service_subtype'][0]}"
        self.control = control
        self.data = data

        synced_times = (self.control['scet_coarse'] >> 31) != 1
        self.control['scet_coarse'] = np.where(synced_times, self.control['scet_coarse'],
                                               self.control['scet_coarse'] ^ 2**31)
        self.control['time_sync'] = synced_times
        scet = self.control['scet_coarse'] + self.control['scet_coarse']/2**16
        min_time_ind = scet.argmin()
        max_time_ind = scet.argmax()
        self.obt_beg = f"{self.control['scet_coarse'][min_time_ind]}:" \
                       f"{self.control['scet_fine'][min_time_ind]:05d}"

        self.obt_end = f"{self.control['scet_coarse'][max_time_ind]}:" \
                       f"{self.control['scet_fine'][max_time_ind]:05d}"

    def __repr__(self):
        return f"<Level0 {self.level}, {self.type}, " \
               f"{self.obt_beg}-{self.obt_end}>"

    def __add__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        other.control['index'] = other.control['index'] + self.control['index'].max() + 1
        other.data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        control = vstack((self.control, other.control))
        control = unique(control, ['scet_coarse', 'scet_fine', 'seq_count'])

        orig_indices = control['index']
        new_index = range(len(control))
        control['index'] = new_index

        data = vstack((self.data, other.data))
        data = data[np.nonzero(orig_indices[:, None] == data['control_index'].data)[1]]
        data['control_index'] = range(len(data))

        if np.abs([((len(data['data'][i]) / 2) - (control['data_len'][i] + 7))
                   for i in range(len(data))]).sum() > 0:
            logger.error('Expected and actual data length do not match')

        return type(self)(control, data)

    @classmethod
    def from_fits(cls, fitspath):
        control = QTable.read(fitspath, hdu='CONTROL')
        data = QTable.read(fitspath, hdu='DATA')
        return cls(control=control, data=data)

    def to_days(self):
        """
        Return a list of the of classes each containing 1 days worth of observations

        Returns
        -------

        """
        scet = self.control['scet_coarse'] + (self.control['scet_coarse'] / (2**16-1))
        scet = scet[(self.control['scet_coarse'] >> 31) != 1]
        days, frac = np.divmod(scet, SECONDS_IN_DAY)
        days = np.unique(days) * SECONDS_IN_DAY
        for day_start in days:
            day_end = day_start + SECONDS_IN_DAY

            inds = np.argwhere((scet >= day_start) & (scet < day_end))
            i = np.arange(inds.min(), inds.max()+1)
            control = self.control[i]
            control_indices = control['index']
            min_index = control_indices.min()
            control['index'] = control['index'] - min_index
            data = self.data[i]
            data['control_index'] = data['control_index'] - min_index

            yield type(self)(control=control, data=data)


def process_tmtc_file(tmfile, basedir):
    fits_processor = FitsLBProcessor(basedir)

    tree = Et.parse(tmfile)
    root = tree.getroot()
    packet_data = defaultdict(list)
    for i, node in enumerate(root.iter('Packet')):
        packet_binary = unhexlify(node.text)
        # Not sure why guess and extra moc header
        tm_packet = process_tm_packet(packet_binary[76:])

        key = f'{tm_packet.data_header.service_type}-{tm_packet.data_header.service_subtype}'
        if getattr(tm_packet, 'pi1_val', None) is not None:
            key += f'{-tm_packet.pi1_val}'
        packet_data[key].append(tm_packet)

    for product, packets in packet_data.items():

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
        control['index'] = np.arange(len(control))

        data = Table()
        data['control_index'] = control['index']
        # data['data'] = binary_data
        data['data'] = hex_data

        prod = LevelB(control=control, data=data)
        fits_processor.write_fits(prod)

    # return packet_data


def process_tm_packet(binary_packet):
    return TMPacket(binary_packet)


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('test')
    # Real data
    raw_tmtc = Path('/Users/shane/Projects/STIX/dataview/data/real')
    tmtc_files = sorted(list(raw_tmtc.glob('*PktTmRaw*.xml')), key=os.path.getmtime)
    tmtc_files = ['/Users/shane/Projects/STIX/dataview/data/real/IX4.DAY2.BatchRequest.PktTmRaw.SOL.0.2020.140.11.55.36.842.yzKB@2020.140.11.55.37.803.1.xml'] # NOQA
    bd = Path('/Users/shane/Projects/STIX/dataview/data/test2')

    for tmtc_file in tmtc_files:
        process_tmtc_file(tmtc_file, basedir=bd)

    tend = perf_counter()
    logger.info('Time taken %f', tend - tstart)

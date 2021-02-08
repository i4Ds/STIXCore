import os
from enum import IntEnum
from time import perf_counter
from pathlib import Path
from binascii import unhexlify
from collections import defaultdict

import numpy as np

from astropy.table.table import Table

from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.product import Product
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import GenericTMPacket, PacketSequence
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class SequenceFlag(IntEnum):
    STANDALONE = 3
    FIRST = 1
    MIDDLE = 0
    LAST = 2


def get_products(packet_list, spids=None):
    """
    Filter and split TM packet by SPID and status complete or incomplete

    Complete product are stand-alone packets, or a packet sequence with with with 1 start,
    n continuation packets and 1 end packet, where n = 0, 1, 2

    Parameters
    ----------
    packet_list : `list`
        List of packets
    spids : `list` (optional)
        List of SPIDs if set only process theses SPID other wise process all

    Returns
    -------
    `tuple`
        Two dictionaries containing the complete and incomplete products
    """
    filtered_packets = {}
    if not spids:
        spids = set([x.spid for x in packet_list])

    for value in spids:
        filtered_packets[value] = list(filter(
            lambda x: x.spid == value and isinstance(x, GenericTMPacket), packet_list))

    complete = defaultdict(list)
    incomplete = defaultdict(list)

    for key, packets in filtered_packets.items():
        sequences = extract_sequences(packets[:])
        for seq in sequences:
            if len(seq) == 1 and \
                    seq[0].source_packet_header.sequence_flag == SequenceFlag.STANDALONE:
                complete[key].append(seq)
            elif (seq[0].source_packet_header.sequence_flag == SequenceFlag.FIRST
                  and seq[0].source_packet_header.sequence_flag == SequenceFlag.LAST):
                complete[key].append(seq)
            else:
                incomplete[key].append(seq)
                logger.warning('Incomplete sequence for %d', key)

    return complete, incomplete


def extract_sequences(packets):
    """
    Extract complete and incomplete sequences of packets.

    Packets can be either stand-alone, first ,continuation or last when TM is downloaded maybe
    missing some packets so we we try to extract complete and incomplete sequences.

    Parameters
    ----------
    packets : list
        List of packets

    Returns
    -------
    list
        A list of packet packets
    """
    out = []
    i = 0
    cur_seq = None
    while i < len(packets):
        cur_packet = packets.pop(i)
        cur_flag = cur_packet.source_packet_header.sequence_flag
        if cur_flag == SequenceFlag.STANDALONE:
            out.append([cur_packet])
        elif cur_flag == SequenceFlag.FIRST:
            cur_seq = [cur_packet]
        if cur_flag == SequenceFlag.MIDDLE:
            if cur_seq:
                cur_seq.append(cur_packet)
            else:
                cur_seq = [cur_packet]
        if cur_flag == SequenceFlag.LAST:
            if cur_seq:
                cur_seq.append(cur_packet)
                out.append(cur_seq)
            else:
                out.append([cur_packet])
            cur_seq = None

    if cur_seq:
        out.append(cur_seq)

    return out


def process_packets(packets, basepath, status=None):
    """
    Process a sequence containing one or more packets for a given product.

    Parameters
    ----------
    packet_lists : list
        List of packet sequences
    spid : int
        SPID
    product : basestring
        Product name
    basepath : pathlib.Path
        Path
    overwrite : bool (optional)
        False (default) will raise error if fits file exits, True overwrite existing file
    """
    fits_processor = FitsL0Processor(basepath)

    pseq = PacketSequence(packets)

    prod = Product._check_registered_widget(level='L0', service_type=pseq.get('service_type')[0],
                                            service_subtype=pseq.get('service_subtype')[0],
                                            ssid=pseq.get('pi1_val')[0], data=None,
                                            control=None)

    cur_prod = prod.from_packets(pseq)
    fits_processor.write_fits(cur_prod)


def fits_to_packets(file):
    logger.info(f'Processing fits file {file}')
    control = Table.read(str(file), hdu=1)
    data = Table.read(str(file), hdu=2)

    packets = [Packet(unhexlify(d)) for d in data['data']]
    if np.abs([((len(data['data'][i])//2) - (control['data_len'][i]+7))
               for i in range(len(data))]).sum() > 0:
        raise ValueError('Packet size and expected length do not match')
    return packets


class Level0:
    def __init__(self, source_dir, output_dir, use_name=False):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')),  key=os.path.getctime)
        self.use_name = use_name

    def process_fits_files(self):
        for file in self.level0_files:
            prod = Product(file)

            packets = [Packet(unhexlify(d)) for d in prod.data['data']]
            if np.abs([((len(prod.data['data'][i]) // 2) - (prod.control['data_length'][i] + 7))
                       for i in range(len(prod.data))]).sum() > 0:
                raise ValueError('Packet size and expected length do not match')

            process_packets(packets, self.output_dir)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/asdfadsf//LB/21/6/30')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/asdfadsf')

    l0processor = Level0(fits_path, bd)
    l0processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)



import os
from enum import IntEnum
from time import perf_counter
from pathlib import Path

from stixcore.io.fits.processors import FitsL0Processor
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class SequenceFlag(IntEnum):
    STANDALONE = 3
    FIRST = 1
    MIDDLE = 0
    LAST = 2


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


class Level0:
    """

    """
    def __init__(self, source_dir, output_dir):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.levelb_files = sorted(list(self.source_dir.rglob('*.fits')),  key=os.path.getctime)
        self.processor = FitsL0Processor(output_dir)

    def process_fits_files(self):
        for file in self.levelb_files:
            levelb = Product(file)
            tmp = Product._check_registered_widget(level='L0', service_type=levelb.service_type,
                                                   service_subtype=levelb.service_subtype,
                                                   ssid=levelb.ssid, data=None, control=None)
            level0 = tmp.from_levelb(levelb)
            self.processor.write_fits(level0)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/asdfadsf/LB/21/6/30')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/asdfadsf')

    l0processor = Level0(fits_path, bd)
    l0processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)
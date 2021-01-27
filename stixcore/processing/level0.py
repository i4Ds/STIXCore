import os
from enum import IntEnum
from time import perf_counter
from pathlib import Path
from binascii import unhexlify
from itertools import chain
from collections import defaultdict

import numpy as np

# from watchdog.events import FileSystemEventHandler, FileMovedEvent
from astropy.table.table import Table

from stixcore.io.fits.processors import FitsL1Processor
from stixcore.products.quicklook import LightCurve
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import GenericTMPacket, PacketSequence
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class SequenceFlag(IntEnum):
    STANDALONE = 3
    FIRST = 1
    MIDDLE = 0
    LAST = 2


SPID_MAP = {
    # Bulk Science
    # 54110: 'xray_l0_auto',
    # 54111: 'xray_l1_auto',
    # 54112: 'xray_l2_auto',
    # 54113: 'xray_l3_auto',
    # 54142: 'spectrogram_auto',
    54114: 'xray_l0_user',
    54115: 'xray_l1_user',
    54116: 'xray_l2_user',
    54117: 'xray_l3_user',
    54143: 'spectrogram_user',
    54125: 'aspect',
    # Quick look
    54118: 'ql_light_curves',
    54119: 'ql_background',
    54120: 'ql_spectra',
    54121: 'ql_variance',
    54122: 'flareflag_location',
    54144: 'tm_status_and_flare_list',
    54124: 'calibration_spectrum',
    # House keeping
    # 54101: 'hk_mini',
    # 54102: 'hk_maxi'
}


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


def process_packets(packet_lists, spid, product, basepath=None, status='', overwrite=False,
                    use_name=False):
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
    fits_processor = FitsL1Processor(basepath)

    # For HK merge all stand alone packets in request
    if spid in [54101, 54102]:
        packet_lists = [list(chain.from_iterable(packet_lists))]

    prod = None

    for packets in packet_lists:
        if packets:
            # parsed_packets = sdt.Packet.merge(packets, spid, value_type='raw')
            # e_parsed_packets = sdt.Packet.merge(packets, spid, value_type='eng')
            pseq = PacketSequence(packets)
            try:
                # if product == 'hk_mini':
                #     cur_prod = MiniReport(parsed_packets)
                # elif product == 'hk_maxi':
                #     cur_prod = MaxiReport(parsed_packets)
                if product == 'ql_light_curves':
                    cur_prod = LightCurve.from_packets(pseq)
                # elif product == 'ql_background':
                #     cur_prod = Background.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'ql_spectra':
                #     cur_prod = Spectra.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'ql_variance':
                #     cur_prod = Variance.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'flareflag_location':
                #     cur_prod = FlareFlagAndLocation.from_packets(parsed_packets)
                # elif product == 'calibration_spectrum':
                #     cur_prod = CalibrationSpectra.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'tm_status_and_flare_list':
                #     cur_prod = TMManagementAndFlareList.from_packets(parsed_packets,
                #                                                      e_parsed_packets)
                # elif product == 'xray_l0_user':
                #     cur_prod = XrayL0.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'xray_l1_user':
                #     cur_prod = XrayL1.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'xray_l2_user':
                #     cur_prod = XrayL2.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'xray_l3_user':
                #     cur_prod = XrayL3.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'spectrogram_user':
                #     cur_prod = Spectrogram.from_packets(parsed_packets, e_parsed_packets)
                # elif product == 'aspect':
                #     cur_prod = Aspect.from_packets(parsed_packets, e_parsed_packets)
                #     # cur_prod = None
                else:
                    logger.debug('Not implemented %s, SPID %d.', product, spid)
                    continue

                if cur_prod is not None and prod is not None:
                    prod = prod + cur_prod
                elif cur_prod is not None and prod is None:
                    prod = cur_prod

                if prod:
                    fits_processor.write_fits(prod)
                # filename = generate_filename('L1', product, cur_prod, 1, status=status)
                # primary_header = generate_primary_header(filename, cur_prod.scet_coarse,
                # cur_prod.scet_fine, cur_prod.obs_beg, cur_prod.obs_avg, cur_prod.obs_end)
                # hdul = cur_prod.to_hdul()
                #
                # hdul[0].header.update(primary_header)
                # hdul[0].header.update({'HISTORY': 'Processed by STIX'})
                #
                # if use_name:
                #     path = basepath
                # else:
                #     path = basepath.joinpath(*[format(cur_prod.obs_beg.year, '04d'),
                #                                format(cur_prod.obs_beg.month, '02d'),
                #                                format(cur_prod.obs_beg.day, '02d'), product_type])
                # path.mkdir(exist_ok=True, parents=True)
                # logger.debug(f'Writing {path / filename}')
                # hdul.writeto(path / filename, checksum=True, overwrite=overwrite)
            except Exception as e:
                logger.error('error', exc_info=True)
                raise e
    # if prod:
    #     fits_processor.write_fits(prod)


def process_products(products, basepath, type='', overwrite=False, use_name=False):
    """

    Parameters
    ----------
    products
    type
    basepath
    overwrite

    Returns
    -------

    """
    for spid, data in products.items():
        logger.debug('Processing %s products SPID %d',  type, spid)
        product = SPID_MAP.get(spid, 'unknown')
        if spid:
            path = basepath
            path.mkdir(exist_ok=True, parents=True)
            # try:
            process_packets(data, spid, product, path, status=type, overwrite=overwrite,
                            use_name=use_name)
            # except Exception as e:
            #     logger.error('error %d, %s', spid, product, exc_info=True)


def fits_to_packets(file):
    logger.info(f'Processing fits file {file}')
    control = Table.read(str(file), hdu=1)
    data = Table.read(str(file), hdu=2)

    packets = [Packet(unhexlify(d)) for d in data['data']]
    if np.abs([((len(data['data'][i])//2) - (control['data_len'][i]+7))
               for i in range(len(data))]).sum() > 0:
        raise ValueError('Packet size and expected length do not match')
    # packets = list(chain.from_iterable(packets))
    # Filter keeping only TM packets
    # packets = list(filter(lambda x: x['header']['TMTC'] == 'TM', packets))
    # Packet ordering is not guaranteed so sort by coarse time then seq count
    # packets.sort(key=lambda x: (x['header']['coarse_time'], x['header']['seq_count']))
    return packets


class Level0:
    def __init__(self, source_dir, output_dir, use_name=False):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.level0_files = sorted(list(self.source_dir.rglob('*.fits')),  key=os.path.getctime)
        self.level0_files = self.level0_files[-17:]
        self.use_name = use_name

    def process_fits_files(self):
        cur_packets = fits_to_packets(self.level0_files[0])
        cur_complete, cur_incomplete = get_products(cur_packets)
        spid = set(list(cur_complete.keys()) + list(cur_incomplete.keys()))
        if len(spid) > 1:
            raise ValueError()
        spid = spid.pop()
        if len(self.level0_files) > 1:
            for file in self.level0_files[1:]:
                next_packets = fits_to_packets(file)
                next_complete, next_incomplete = get_products(next_packets)

                if len(cur_incomplete[spid]) > 0 and len(next_incomplete[spid]) > 0:
                    incomplete_combined, incomplete_remaining\
                        = get_products(cur_incomplete[spid][0] + next_incomplete[spid][0])

                    if len(incomplete_combined) > 0:
                        complete = cur_complete.copy()
                        complete[spid] = [complete[spid][0] + incomplete_combined[spid][0]]
                        incomplete = {}
                        next_incomplete = incomplete_remaining
                    else:
                        complete = cur_complete
                        incomplete = cur_incomplete
                else:
                    complete = cur_complete
                    incomplete = cur_incomplete

                cur_complete = next_complete
                cur_incomplete = next_incomplete

                logger.info(f'Complete {[(k, len(v)) for k, v in complete.items()]}')
                logger.info(f'Incomplete {[(k, len(v)) for k, v in incomplete.items()]}')

                process_products(complete, self.output_dir, overwrite=True, use_name=self.use_name)
                process_products(incomplete, self.output_dir, 'IC', overwrite=True,
                                 use_name=self.use_name)

        # TODO Rework loop so this isn't necessary
        process_products(cur_complete, self.output_dir, overwrite=True, use_name=self.use_name)
        process_products(cur_incomplete, self.output_dir, 'IC', overwrite=True,
                         use_name=self.use_name)

# class TMTCFileHandler(FileSystemEventHandler):
#     def __init__(self, func, output_path, use_name):
#         self.func = func
#         self.output_path = output_path
#         self.use_name = use_name
#
#     def on_moved(self, event):
#         if isinstance(event, FileMovedEvent):
#             time.sleep(2)
#             self.func(Path(event.dest_path), self.output_path, use_name=self.use_name)


if __name__ == '__main__':
    tstart = perf_counter()

    fits_path = Path('/Users/shane/Projects/stix/dataview/data/test2/L0/21/6/30')
    bd = Path('/Users/shane/Projects/STIX/dataview/data/test2')

    l0processor = Level0(fits_path, bd)
    l0processor.process_fits_files()

    tend = perf_counter()
    logger.info('Time taken %f', tend-tstart)

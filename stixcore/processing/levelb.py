import os
import logging
from time import perf_counter
from pathlib import Path

import numpy as np

from astropy.table import unique, vstack
from astropy.table.table import QTable, Table

from stixcore.io.fits.processors import SEC_IN_DAY, FitsLBProcessor
from stixcore.io.soc.manager import SOCManager
from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

__all__ = ['LevelB']


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
        return f"<LevelB {self.level}, {self.type}, " \
               f"{self.obt_beg}-{self.obt_end}>"

    def __add__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        other.control['index'] = other.control['index'] + self.control['index'].max() + 1
        other.data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        control = vstack((self.control, other.control))
        control = unique(control, ['scet_coarse', 'scet_fine', 'sequence_count'])

        orig_indices = control['index']
        new_index = range(len(control))
        control['index'] = new_index

        data = vstack((self.data, other.data))
        data = data[np.nonzero(orig_indices[:, None] == data['control_index'].data)[1]]
        data['control_index'] = range(len(data))

        if np.abs([((len(data['data'][i]) / 2) - (control['data_length'][i] + 7))
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
        days, frac = np.divmod(scet, SEC_IN_DAY)
        days = np.unique(days) * SEC_IN_DAY
        for day_start in days:
            day_end = day_start + SEC_IN_DAY

            inds = np.argwhere((scet >= day_start) & (scet < day_end))
            i = np.arange(inds.min(), inds.max()+1)
            control = self.control[i]
            control_indices = control['index']
            min_index = control_indices.min()
            control['index'] = control['index'] - min_index
            data = self.data[i]
            data['control_index'] = data['control_index'] - min_index

            yield type(self)(control=control, data=data)

    @classmethod
    def process(cls, tmfile, fits_processor):
        tmfile.read_packet_data_header()

        for product, packets in tmfile.packet_data.items():

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
            data['data'] = hex_data
            product = LevelB(control=control, data=data)
            fits_processor.write_fits(product)

        tmfile.free_packet_data()


if __name__ == '__main__':
    tstart = perf_counter()
    logger.info('LevelB run')

    socm = SOCManager(Path('/Users/shane/Projects/STIX/dataview/data/real'))
    out_dir = Path('/Users/shane/Projects/STIX/dataview/data/test2')

    if not out_dir.exists():
        os.makedirs(out_dir)

    fits_processor = FitsLBProcessor(out_dir)

    files_to_process = socm.get_files(TMTC.TM)
    for tmtc_file in files_to_process:
        prod = LevelB.process(tmtc_file, fits_processor)

    tend = perf_counter()
    logger.info('Time taken %f', tend - tstart)

from enum import IntEnum
from collections import defaultdict

import numpy as np

from astropy.table.operations import unique, vstack
from astropy.table.table import Table

from stixcore.products.product import BaseProduct
from stixcore.time import SCETime
from stixcore.tmtc.packets import TMPacket
from stixcore.util.logging import get_logger

__all__ = ['LevelB', 'SequenceFlag']

logger = get_logger(__name__)
logger.setLevel('DEBUG')


class SequenceFlag(IntEnum):
    """Enum class for the packet sequence flag."""

    STANDALONE = 3
    """A singelton standalone package. No sequence at all."""

    FIRST = 1
    """The First package in a sequence. More to come."""

    MIDDLE = 0
    """Continouse packages in a sequence. More to come."""

    LAST = 2
    """The Last packet in a sequence. No more to come."""


class LevelB(BaseProduct):
    """Class representing level binary data (TM products)."""

    def __init__(self, *, service_type, service_subtype, ssid, control, data, **kwargs):
        """Create a new LevelB object.

        Parameters
        ----------
        control : ´Table´
            the control table
        data : ´Table´
            the data table
        prod_key : ´tuple´
            (service, subservice [, ssid])
        """
        self.level = 'LB'
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.control = control
        self.data = data
        self.type = ''

        # TODO better encapsulated time handling?
        times = [SCETime(c, f) for c, f in self.control[['scet_coarse', 'scet_fine']]]
        self.control['scet_coarse'] = [t.coarse for t in times]
        self.control['time_sync'] = [t.time_sync for t in times]

        # TODO check if need to sort before this
        self.obt_beg = SCETime(self.control['scet_coarse'][0], self.control['scet_fine'][0])
        self.obt_end = SCETime(self.control['scet_coarse'][-1], self.control['scet_fine'][-1])
        self.obt_avg = self.obt_beg + (self.obt_end - self.obt_beg)/2

    @property
    def parent(self):
        """List of all parent data files this data product is compiled from.

        For level binary this will be allways the parent raw TM file.

        Returns
        -------
        `list(str)
            the TM file that this data product contains.
        """
        return np.unique(self.control['raw_file']).tolist()

    @property
    def raw(self):
        """List of all TM raw files this product is compiled from.

        Returns
        -------
        `list(str)
            the TM file that this data product contains.
        """
        return np.unique(self.control['raw_file']).tolist()

    def __repr__(self):
        return f"<LevelB {self.level}, {self.service_type}, {self.service_subtype}, {self.ssid} " \
               f"{self.obt_beg}-{self.obt_end}>"

    def __add__(self, other):
        """Combine two packets of same type. Same time points gets overridden.

        Parameters
        ----------
        other : `LevelB`
            The other packet to combine with.

        Returns
        -------
        `LevelB`
            a new LevelB instance with combined data.

        Raises
        ------
        TypeError
            If not of same type: (service, subservice [, ssid])
        """
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        # Copy so don't overwrite
        other = other[:]
        other.control['index'] = other.control['index'].data + self.control['index'].data.max() + 1
        other.data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        # For level b the control info is used to sort and select
        control = vstack((self.control, other.control))
        control = unique(control, ['scet_coarse', 'scet_fine', 'sequence_count'])

        orig_indices = control['index']
        # some BS for windows!
        new_index = np.array(range(len(control)), dtype=np.int64)
        control['index'] = new_index

        data = vstack((self.data, other.data))
        data = data[np.nonzero(orig_indices[:, None] == data['control_index'].data)[1]]
        data['control_index'] = np.array(range(len(data)), dtype=np.int64)

        if np.abs([((len(data['data'][i]) / 2) - (control['data_length'][i] + 7))
                   for i in range(len(data))]).sum() > 0:
            logger.error('Expected and actual data length do not match')

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data)

    def to_days(self):
        """Split the data into a sequence of LevelB objects each containing 1 daysworth\
        of observations.

        Yields
        ------
        `LevelB`
            the next `LevelB` objects for another day.

        """
        # TODO reploace with SCETime
        scet = self.control['scet_coarse'] + (self.control['scet_coarse'] / (2**16-1))
        # scet = scet[(self.control['scet_coarse'] >> 31) != 1]
        from stixcore.io.fits.processors import SEC_IN_DAY
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

            yield type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                             ssid=self.ssid, control=control, data=data)

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self.data))
            data = self.data[start:stop:step]
        elif isinstance(key, list):
            data = self.data[key]
        elif isinstance(key, int):
            data = self.data[key]

        control_ids = np.unique(data['control_index'])
        control = self.control[np.isin(self.control['index'], control_ids)]

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data)

    def extract_sequences(self):
        """
        Extract complete and incomplete packet sequences.

        Returns
        -------
        tuple
            LevelB products for the complete and incomplete sequences
        """
        if self.service_type == 3 and self.service_subtype == 25 and self.ssid in {1, 2}:
            return [self], []
        elif self.service_type == 21 and self.service_subtype == 6 and self.ssid in {30, 31, 32,
                                                                                     33, 34, 43}:
            return [self], []

        sequences = []
        flags = self.control['sequence_flag']
        cur_seq = None
        for i, f in enumerate(flags):
            if f == SequenceFlag.STANDALONE:
                sequences.append([i])
            elif f == SequenceFlag.FIRST:
                cur_seq = [i]

            if f == SequenceFlag.MIDDLE:
                if cur_seq:
                    cur_seq.append(i)
                else:
                    cur_seq = [i]

            if f == SequenceFlag.LAST:
                if cur_seq:
                    cur_seq.append(i)
                    sequences.append(cur_seq)
                else:
                    sequences.append([i])

                cur_seq = None

        if cur_seq:
            sequences.append(cur_seq)

        complete = []
        incomplete = []

        for seq in sequences:
            if len(seq) == 1 and flags[seq[0]] == SequenceFlag.STANDALONE:
                complete.append(self[seq])
            elif flags[seq[0]] == SequenceFlag.FIRST and flags[seq[-1]] == SequenceFlag.LAST:
                complete.append(self[seq])
            else:
                incomplete.append(self[seq])
                logger.warning('Incomplete sequence')

        return complete, incomplete

    @classmethod
    def from_tm(cls, tmfile):
        """Process the given SOCFile and creates LevelB FITS files.

        Parameters
        ----------
        tmfile : `SOCPacketFile`
            The input data file.
        """
        packet_data = defaultdict(list)

        for packet_no, binary in tmfile.get_packet_binaries():
            try:
                packet = TMPacket(binary)
            except Exception:
                logger.error('Error parsing %s, %d', tmfile.name, packet_no, exc_info=True)
                return
            packet.source = (tmfile.file.name, packet_no)
            packet_data[packet.key].append(packet)

        for prod_key, packets in packet_data.items():
            headers = []
            hex_data = []
            for packet in packets:
                sh = vars(packet.source_packet_header)
                bs = sh.pop('bitstream')
                hex_data.append(bs.hex)
                dh = vars(packet.data_header)
                dh.pop('datetime')
                headers.append({**sh, **dh, 'raw_file': packet.source[0],
                                'packet': packet.source[1]})

            control = Table(headers)
            control['index'] = np.arange(len(control), dtype=np.int64)

            data = Table()
            data['control_index'] = np.array(control['index'], dtype=np.int64)
            data['data'] = hex_data

            control = unique(control, keys=['scet_coarse', 'scet_fine', 'sequence_count'])

            # Only keep data that is in the control table via index
            data = data[np.nonzero(control['index'][:, None] == data['control_index'])[1]]

            # now reindex both data and control
            control['index'] = range(len(control))
            data['control_index'] = control['index']

            service_type, service_subtype, ssid = prod_key
            if ssid is not None:
                control['ssid'] = ssid
            product = LevelB(service_type=service_type, service_subtype=service_subtype,
                             ssid=ssid, control=control, data=data)
            yield product

    @classmethod
    def is_datasource_for(cls, **kwargs):
        return kwargs['level'] == 'LB'

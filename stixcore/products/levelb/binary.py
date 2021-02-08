from collections import defaultdict

import numpy as np

from astropy.table.operations import unique, vstack
from astropy.table.table import QTable, Table

# from stixcore.io.fits.processors import SEC_IN_DAY
from stixcore.products.product import BaseProduct
from stixcore.tmtc.packets import TMPacket
from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class LevelB(BaseProduct):
    """Class representing level binary data."""

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

        # TODO better encapsulated time handling?
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

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data)

    @classmethod
    def from_fits(cls, fitspath):
        """Read fits file data into a LevelB object.

        Parameters
        ----------
        fitspath : `Path`
            the path to the FITS file.

        Returns
        -------
        `LevelB`
            a new `LevelB` object.
        """
        control = QTable.read(fitspath, hdu='CONTROL')
        data = QTable.read(fitspath, hdu='DATA')
        return cls(control=control, data=data, prod_key=None)

    def to_days(self):
        """Split the data into a sequence of LevelB objects each containing 1 daysworth\
        of observations.

        Yields
        ------
        `LevelB`
            the next `LevelB` objects for another day.

        """
        scet = self.control['scet_coarse'] + (self.control['scet_coarse'] / (2**16-1))
        scet = scet[(self.control['scet_coarse'] >> 31) != 1]
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

    @classmethod
    def process(cls, tmfile, fits_processor):
        """Process the given SOCFile and creates LevelB FITS files.

        Parameters
        ----------
        tmfile : `SOCPacketFile`
            The input data file.
        fits_processor : `FitsLBProcessor`
            The FITS file processor that does the writing and merging.
        """
        packet_data = defaultdict(list)
        for binary in tmfile.get_packet_binaries():
            packet = TMPacket(binary)
            if packet.key == (21, 6, 30):
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
                headers.append({**sh, **dh})

            control = Table(headers)
            control['index'] = np.arange(len(control))

            data = Table()
            data['control_index'] = control['index']
            data['data'] = hex_data
            service_type, service_subtype, ssid = prod_key
            product = LevelB(service_type=service_type, service_subtype=service_subtype,
                             ssid=ssid, control=control, data=data)

    @classmethod
    def is_datasource_for(cls, **kwargs):
        return kwargs['level'] == 'LB'

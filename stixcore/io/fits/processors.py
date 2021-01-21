""""
Quick Look fits file definitions
"""
import logging
from datetime import datetime

import numpy as np

from astropy.io import fits

from stixcore.util.logging import get_logger

# from stix_parser.io.fits.tm_to_fits import generate_filename


logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)

sec_in_day = 24 * 60 * 60


class FitsLBProcessor:
    def __init__(self, archive_path):
        self.archive_path = archive_path

    def write_fits(self, product):
        # if product.type == '21-6' and product.control['ssid'][0] in [20]:
        #     for prod in product.to_requests():
        #         pass

        for prod in product.to_days():
            filename = self.generate_filename(prod, version=1)
            if getattr(prod, 'ssid', False):
                parts = [prod.level, prod.control['service_type'][0],
                         prod.control['service_subtype'][0], prod.ssid]
            else:
                parts = [prod.level, prod.control['service_type'][0],
                         prod.control['service_subtype'][0]]
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)
            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = prod.from_fits(fitspath)
                if np.abs([((len(existing.data['data'][i])/2) - (existing.control['data_len'][i]+7))
                           for i in range(len(existing.data))]).sum() > 0:
                    raise ValueError()
                logger.debug('Existing %s \n New %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            # control = unique(prod.control, ['scet_coarse', 'scet_fine', 'seq_count'])
            # data = prod.data[np.isin(prod.data['control_index'], control['index'])]

            control = prod.control
            data = prod.data

            if np.abs([((len(data['data'][i]) / 2) - (control['data_len'][i] + 7))
                       for i in range(len(data))]).sum() > 0:
                raise ValueError()

            primary_header = self.generate_primary_header(prod, filename)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            control_hdu = fits.BinTableHDU(control)
            control_hdu.name = 'CONTROL'
            data_hdu = fits.BinTableHDU(data)
            data_hdu.name = 'DATA'
            hdul = fits.HDUList([primary_hdu, control_hdu, data_hdu])

            logger.info(f'Writing fits file to {path / filename}')
            hdul.writeto(path / filename, overwrite=True, checksum=True)

    def generate_primary_header(self, product, filename):
        """
        Generate primary header cards.

        Parameters
        ----------
        filename : str
            Filename

        Returns
        -------
        tuple
            List of header cards as tuples (name, value, comment)
        """
        headers = (
            # Name, Value, Comment
            ('TELESCOP', 'SOLO/STIX', 'Telescope/Sensor name'),
            ('INSTRUME', 'STIX', 'Instrument name'),
            ('OBSRVTRY', 'Solar Orbiter', 'Satellite name'),
            ('FILENAME', filename, 'FITS filename'),
            ('DATE', datetime.now().isoformat(timespec='milliseconds'),
             'FITS file creation date in UTC'),
            ('OBT_BEG', f'{product.control["scet_coarse"][0]}:{product.control["scet_fine"][0]}'),
            ('OBT_END', f'{product.control["scet_coarse"][-1]}:{product.control["scet_fine"][-1]}'),
            ('TIMESYS', 'OBT', 'System used for time keywords'),
            ('LEVEL', 'LB)', 'Processing level of the data'),
            ('ORIGIN', 'STIX Team, FHNW', 'Location where file has been generated'),
            ('CREATOR', 'STIX-SWF', 'FITS creation software'),
            ('VERSION', 1, 'Version of data product'),
            ('OBS_MODE', 'Nominal'),
            ('VERS_SW', 1, 'Software version')
        )
        return headers

    def generate_filename(self, product, version, status=''):
        """
        Generate fits file name with SOLO conventions.

        Parameters
        ----------
        product : stix_parser.product.BaseProduct
            Product
        version : int
            Version of this product
        status : str
            Status of the packets

        Returns
        -------
        str
            The filename
        """
        user_req = getattr(product.control, 'request_id', '')
        if user_req:
            user_req = f'_{user_req}'

        sec_in_day = 24 * 60 * 60
        scet_obs = (product.control["scet_coarse"][0] // sec_in_day) * sec_in_day
        ssid = getattr(product, 'ssid', '')
        if ssid:
            ssid = f"-{ssid}"
        name = f"{product.control['service_type'][0]}-{product.control['service_subtype'][0]}" \
               f"{ssid}"
        return f'solo_{product.level}_stix-{name}' \
               f'_{scet_obs}_V{version:02d}{status}.fits'

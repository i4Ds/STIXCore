"""Module for the different processing levels."""
import logging
from datetime import datetime

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.table import QTable

from stixcore.util.logging import get_logger

__all__ = ['SEC_IN_DAY', 'FitsProcessor', 'FitsLBProcessor']


logger = get_logger(__name__, level=logging.DEBUG)

SEC_IN_DAY = 24 * 60 * 60


class FitsProcessor:
    # TODO abstract some general processing pattern methods

    @classmethod
    def generate_filename(cls, product, version, status=''):
        """
        Generate fits file name with SOLO conventions.
        Parameters
        ----------
        product : `BaseProduct`
            Product
        version : `int`
            Version of this product
        status : `str`
            Status of the packets
        Returns
        -------
        `str`
            The filename
        """
        user_req = getattr(product.control, 'request_id', '')
        if user_req:
            user_req = f'_{user_req}'

        scet_obs = (product.control["scet_coarse"][0] // SEC_IN_DAY) * SEC_IN_DAY
        name = "-".join([str(x) for x in (getattr(product, 'type', ()))])
        return f'solo_{product.level}_stix-{name}' \
               f'_{scet_obs}_V{version:02d}{status}.fits'

    @classmethod
    def generate_primary_header(cls, product, filename):
        """
        Generate primary header cards.
        Parameters
        ----------
        product : `BaseProduct`
            the product the FITS will contain.
        filename : `str`
            the filename of the FITS file.
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
            ('VERS_SW', 1, 'Software version'),
            ('TYPE', "-".join([str(x) for x in (getattr(product, 'type', ()))]),
             'Packet Type service-subservice[-ssid])')
        )
        return headers


class FitsLBProcessor(FitsProcessor):
    """Class representing a FITS processor for LevelB products."""

    def __init__(self, archive_path):
        """Create a new processor object for the given directory.
        Parameters
        ----------
        archive_path : `Path`
            The root path where the FITS file should be generated.
        """
        self.archive_path = archive_path

    def write_fits(self, product):
        """Write or merge the product data into a FITS file.
        Parameters
        ----------
        product : `LevelB`
            The data product to write.
        Raises
        ------
        ValueError
            TODO what does the length check guaranties?
        ValueError
            TODO what does the length check guaranties?
        """
        for prod in product.to_days():
            filename = self.generate_filename(prod, version=1)
            parts = [prod.level]
            parts.extend(prod.type)
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)
            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = prod.from_fits(fitspath)
                existing.type = prod.type
                if np.abs([((len(existing.data['data'][i])/2) -
                            (existing.control['data_length'][i]+7))
                          for i in range(len(existing.data))]).sum() > 0:
                    raise ValueError()
                logger.debug('Existing %s \n New %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            # control = unique(prod.control, ['scet_coarse', 'scet_fine', 'seq_count'])
            # data = prod.data[np.isin(prod.data['control_index'], control['index'])]

            control = prod.control
            data = prod.data

            if np.abs([((len(data['data'][i]) / 2) - (control['data_length'][i] + 7))
                       for i in range(len(data))]).sum() > 0:
                raise ValueError()

            primary_header = self.generate_primary_header(prod, filename)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIXCore'})

            control_hdu = fits.BinTableHDU(control)
            control_hdu.name = 'CONTROL'
            data_hdu = fits.BinTableHDU(data)
            data_hdu.name = 'DATA'
            hdul = fits.HDUList([primary_hdu, control_hdu, data_hdu])

            logger.info(f'Writing fits file to {path / filename}')
            hdul.writeto(path / filename, overwrite=True, checksum=True)


class FitsL0Processor:
    """
    FITS level 1 processor
    """
    def __init__(self, archive_path):
        """
        Create a new processor object.

        Parameters
        ----------
        archive_path : `pathlib.Path`
        """
        self.archive_path = archive_path

    def write_fits(self, product):
        """
        Write level 1 products into fits files.

        Parameters
        ----------
        product : ``

        Returns
        -------

        """
        if callable(getattr(product, 'to_days', None)):
            products = product.to_days()
        else:
            products = product.to_requests()

        for prod in products:
            filename = self.generate_filename(product=prod, version=1)
            # start_day = np.floor((prod.obs_beg.as_float()
            #                       // (1 * u.day).to('s')).value * SEC_IN_DAY).astype(int)
            parts = ['L0', prod.service_type, prod.service_subtype,
                     prod.pi1_val]
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)

            path.mkdir(parents=True, exist_ok=True)
            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = prod.from_fits(fitspath)
                logger.debug('Existing %s \n Current %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            control = prod.control
            data = prod.data

            elow, ehigh = prod.get_energies()

            energies = QTable()
            energies['channel'] = range(len(elow))
            energies['e_low'] = elow * u.keV
            energies['e_high'] = ehigh * u.keV

            # Convert time to be relative to start date
            data['time'] = (data['time'] - prod.obs_beg.as_float()).to(u.s)

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            control_hdu = fits.BinTableHDU(control)
            control_hdu.name = 'CONTROL'
            data_hdu = fits.BinTableHDU(data)
            data_hdu.name = 'DATA'
            energy_hdu = fits.BinTableHDU(energies)
            energy_hdu.name = 'ENERGIES'

            hdul = fits.HDUList([primary_hdu, control_hdu, data_hdu, energy_hdu])

            logger.debug(f'Writing fits file to {path / filename}')
            hdul.writeto(path / filename, overwrite=True, checksum=True)

    @staticmethod
    def generate_filename(product=None, version=None, status=''):
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
        status = ''
        if status:
            status = f'_{status}'

        user_req = ''
        if 'request_id' in product.control.colnames:
            user_req = f"-{product.control['request_id'][0]}"

        tc_control = ''
        if 'tc_packet_seq_control' in product.control.colnames and user_req != '':
            tc_control = f'_{product.control["tc_packet_seq_control"][0]}'

        if product.type == 'ql':
            date_range = f'{((product.obs_avg.coarse // (24 * 60 * 60)) ) * 24 * 60 * 60:010d}'
        else:
            start_obs = product.obs_beg.to_datetime().strftime("%Y%m%dT%H%M%S")
            end_obs = product.obs_end.to_datetime().strftime("%Y%m%dT%H%M%S")
            date_range = f'{start_obs}-{end_obs}'
        return f'solo_{product.level}_stix-{product.type}-' \
               f'{product.name.replace("_", "-")}{user_req}' \
               f'_{date_range}_V{version:02d}{status}{tc_control}.fits'

    @staticmethod
    def generate_primary_header(filename, product):
        """
        Generate primary header cards.

        filename : str
            Filename
        product : `stixcore.product.quicklook.Product`
            Product

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
            ('OBT_BEG', str(product.obs_beg)),
            ('OBT_END', str(product.obs_end)),
            ('TIMESYS', 'UTC', 'System used for time keywords'),
            ('LEVEL', 'L1', 'Processing level of the data'),
            ('ORIGIN', 'STIX Team, FHNW', 'Location where file has been generated'),
            ('CREATOR', 'STIX-SWF', 'FITS creation software'),
            ('VERSION', 1, 'Version of data product'),
            ('OBS_MODE', 'Nominal '),
            ('VERS_SW', 1, 'Software version'),
            ('DATE_OBS', str(product.obs_beg),
             'Start of acquisition time in UT'),
            ('DATE_BEG', str(product.obs_beg)),
            ('DATE_AVG', str(product.obs_avg)),
            ('DATE_END', str(product.obs_end)),
            # ('MJDREF', product.obs_beg.mjd),
            # ('DATEREF', product.obs_beg.fits),
            ('OBS_TYPE', 'LC'),
            # TODO figure out where this info will come from
            ('SOOP_TYP', 'SOOP'),
            ('OBS_ID', 'obs_id'),
            ('TARGET', 'Sun'),
            ('STYPE', product.service_type),
            ('SSTYPE', product.service_subtype),
            ('SSID', product.pi1_val),
            ('SPID', product.spid)
        )
        return headers

"""Module for the different processing levels."""
import logging
from datetime import datetime

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import QTable

import stixcore
from stixcore.products.product import Product
from stixcore.util.logging import get_logger

__all__ = ['SEC_IN_DAY', 'FitsProcessor', 'FitsLBProcessor', 'FitsL0Processor', 'FitsL1Processor']


logger = get_logger(__name__, level=logging.DEBUG)

SEC_IN_DAY = 24 * 60 * 60


class FitsProcessor:
    # TODO abstract some general processing pattern methods

    @classmethod
    def generate_filename(cls, product, *, version, date_range, status=''):
        """
        Generate fits file name with SOLO conventions.
        Parameters
        ----------
        product : `BaseProduct`
            QLProduct
        version : `int`
            Version of this product
        status : `str`
            Status of the packets
        Returns
        -------
        `str`
            The filename
        """
        if status:
            status = f'_{status}'

        user_req = ''
        if 'request_id' in product.control.colnames:
            user_req = f"-{product.control['request_id'][0]}"

        tc_control = ''
        if 'tc_packet_seq_control' in product.control.colnames and user_req != '':
            tc_control = f'_{product.control["tc_packet_seq_control"][0]}'

        return f'solo_{product.level}_stix-{product.type}-' \
               f'{product.name.replace("_", "-")}{user_req}' \
               f'_{date_range}_V{version:02d}{status}{tc_control}.fits'

    @classmethod
    def generate_common_header(cls, filename, product):
        headers = (
            # Name, Value, Comment
            ('TELESCOP', 'SOLO/STIX', 'Telescope/Sensor name'),
            ('INSTRUME', 'STIX', 'Instrument name'),
            ('OBSRVTRY', 'Solar Orbiter', 'Satellite name'),
            ('FILENAME', filename, 'FITS filename'),
            ('DATE', datetime.now().isoformat(timespec='milliseconds'),
                'FITS file creation date in UTC'),
            ('ORIGIN', 'STIX Team, FHNW', 'Location where file has been generated'),
            ('CREATOR', 'stixcore', 'FITS creation software'),
            ('VERSION', 1, 'Version of data product'),
            ('OBS_MODE', 'Nominal '),
            ('VERS_SW', str(stixcore.__version__), 'Software version'),
            # TODO figure out where this info will come from
            ('SOOP_TYP', 'SOOP'),
            ('OBS_ID', 'obs_id'),
            ('TARGET', 'Sun'),
            ('OBS_TYPE', product.type),
            ('STYPE', product.service_type),
            ('SSTYPE', product.service_subtype),
            ('SSID', product.ssid if product.ssid is not None else '')
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

    @classmethod
    def generate_filename(cls, product, *, version, status=''):
        """
        Generate fits file name with SOLO conventions.
        Parameters
        ----------
        product : `BaseProduct`
            QLProduct
        version : `int`
            Version of this product
        status : `str`
            Status of the packets
        Returns
        -------
        `str`
            The filename
        """
        scet_obs = int(product.obt_avg.as_float().value // SEC_IN_DAY) * SEC_IN_DAY

        parts = [str(x) for x in [product.service_type, product.service_subtype, product.ssid]]
        if product.ssid is None:
            parts = parts[:-1]
        name = '-'.join(parts)
        return f'solo_{product.level}_stix-{name}' \
               f'_{scet_obs:010d}_V{version:02d}{status}.fits'

    @classmethod
    def generate_primary_header(cls, filename, product):
        """
        Generate primary header cards.
        Parameters
        ----------
        filename : `str`
            the filename of the FITS file.
        product : `BaseProduct`
            the product the FITS will contain.
        Returns
        -------
        tuple
            List of header cards as tuples (name, value, comment)
        """
        # if product.level != 'LB':
        #     raise ValueError(f"Try to crate FITS file LB for {product.level} data product")
        # if 'scet_coarse' not in product.control:
        #     raise ValueError("Expected scet_coarse in the control structure")

        headers = FitsProcessor.generate_common_header(filename, product) + (
            # Name, Value, Comment
            ('OBT_BEG', product.obt_beg.to_string(), 'Start of acquisition time in OBT'),
            ('OBT_END', product.obt_end.to_string(), 'End of acquisition time in OBT'),
            ('TIMESYS', 'OBT', 'System used for time keywords'),
            ('LEVEL', 'LB', 'Processing level of the data'),
            ('DATE_OBS', product.obt_beg.to_string(), 'Start of acquisition time in OBT'),
            ('DATE_BEG', product.obt_beg.to_string(), 'Start of acquisition time in OBT'),
            ('DATE_END', product.obt_end.to_string(), 'End of acquisition time in OBT')
        )
        return headers

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
        files = []
        for prod in product.to_days():
            filename = self.generate_filename(prod, version=1)
            parts = [prod.level, prod.service_type, prod.service_subtype, prod.ssid]
            if prod.ssid is None:
                parts = parts[:-1]
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)
            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = Product(fitspath)
                # existing.type = prod.type
                if np.abs([((len(existing.data['data'][i])/2) -
                            (existing.control['data_length'][i]+7))
                          for i in range(len(existing.data))]).sum() > 0:
                    raise ValueError()
                logger.debug('Existing %s, New %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            # control = unique(prod.control, ['scet_coarse', 'scet_fine', 'seq_count'])
            # data = prod.data[np.isin(prod.data['control_index'], control['index'])]

            control = prod.control
            data = prod.data

            if np.abs([((len(data['data'][i]) / 2) - (control['data_length'][i] + 7))
                       for i in range(len(data))]).sum() > 0:
                raise ValueError()

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIXCore'})

            control_hdu = fits.BinTableHDU(control)
            control_hdu.name = 'CONTROL'
            data_hdu = fits.BinTableHDU(data)
            data_hdu.name = 'DATA'
            hdul = fits.HDUList([primary_hdu, control_hdu, data_hdu])

            logger.info(f'Writing fits file to {path / filename}')
            fullpath = path / filename
            hdul.writeto(fullpath, overwrite=True, checksum=True)
            files.append(fullpath)

        return files


class FitsL0Processor:
    """
    FITS level 0 processor
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
        Write level 0 products into fits files.

        Parameters
        ----------
        product : `stixcore.product.level0`

        Returns
        -------
        list
            of created file as `pathlib.Path`

        """
        created_files = []
        if callable(getattr(product, 'to_days', None)):
            products = product.to_days()
        else:
            products = product.to_requests()

        for prod in products:
            filename = self.generate_filename(product=prod, version=1)
            # start_day = np.floor((prod.obs_beg.as_float()
            #                       // (1 * u.day).to('s')).value * SEC_IN_DAY).astype(int)
            parts = [prod.level, prod.service_type, prod.service_subtype,
                     prod.ssid]
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)

            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = Product(fitspath)
                logger.debug('Existing %s, Current %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            control = prod.control
            data = prod.data

            idb_versions = QTable(rows=[(version, range.start.as_float(), range.end.as_float())
                                  for version, range in product.idb_versions.items()],
                                  names=["version", "obt_start", "obt_end"])

            # Convert time to be relative to start date
            data['time'] = (data['time'] - prod.scet_timerange.start).as_float()
            data['timedel'] = data['timedel'].as_float()
            try:
                control['time_stamp'] = control['time_stamp'].as_float()
            except KeyError as e:
                if 'time_stamp' not in repr(e):
                    raise e

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            control_enc = fits.connect._encode_mixins(control)
            control_hdu = table_to_hdu(control_enc)
            control_hdu.name = 'CONTROL'
            data_enc = fits.connect._encode_mixins(data)
            data_hdu = table_to_hdu(data_enc)
            data_hdu.name = 'DATA'
            idb_enc = fits.connect._encode_mixins(idb_versions)
            idb_hdu = table_to_hdu(idb_enc)
            idb_hdu.name = 'IDB_VERSIONS'

            hdul = [primary_hdu, control_hdu, data_hdu, idb_hdu]

            if getattr(prod, 'get_energies', False) is not False:
                elow, ehigh = prod.get_energies()
                energies = QTable()
                energies['channel'] = range(len(elow))
                energies['e_low'] = elow * u.keV
                energies['e_high'] = ehigh * u.keV

                energy_enc = fits.connect._encode_mixins(energies)
                energy_hdu = table_to_hdu(energy_enc)
                energy_hdu.name = 'ENERGIES'
                hdul.append((energy_hdu))

            hdul = fits.HDUList(hdul)

            filetowrite = path / filename
            logger.debug(f'Writing fits file to {filetowrite}')
            hdul.writeto(filetowrite, overwrite=True, checksum=True)
            created_files.append(filetowrite)
        return created_files

    @staticmethod
    def generate_filename(product, version=None, status=''):
        """
        Generate fits file name with SOLO conventions.

        Parameters
        ----------
        product : stix_parser.product.BaseProduct
            QLProduct
        version : int
            Version of this product
        status : str
            Status of the packets

        Returns
        -------
        str
            The filename
        """

        if product.type != 'sci' or product.name == 'burst-aspect':
            date_range = f'{(product.scet_timerange.avg.coarse // (24 * 60 * 60) ) *24*60* 60:010d}'
        else:
            start_obs = product.scet_timerange.start.to_string(sep='f')
            end_obs = product.scet_timerange.end.to_string(sep='f')
            date_range = f'{start_obs}-{end_obs}'
        return FitsProcessor.generate_filename(product, version=version,
                                               date_range=date_range, status=status)

    @classmethod
    def generate_primary_header(cls, filename, product):
        """
        Generate primary header cards.

        filename : str
            Filename
        product : `stixcore.product.quicklook.QLProduct`
            QLProduct

        Returns
        -------
        tuple
            List of header cards as tuples (name, value, comment)
        """
        # if product.level != 'L0':
        #     raise ValueError(f"Try to crate FITS file L0 for {product.level} data product")
        # if not isinstance(product.obt_beg, SCETime):
        #     raise ValueError("Expected SCETime as time format")

        headers = FitsProcessor.generate_common_header(filename, product) + (
            # Name, Value, Comment
            # ('MJDREF', product.obs_beg.mjd),
            # ('DATEREF', product.obs_beg.fits),
            ('OBT_BEG', product.scet_timerange.start.to_string()),
            ('OBT_END', product.scet_timerange.end.to_string()),
            ('TIMESYS', 'OBT', 'System used for time keywords'),
            ('LEVEL', 'L0', 'Processing level of the data'),
            ('DATE_OBS', product.scet_timerange.start.to_string(),
             'Start of acquisition time in UT'),
            ('DATE_BEG', product.scet_timerange.start.to_string()),
            ('DATE_AVG', product.scet_timerange.avg.to_string()),
            ('DATE_END', product.scet_timerange.end.to_string())
        )

        return headers


class FitsL1Processor(FitsL0Processor):
    def __init__(self, archive_path):
        self.archive_path = archive_path

    @classmethod
    def generate_filename(cls, product, *, version, status=''):

        date_range = f'{product.utc_timerange.start.strftime("%Y%m%dT%H%M%S")}_' +\
                     f'{product.utc_timerange.end.strftime("%Y%m%dT%H%M%S")}'
        if product.type in ['ql', 'hk'] or product.name == 'burst-aspect':
            date_range = product.utc_timerange.center.strftime("%Y%m%d")

        return FitsProcessor.generate_filename(product, version=version, date_range=date_range,
                                               status=status)

    @classmethod
    def generate_primary_header(cls, filename, product):
        if product.level != 'L1':
            raise ValueError(f"Try to crate FITS file L1 for {product.level} data product")

        headers = FitsProcessor.generate_common_header(filename, product) + (
            # Name, Value, Comment
            ('OBT_BEG', product.scet_timerange.start.to_string(),
             'Start of acquisition time in OBT'),
            ('OBT_END', product.scet_timerange.end.to_string(), 'End of acquisition time in OBT'),
            ('TIMESYS', 'UTC', 'System used for time keywords'),
            ('LEVEL', 'L1', 'Processing level of the data'),
            ('DATE_OBS', product.utc_timerange.start.fits, 'Start of acquisition time in UTC'),
            ('DATE_BEG', product.utc_timerange.start.fits, 'Start of acquisition time in UTC'),
            ('DATE_AVG', product.utc_timerange.center.fits, 'Center of acquisition time in UTC'),
            ('DATE_END', product.utc_timerange.end.fits, 'End of acquisition time in UTC')
        )
        return headers

    def write_fits(self, product):
        """
        Write level 0 products into fits files.

        Parameters
        ----------
        product : `stixcore.product.level0`

        Returns
        -------
        list
            of created file as `pathlib.Path`

        """
        created_files = []
        if callable(getattr(product, 'to_days', None)):
            products = product.to_days()
        else:
            products = product.to_requests()

        for prod in products:
            filename = self.generate_filename(product=prod, version=1)
            # start_day = np.floor((prod.obs_beg.as_float()
            #                       // (1 * u.day).to('s')).value * SEC_IN_DAY).astype(int)

            parts = [prod.level, prod.utc_timerange.center.strftime('%Y/%m/%d'), prod.type.upper()]
            path = self.archive_path.joinpath(*[str(x) for x in parts])
            path.mkdir(parents=True, exist_ok=True)

            fitspath = path / filename
            if fitspath.exists():
                logger.info('Fits file %s exists appending data', fitspath.name)
                existing = Product(fitspath)
                logger.debug('Existing %s, Current %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            control = prod.control
            data = prod.data

            idb_versions = QTable(rows=[(version, range.start.as_float(), range.end.as_float())
                                  for version, range in product.idb_versions.items()],
                                  names=["version", "obt_start", "obt_end"])

            elow, ehigh = prod.get_energies()
            energies = QTable()
            energies['channel'] = range(len(elow))
            energies['e_low'] = elow * u.keV
            energies['e_high'] = ehigh * u.keV

            # Convert time to be relative to start date
            data['time'] = (data['time'] - prod.scet_timerange.start).as_float()
            data['timedel'] = data['timedel'].as_float()
            try:
                control['time_stamp'] = control['time_stamp'].as_float()
            except KeyError as e:
                if 'time_stamp' not in repr(e):
                    raise e

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            control_enc = fits.connect._encode_mixins(control)
            control_hdu = table_to_hdu(control_enc)
            control_hdu.name = 'CONTROL'
            data_enc = fits.connect._encode_mixins(data)
            data_hdu = table_to_hdu(data_enc)
            data_hdu.name = 'DATA'
            idb_enc = fits.connect._encode_mixins(idb_versions)
            idb_hdu = table_to_hdu(idb_enc)
            idb_hdu.name = 'IDB_VERSIONS'

            energy_enc = fits.connect._encode_mixins(energies)
            energy_hdu = table_to_hdu(energy_enc)
            energy_hdu.name = 'ENERGIES'

            hdul = fits.HDUList([primary_hdu, control_hdu, data_hdu, idb_hdu, energy_hdu])

            filetowrite = path / filename
            logger.debug(f'Writing fits file to {filetowrite}')
            hdul.writeto(filetowrite, overwrite=True, checksum=True)
            created_files.append(filetowrite)
        return created_files

"""Module for the different processing levels."""
import logging
from pathlib import Path
from datetime import datetime

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import QTable

import stixcore
from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice
from stixcore.products.product import Product
from stixcore.soop.manager import SOOPManager, SoopObservationType
from stixcore.util.logging import get_logger

__all__ = ['SEC_IN_DAY', 'FitsProcessor', 'FitsLBProcessor', 'FitsL0Processor', 'FitsL1Processor']


logger = get_logger(__name__, level=logging.WARNING)

SEC_IN_DAY = 24 * 60 * 60


def set_bscale_unsigned(table_hdu):
    """
    Set bscale value to 1 if unsigned int.

    For mrdfits compatibility need have bscale set to 1 when using unsigned int convention
    """
    for col in table_hdu.columns:
        if col.bzero and col.bscale is None:
            col.bscale = 1

    return table_hdu


def add_default_tuint(table_hdu):
    """
    Add a default empty string tunit if not already defined

    Parameters
    ----------
    table_hdu : `astropy.io.fits.BinTableHDU`
        Binary table to add tunit to

    Returns
    -------
    `astropy.io.fits.BinTableHDU`
        Table with added tunits kewords
    """
    for col in table_hdu.columns:
        if not col.unit:
            col.unit = ' '
    return table_hdu


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
            user_req = f"_{product.control['request_id'][0]}"

        tc_control = ''
        if 'tc_packet_seq_control' in product.control.colnames and user_req != '':
            tc_control = f'-{product.control["tc_packet_seq_control"][0]}'

        return f'solo_{product.level}_stix-{product.type}-{product.name.replace("_", "-")}' \
               f'_{date_range}_V{version:02d}{status}{user_req}{tc_control}.fits'

    @classmethod
    def generate_common_header(cls, filename, product):
        headers = (
            # Name, Value, Comment
            ('FILENAME', filename, 'FITS filename'),
            ('RAW_FILE', ';'.join(list(product.raw)), 'Raw filename(s)'),
            ('PARENT', ';'.join(list(product.parent)), 'Source file current data product'),
            # ('APID', '', 'APIC number of associated TM'),
            ('DATE', datetime.now().isoformat(timespec='milliseconds'),
             'FITS file creation date in UTC'),

            ('LEVEL', product.level, 'Data processing level'),
            ('ORIGIN', 'STIX Team, FHNW', 'FHNW'),
            ('CREATOR', 'stixcore', 'FITS creation software'),
            ('VERS_SW', str(stixcore.__version__), 'Version of SW that provided FITS file'),
            # ('VERS_CAL', '', 'Version of the calibration pack'),
            ('VERSION', 1, 'Version of data product'),
            ('OBSRVTRY', 'Solar Orbiter', 'Satellite name'),
            ('TELESCOP', 'SOLO/STIX', 'Telescope/Sensor name'),
            ('INSTRUME', 'STIX', 'Instrument name'),
            # ('XPOSURE', '', '[s] Total effective exposure time'),

            ('STYPE', product.service_type, 'Service Type'),
            ('SSTYPE', product.service_subtype, 'Sub-service Type'),
            ('SSID', product.ssid if product.ssid is not None else '', 'Science Structure ID'),
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
            If the data length in the header and actual data length differ
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
                if np.abs([((len(existing.data['data'][i])/2) -
                            (existing.control['data_length'][i]+7))
                          for i in range(len(existing.data))]).sum() > 0:
                    raise ValueError('Header data lengths and data lengths do not agree')
                logger.debug('Existing %s, New %s', existing, prod)
                prod = prod + existing
                logger.debug('Combined %s', prod)

            # control = unique(prod.control, ['scet_coarse', 'scet_fine', 'seq_count'])
            # data = prod.data[np.isin(prod.data['control_index'], control['index'])]

            control = prod.control
            data = prod.data

            if np.abs([((len(data['data'][i]) / 2) - (control['data_length'][i] + 7))
                       for i in range(len(data))]).sum() > 0:
                raise ValueError('Header data lengths and data lengths do not agree')

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
            parts = [prod.level, prod.service_type, prod.service_subtype]
            if prod.ssid is not None:
                parts.append(prod.ssid)
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

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            # Convert time to be relative to start date
            # it is important that the change to the relative time is done after the header is
            # generated as this will use the original SCET time data

            # In TM sent as uint in units of 0.1 so convert back
            data['time'] = np.uint32(np.around((data['time']
                                                - prod.scet_timerange.start).as_float()).to(u.ds))
            data['timedel'] = np.uint32(np.around(data['timedel'].as_float()).to(u.ds))
            try:
                control['time_stamp'] = control['time_stamp'].as_float()
            except KeyError as e:
                if 'time_stamp' not in repr(e):
                    raise e

            control_enc = fits.connect._encode_mixins(control)
            control_hdu = table_to_hdu(control_enc)
            control_hdu = set_bscale_unsigned(control_hdu)
            control_hdu = add_default_tuint(control_hdu)
            control_hdu.name = 'CONTROL'

            data_enc = fits.connect._encode_mixins(data)
            data_hdu = table_to_hdu(data_enc)
            data_hdu = set_bscale_unsigned(data_hdu)
            data_hdu = add_default_tuint(data_hdu)
            data_hdu.name = 'DATA'

            idb_enc = fits.connect._encode_mixins(idb_versions)
            idb_hdu = table_to_hdu(idb_enc)
            idb_hdu = set_bscale_unsigned(idb_hdu)
            idb_hdu = add_default_tuint(idb_hdu)
            idb_hdu.name = 'IDB_VERSIONS'

            hdul = [primary_hdu, control_hdu, data_hdu, idb_hdu]

            if getattr(prod, 'get_energies', False) is not False:
                elow, ehigh = prod.get_energies()
                energies = QTable()
                energies['channel'] = np.float16(range(len(elow)))
                energies['e_low'] = np.float16(elow * u.keV)
                energies['e_high'] = np.float16(ehigh * u.keV)

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
        dmin = 0.0
        dmax = 0.0
        bunit = ' '
        if 'counts' in product.data.colnames:
            dmax = product.data['counts'].max().value
            dmin = product.data['counts'].min().value
            bunit = 'counts'

        headers = FitsProcessor.generate_common_header(filename, product) + (
            # Name, Value, Comment
            # ('MJDREF', product.obs_beg.mjd),
            # ('DATEREF', product.obs_beg.fits),
            ('OBT_BEG', product.scet_timerange.start.as_float().value,
             'Start acquisition time in OBT'),
            ('OBT_END', product.scet_timerange.end.as_float().value, 'End acquisition time in OBT'),
            ('TIMESYS', 'OBT', 'System used for time keywords'),
            ('LEVEL', 'L0', 'Processing level of the data'),
            ('DATE_OBS', product.scet_timerange.start.to_string(), 'Depreciated, same as DATE-BEG'),
            ('DATE_BEG', product.scet_timerange.start.to_string(), 'Start time of observation'),
            ('DATE_AVG', product.scet_timerange.avg.to_string(), 'Average time of observation'),
            ('DATE_END', product.scet_timerange.end.to_string(), 'End time of observation'),
            ('DATAMIN', dmin, 'Minimum valid physical value'),
            ('DATAMAX', dmax, 'Maximum valid physical value'),
            ('BUNIT', bunit, 'Units of physical value, after application of BSCALE, BZERO')
        )

        return headers


class FitsL1Processor(FitsL0Processor):
    def __init__(self, archive_path):
        self.archive_path = archive_path
        soop_path = CONFIG.get('Paths', 'soop_files')
        if str(soop_path) == '':
            soop_path = Path(__file__).parent.parent.parent / 'data' / 'test' / 'soop'
        self.soop_manager = SOOPManager(soop_path)

    @classmethod
    def generate_filename(cls, product, *, version, status=''):

        date_range = f'{product.utc_timerange.start.strftime("%Y%m%dT%H%M%S")}_' +\
                     f'{product.utc_timerange.end.strftime("%Y%m%dT%H%M%S")}'
        if product.type != 'sci' or product.name == 'burst-aspect':
            date_range = product.utc_timerange.center.strftime("%Y%m%d")

        return FitsProcessor.generate_filename(product, version=version, date_range=date_range,
                                               status=status)

    def generate_primary_header(self, filename, product):
        if product.level != 'L1':
            raise ValueError(f"Try to crate FITS file L1 for {product.level} data product")

        headers = FitsProcessor.generate_common_header(filename, product)

        dmin = 0.0
        dmax = 0.0
        bunit = ' '
        exposure = 0.0
        if 'counts' in product.data.colnames:
            dmax = product.data['counts'].max().value
            dmin = product.data['counts'].min().value
            bunit = 'counts'
            exposure = product.data['timedel'].as_float().min().to_value('s')
        data_headers = (
            ('DATAMIN', dmin, 'Minimum valid physical value'),
            ('DATAMAX', dmax, 'Maximum valid physical value'),
            ('BUNIT', bunit, 'Units of physical value, after application of BSCALE, BZERO'),
            ('XPOSURE', exposure, '[s] Total effective exposure time')
        )

        soop_keywords = self.soop_manager.get_keywords(start=product.utc_timerange.start,
                                                       end=product.utc_timerange.end,
                                                       otype=SoopObservationType.ALL)
        soop_headers = tuple(kw.tuple for kw in soop_keywords)
        soop_defaults = (
            ('OBS_MODE', '', 'Observation mode'),
            ('OBS_TYPE', '', 'Encoded version of OBS_MODE'),
            ('OBS_ID', '', 'Unique ID of the individual observation'),
            ('SOOPNAME', '', 'Name of the SOOP Campaign that the data belong to'),
            ('SOOPTYPE', '', 'Campaign ID(s) that the data belong to'),
            ('TARGET', '', 'Type of target from planning')
        )

        soop_key_names = [sh[0] for sh in soop_headers]
        for default in soop_defaults:
            if default[0] not in soop_key_names:
                soop_headers += tuple([default])

        time_headers = (
            # Name, Value, Comment
            ('OBT_BEG', product.scet_timerange.start.as_float().value,
             'Start of acquisition time in OBT'),
            ('OBT_END', product.scet_timerange.end.as_float().value,
             'End of acquisition time in OBT'),
            ('TIMESYS', 'UTC', 'System used for time keywords'),
            ('LEVEL', 'L1', 'Processing level of the data'),
            ('DATE_OBS', product.utc_timerange.start.fits, 'Start of acquisition time in UTC'),
            ('DATE_BEG', product.utc_timerange.start.fits, 'Start of acquisition time in UTC'),
            ('DATE_AVG', product.utc_timerange.center.fits, 'Center of acquisition time in UTC'),
            ('DATE_END', product.utc_timerange.end.fits, 'End of acquisition time in UTC')
        )

        ephemeris_headers = \
            Spice.instance.get_fits_headers(start_time=product.utc_timerange.start,
                                            average_time=product.utc_timerange.center)

        return headers + data_headers + soop_headers + time_headers + ephemeris_headers

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

            primary_header = self.generate_primary_header(filename, prod)
            primary_hdu = fits.PrimaryHDU()
            primary_hdu.header.update(primary_header)
            primary_hdu.header.update({'HISTORY': 'Processed by STIX'})

            # Convert time to be relative to start date
            # it is important that the change to the relative time is done after the header is
            # generated as this will use the original SCET time data

            # In TM sent as uint in units of 0.1 so convert back
            data['time'] = np.uint32(np.around((data['time']
                                                - prod.scet_timerange.start).as_float()).to(u.ds))
            data['timedel'] = np.uint32(np.around(data['timedel'].as_float()).to(u.ds))

            try:
                control['time_stamp'] = control['time_stamp'].as_float()
            except KeyError as e:
                if 'time_stamp' not in repr(e):
                    raise e

            control_enc = fits.connect._encode_mixins(control)
            control_hdu = table_to_hdu(control_enc)
            control_hdu = set_bscale_unsigned(control_hdu)
            control_hdu = add_default_tuint(control_hdu)
            control_hdu.name = 'CONTROL'

            data_enc = fits.connect._encode_mixins(data)
            data_hdu = table_to_hdu(data_enc)
            data_hdu = set_bscale_unsigned(data_hdu)
            data_hdu = add_default_tuint(data_hdu)
            data_hdu.name = 'DATA'

            idb_enc = fits.connect._encode_mixins(idb_versions)
            idb_hdu = table_to_hdu(idb_enc)
            idb_hdu = add_default_tuint(idb_hdu)
            idb_hdu.name = 'IDB_VERSIONS'

            hdus = [primary_hdu, control_hdu, data_hdu, idb_hdu]

            if getattr(prod, 'get_energies', False) is not False:
                elow, ehigh = prod.get_energies()
                energies = QTable()
                energies['channel'] = np.float16(range(len(elow)))
                energies['e_low'] = np.float16(elow * u.keV)
                energies['e_high'] = np.float16(ehigh * u.keV)

                energy_enc = fits.connect._encode_mixins(energies)
                energy_hdu = table_to_hdu(energy_enc)
                energy_hdu.name = 'ENERGIES'
                hdus.append(energy_hdu)

            hdul = fits.HDUList(hdus)

            filetowrite = path / filename
            logger.debug(f'Writing fits file to {filetowrite}')
            hdul.writeto(filetowrite, overwrite=True, checksum=True)
            created_files.append(filetowrite)
        return created_files

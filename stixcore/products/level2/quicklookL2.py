"""
.
"""
import re
import subprocess
from pathlib import Path
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import Column, QTable

from stixcore.calibration.elut_manager import ELUTManager
from stixcore.config.config import CONFIG
from stixcore.ecc.manager import ECCManager
from stixcore.products.level0.quicklookL0 import QLProduct
from stixcore.products.product import EnergyChannelsMixin, GenericProduct, L2Mixin
from stixcore.time import SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ['LightCurve', 'Background', 'Spectra', 'Variance', 'FlareFlag',
           'EnergyCalibration', 'TMStatusFlareList']

logger = get_logger(__name__)


class LightCurve(QLProduct, L2Mixin):
    """Quick Look Light Curve data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'lightcurve'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 30)


class Background(QLProduct, L2Mixin):
    """Quick Look Background Light Curve data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'background'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 31)


class Spectra(QLProduct, L2Mixin):
    """Quick Look Spectra data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'spectra'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 32)


class Variance(QLProduct, L2Mixin):
    """Quick Look Variance data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'variance'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 33)


class FlareFlag(QLProduct, L2Mixin):
    """Quick Look Flare Flag and Location data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'flareflag'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 34)


class TMStatusFlareList(QLProduct, L2Mixin):
    """Quick Look TM Management status and Flare list data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'ql-tmstatusflarelist'
        self.level = 'L2'

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 43)


class EnergyCalibration(GenericProduct, EnergyChannelsMixin, L2Mixin):
    """Quick Look energy calibration data product.

    In level 2 format.
    """

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                         control=control, data=data, idb_versions=idb_versions, **kwargs)

        self.name = 'energy'
        self.level = 'L2'
        self.type = 'cal'

    @property
    def fits_daily_file(self):
        return True

    @property
    def dmin(self):
        # default for FITS HEADER
        return 4

    @property
    def dmax(self):
        # default for FITS HEADER
        return 150

    @property
    def bunit(self):
        # default for FITS HEADER
        return 'kEV'

    @property
    def exposure(self):
        # default for FITS HEADER
        return self.control['integration_time'].min().to_value(u.s)

    @property
    def max_exposure(self):
        # default for FITS HEADER
        return self.control['integration_time'].max().to_value(u.s)

    @classmethod
    def is_datasource_for(cls,  *, service_type, service_subtype, ssid, **kwargs):
        return (kwargs['level'] == 'L2' and service_type == 21
                and service_subtype == 6 and ssid == 41)

    def get_additional_extensions(self):
        return []

    @classmethod
    def from_level1(cls, l1product, parent='', idlprocessor=None):

        l2 = super().from_level1(l1product, parent=parent)[0]

        date = l2.utc_timerange.start.datetime
        ob_elut, sci_channels = ELUTManager.instance.get_elut(date)

        e_actual_list = []
        off_gain_list = []

        l2.control.add_column(Column(name='ob_elut_name', data=np.repeat('_' * 50, len(l2.control)),
                                     description="Name of the ELUT active on instrument"))

        for spec_idx, spec in enumerate(l2.data['counts']):
            if spec.shape != (32, 12, 1024):
                raise ValueError(f"Unexpected shape {spec.shape} for counts in {l1product.name}")

            spec_ecc = spec.reshape(12 * 32, 1024).T.astype(np.float32)

            spec_ecc_table = QTable(spec_ecc, names=["ch{:03d}".format(i) for i in range(12 * 32)])
            spec_ecc_table.add_column(np.arange(1024, dtype=np.float32) * u.Unit('adu'),
                                      index=0, name='PHA_A')
            spec_ecc_table.add_column((np.arange(1024, dtype=np.float32) + 0.5) * u.Unit('adu'),
                                      index=1, name='PHA_center')
            spec_ecc_table.add_column((np.arange(1024, dtype=np.float32) + 1) * u.Unit('adu'),
                                      index=2, name='PHA_B')

            spec_filename = l1product.fits_header['FILENAME']

            if not re.match(r'^solo_L1_stix-cal-energy_\d+_V.*.fits$', spec_filename):
                raise ValueError(f"Invalid filename {spec_filename} for energy calibration data")

            ecc_install_path = Path(CONFIG.get('ECC', 'ecc_path'))

            with ECCManager.instance.context(date) as ecc_run_context_path:
                spec_file = ecc_run_context_path / spec_filename
                erg_path = ecc_run_context_path / 'ECC_para.fits'
                bash_script = f"""#!/bin/bash
                              cd {ecc_run_context_path}

                              {ecc_install_path}/Bkg
                              {ecc_install_path}/ECC --f_obs "{spec_filename}"
                              """
                primary_hdu = fits.PrimaryHDU()
                spec_enc = fits.connect._encode_mixins(spec_ecc_table)
                spec_hdu = table_to_hdu(spec_enc)
                spec_hdu.name = 'SPEC_ECC'
                hdul = [primary_hdu, spec_hdu]
                hdul = fits.HDUList(hdul)
                hdul.writeto(spec_file, overwrite=True, checksum=True)

                if not spec_file.exists():
                    raise FileNotFoundError("Failed to write energy calibration "
                                            f"data in ECC format to {spec_file}")
                logger.info(f"Energy calibration data in ECC format written to {spec_file}")

                # Run bash script directly
                process = subprocess.run(["bash"], input=bash_script,
                                         text=True, capture_output=True)
                if process.returncode != 0:
                    raise RuntimeError(f"ECC Bash script failed: {process.stderr}")

                logger.info("ECC bash script executed successfully: %s", process.stdout)

                if not erg_path.exists():
                    raise FileNotFoundError(f"Failed to read ECC result file {erg_path}")

                with fits.open(erg_path) as hdul:
                    # Access a specific extension by index or name
                    erg_table = QTable(hdul[1].data)
                    off_gain = np.array([4.0 * erg_table["off"].value.reshape(32, 12),
                                         1.0 / (4.0 * erg_table["gain"].value.reshape(32, 12)),
                                         erg_table["goc"].value.reshape(32, 12)])
                    l2.control['ob_elut_name'][l2.data['control_index'][spec_idx]] = ob_elut.file
                    off_gain_list.append(off_gain)

                    gain = off_gain[1, :, :]
                    offset = off_gain[0, :, :]

                    adc = (offset[..., None] +
                           (sci_channels["Elower"].to_value() / gain[..., None]))\
                        .round().astype(np.uint16)
                    e_actual = (np.searchsorted(np.arange(4096), adc) - offset[..., None])\
                        * gain[..., None]
                    e_actual[:, :, -1] = np.inf
                    e_actual[:, :, 0] = 0.0
                    e_actual_list.append(e_actual)

            l2.data.add_column(Column(name='e_edges_actual', data=e_actual_list,
                                      description="actual energy edges fitted by ECC"))
            l2.data["e_edges_actual"].unit = u.keV
            l2.data.add_column(Column(name='ecc_offset_gain_goc', data=off_gain_list,
                                      description="result of the ecc fitting: offset, gain, goc"))

            del l2.data["counts_comp_err"]
            del l2.data["counts"]

        return [l2]

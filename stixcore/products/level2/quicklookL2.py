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

from stixcore.calibration.ecc_post_fit import ecc_post_fit
from stixcore.calibration.elut_manager import ELUTManager
from stixcore.config.config import CONFIG
from stixcore.ecc.manager import ECCManager
from stixcore.products.level0.quicklookL0 import QLProduct
from stixcore.products.product import EnergyChannelsMixin, GenericProduct, L2Mixin
from stixcore.time import SCETimeRange
from stixcore.util.logging import get_logger

__all__ = ["LightCurve", "Background", "Spectra", "Variance", "FlareFlag", "EnergyCalibration", "TMStatusFlareList"]

logger = get_logger(__name__)


class LightCurve(QLProduct, L2Mixin):
    """Quick Look Light Curve data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "lightcurve"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 30


class Background(QLProduct, L2Mixin):
    """Quick Look Background Light Curve data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "background"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 31


class Spectra(QLProduct, L2Mixin):
    """Quick Look Spectra data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "spectra"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 32


class Variance(QLProduct, L2Mixin):
    """Quick Look Variance data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "variance"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 33


class FlareFlag(QLProduct, L2Mixin):
    """Quick Look Flare Flag and Location data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "flareflag"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 34


class TMStatusFlareList(QLProduct, L2Mixin):
    """Quick Look TM Management status and Flare list data product.

    In level 2 format.
    """

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = "ql-tmstatusflarelist"
        self.level = "L2"

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L2" and service_type == 21 and service_subtype == 6 and ssid == 43


class EnergyCalibration(GenericProduct, EnergyChannelsMixin, L2Mixin):
    """Quick Look energy calibration data product.

    In level 2 format.
    """

    NAME = "energy"
    LEVEL = "L2"
    TYPE = "cal"

    def __init__(
        self, *, service_type, service_subtype, ssid, control, data, idb_versions=defaultdict(SCETimeRange), **kwargs
    ):
        super().__init__(
            service_type=service_type,
            service_subtype=service_subtype,
            ssid=ssid,
            control=control,
            data=data,
            idb_versions=idb_versions,
            **kwargs,
        )

        self.name = EnergyCalibration.NAME
        self.level = EnergyCalibration.LEVEL
        self.type = EnergyCalibration.TYPE

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
        return "kEV"

    @property
    def exposure(self):
        # default for FITS HEADER
        return self.control["integration_time"].min().to_value(u.s)

    @property
    def max_exposure(self):
        # default for FITS HEADER
        return self.control["integration_time"].max().to_value(u.s)

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == EnergyCalibration.LEVEL and service_type == 21 and service_subtype == 6 and ssid == 41

    def get_additional_extensions(self):
        return []

    @classmethod
    def from_level1(cls, l1product, parent="", idlprocessor=None):
        l2 = super().from_level1(l1product, parent=parent)[0]

        date = l2.utc_timerange.start.datetime
        ob_elut, sci_channels = ELUTManager.instance.get_elut(date)

        e_actual_list = []
        off_gain_list = []

        ecc_only_e_actual_list = []
        ecc_only_off_gain_list = []

        ecc_err_list = []
        gain_range_ok_list = []

        l2.control.add_column(
            Column(
                name="ob_elut_name",
                data=np.repeat("_" * 50, len(l2.control)),
                description="Name of the ELUT active on instrument",
            )
        )

        for spec_idx, spec in enumerate(l2.data["counts"]):
            if spec.shape != (32, 12, 1024):
                raise ValueError(f"Unexpected shape {spec.shape} for counts in {l1product.name}")

            all_spec = spec.sum(axis=1).sum(axis=0)
            all_spec_table_total_rate = QTable(
                [[np.int16(len(all_spec))], np.int32([all_spec])], names=["NUM_POINTS", "COUNTS"]
            )
            all_spec_rate = spec.reshape(12 * 32, 1024)
            all_spec_table_rate = QTable(
                [
                    np.repeat(np.arange(32), 12).astype(np.uint8),
                    np.tile(np.arange(12), 32).astype(np.uint8),
                    np.zeros(12 * 32, dtype=int).astype(np.uint8),
                    np.full(12 * 32, 1024, dtype=int).astype(np.int16),
                    all_spec_rate.astype(np.int32),
                ],
                names=["DETECTOR_ID", "PIXEL_ID", "SUBSPEC_ID", "NUM_POINTS", "COUNTS"],
            )

            spec_ecc = spec.reshape(12 * 32, 1024).T.astype(np.float32)

            spec_ecc_table = QTable(spec_ecc, names=[f"ch{i:03d}" for i in range(12 * 32)])
            spec_ecc_table.add_column(np.arange(1024, dtype=np.float32) * u.Unit("adu"), index=0, name="PHA_A")
            spec_ecc_table.add_column(
                (np.arange(1024, dtype=np.float32) + 0.5) * u.Unit("adu"), index=1, name="PHA_center"
            )
            spec_ecc_table.add_column((np.arange(1024, dtype=np.float32) + 1) * u.Unit("adu"), index=2, name="PHA_B")

            spec_filename = l1product.fits_header["FILENAME"]

            if not re.match(r"^solo_L1_stix-cal-energy_\d+_V.*.fits$", spec_filename):
                raise ValueError(f"Invalid filename {spec_filename} for energy calibration data")

            spec_filename = spec_filename.replace(".fits", "_ecc_in.fits")
            ecc_install_path = Path(CONFIG.get("ECC", "ecc_path"))

            with ECCManager.instance.context(date) as ecc_run_context:
                ecc_run_context_path, ecc_run_cfg = ecc_run_context

                spec_file = ecc_run_context_path / spec_filename
                all_file = ecc_run_context_path / "spec_all.fits"
                spec_all_erg = ecc_run_context_path / "spec_all_erg.fits"
                erg_path = ecc_run_context_path / "ECC_para.fits"
                bash_script = f"""#!/bin/bash
                              cd {ecc_run_context_path}

                              {ecc_install_path}/Bkg
                              {ecc_install_path}/ECC --f_obs "{spec_filename}"
                              {ecc_install_path}/STX_Calib spec_all.fits ECC_para.fits[1]
                              """
                primary_hdu = fits.PrimaryHDU()

                all_spec_enc = fits.connect._encode_mixins(all_spec_table_rate)
                all_spec = table_to_hdu(all_spec_enc)
                all_spec.name = "RATE"

                all_spec_total_enc = fits.connect._encode_mixins(all_spec_table_total_rate)
                all_spec_total = table_to_hdu(all_spec_total_enc)
                all_spec_total.name = "TOTAL_RATE"

                hdul = [primary_hdu, all_spec, all_spec_total]
                hdul = fits.HDUList(hdul)
                hdul.writeto(all_file, overwrite=True, checksum=True)

                primary_hdu = fits.PrimaryHDU()
                spec_enc = fits.connect._encode_mixins(spec_ecc_table)
                spec_hdu = table_to_hdu(spec_enc)
                spec_hdu.name = "SPEC_ECC"
                hdul = [primary_hdu, spec_hdu]
                hdul = fits.HDUList(hdul)
                hdul.writeto(spec_file, overwrite=True, checksum=True)

                if not spec_file.exists():
                    raise FileNotFoundError(f"Failed to write energy calibration data in ECC format to {spec_file}")
                logger.info(f"Energy calibration data in ECC format written to {spec_file}")

                # Run bash script directly
                process = subprocess.run(["bash"], input=bash_script, text=True, capture_output=True)
                if process.returncode != 0:
                    raise RuntimeError(f"ECC Bash script failed: {process.stderr}")

                logger.info("ECC bash script executed successfully: \n%s", process.stdout)

                if not erg_path.exists():
                    raise FileNotFoundError(f"Failed to read ECC result file {erg_path}")

                control_idx = l2.data["control_index"][spec_idx]
                livetime = l1product.control["live_time"][control_idx].to_value(u.s)
                ecc_pf_df, idx_ecc = ecc_post_fit(spec_all_erg, erg_path, livetime)
                logger.info(
                    "Run ecc post fit: replaced [%s %%] gain offset pairs with 'better fits'",
                    round((len(idx_ecc) - idx_ecc.sum()) / max(1, len(idx_ecc)) * 100, ndigits=1),
                )

                off_gain = np.array(
                    [
                        4.0 * ecc_pf_df["Offset_Cor"].values.reshape(32, 12),
                        1.0 / (4.0 * ecc_pf_df["Gain_Cor"].values.reshape(32, 12)),
                        ecc_pf_df["goc"].values.reshape(32, 12),
                    ]
                )
                off_gain_list.append(off_gain)

                off_gain_ecc = np.array(
                    [
                        4.0 * ecc_pf_df["Offset_ECC"].values.reshape(32, 12),
                        1.0 / (4.0 * ecc_pf_df["Gain_ECC"].values.reshape(32, 12)),
                        ecc_pf_df["goc"].values.reshape(32, 12),
                    ]
                )
                ecc_only_off_gain_list.append(off_gain_ecc)

                l2.control["ob_elut_name"][l2.data["control_index"][spec_idx]] = ob_elut.file

                ecc_err_list.append(
                    np.array(
                        [
                            ecc_pf_df["err_P31"].values.reshape(32, 12),
                            ecc_pf_df["err_dE31"].values.reshape(32, 12),
                            ecc_pf_df["err_P81"].values.reshape(32, 12),
                            ecc_pf_df["err_dE81"].values.reshape(32, 12),
                        ]
                    )
                )

                gain_range_ok = True
                for h in ecc_pf_df.index[ecc_pf_df["Gain_Prime"] > ecc_run_cfg.Max_Gain_Prime]:
                    det_pix_can = [ecc_pf_df["DET"][h], ecc_pf_df["PIX"][h]]
                    if det_pix_can not in ecc_run_cfg.Ignore_Max_Gain_Prime_Det_Pix_List:
                        logger.warning(
                            f"ECC result Gain_Prime {ecc_pf_df['Gain_Prime'][h]} "
                            f"for DET {det_pix_can[0]} PIX {det_pix_can[1]} exceeds "
                            f"Max_Gain {ecc_run_cfg.Max_Gain_Prime}, "
                            "but not in ignore list"
                        )
                        gain_range_ok = False

                for h in ecc_pf_df.index[ecc_pf_df["Gain_Prime"] < ecc_run_cfg.Min_Gain_Prime]:
                    det_pix_can = [ecc_pf_df["DET"][h], ecc_pf_df["PIX"][h]]
                    if det_pix_can not in ecc_run_cfg.Ignore_Min_Gain_Prime_Det_Pix_List:
                        logger.warning(
                            f"ECC result Gain_Prime {ecc_pf_df['Gain_Prime'][h]} "
                            f"for DET {det_pix_can[0]} PIX {det_pix_can[1]} falls below "
                            f"Min_Gain_Prime {ecc_run_cfg.Min_Gain_Prime}, "
                            "but not in ignore list"
                        )
                        gain_range_ok = False

                for h in ecc_pf_df.index[ecc_pf_df["Gain_Cor"] < ecc_run_cfg.Min_Gain]:
                    det_pix_can = [ecc_pf_df["DET"][h], ecc_pf_df["PIX"][h]]
                    if det_pix_can not in ecc_run_cfg.Ignore_Min_Gain_Det_Pix_List:
                        logger.warning(
                            f"ECC result Gain_Cor {ecc_pf_df['Gain_Cor'][h]} "
                            f"for DET {det_pix_can[0]} PIX {det_pix_can[1]} falls below "
                            f"Min_Gain {ecc_run_cfg.Min_Gain}, "
                            "but not in ignore list"
                        )
                        gain_range_ok = False

                gain_range_ok_list.append(gain_range_ok)

                gain = off_gain[1, :, :]
                offset = off_gain[0, :, :]

                # calculate the actual energy edges taking the applied ELUT into
                # account for calibration of data recorded with the ELUT
                e_actual = (ob_elut.adc - offset[..., None]) * gain[..., None]

                e_actual_ext = np.pad(
                    e_actual,
                    # pad last axis by 1 on both sides
                    pad_width=((0, 0), (0, 0), (1, 1)),
                    mode="constant",
                    # first pad with 0, last pad with inf
                    constant_values=(0, np.inf),
                )
                e_actual_list.append(e_actual_ext)

                gain_ecc = off_gain_ecc[1, :, :]
                offset_ecc = off_gain_ecc[0, :, :]

                # calculate the actual energy edges taking the applied ELUT into
                # account for calibration of data recorded with the ELUT
                e_actual_ecc = (ob_elut.adc - offset_ecc[..., None]) * gain_ecc[..., None]

                e_actual_ext_ecc = np.pad(
                    e_actual_ecc,
                    # pad last axis by 1 on both sides
                    pad_width=((0, 0), (0, 0), (1, 1)),
                    mode="constant",
                    # first pad with 0, last pad with inf
                    constant_values=(0, np.inf),
                )
                ecc_only_e_actual_list.append(e_actual_ext_ecc)

            # end of ECC context block
        # end of for each spectrum
        l2.data.add_column(
            Column(
                name="e_edges_actual",
                data=e_actual_list,
                description="actual energy edges fitted by ECC and post fitting",
            )
        )  # noqa
        l2.data["e_edges_actual"].unit = u.keV

        l2.data.add_column(
            Column(
                name="offset_gain_goc",
                data=off_gain_list,
                description="result of the ecc fitting: offset, gain, goc and post fitting",
            )
        )  # noqa

        l2.data.add_column(
            Column(
                name="ecc_only_e_edges_actual",
                data=ecc_only_e_actual_list,
                description="actual energy edges fitted by ECC only",
            )
        )  # noqa
        l2.data["ecc_only_e_edges_actual"].unit = u.keV

        l2.data.add_column(
            Column(
                name="ecc_only_offset_gain_goc",
                data=ecc_only_off_gain_list,
                description="result of the ecc fitting only: offset, gain, goc",
            )
        )  # noqa

        l2.data.add_column(
            Column(
                name="ecc_error",
                data=ecc_err_list,
                description="error estimate from ECC: err_P31, err_dE31, err_P81, err_dE81",
            )
        )  # noqa
        l2.data.add_column(
            Column(name="gain_range_ok", data=gain_range_ok_list, description="is gain in expected range")
        )

        del l2.data["counts_comp_err"]
        del l2.data["counts"]

        return [l2]

import re
import subprocess
from pathlib import Path
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.io import fits
from astropy.io.fits import table_to_hdu
from astropy.table import Column, QTable

from stixcore.calibration.ecc_post_fit import ecc_post_fit_on_fits
from stixcore.calibration.elut_manager import ELUTManager
from stixcore.config.config import CONFIG
from stixcore.products.product import EnergyChannelsMixin, GenericProduct, L2Mixin
from stixcore.time import SCETimeRange
from stixcore.util.logging import get_logger

__all__ = [
    "EnergyCalibration",
]

logger = get_logger(__name__)


class EnergyCalibration(GenericProduct, EnergyChannelsMixin, L2Mixin):
    """Quick Look energy calibration data product.

    In level 2 format.
    """

    NAME = "energy"
    LEVEL = "CAL"
    TYPE = "cal"
    PRODUCT_PROCESSING_VERSION = 3

    def __init__(
        self,
        *,
        service_type=21,
        service_subtype=6,
        ssid=41,
        control,
        data,
        idb_versions=defaultdict(SCETimeRange),
        **kwargs,
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
        return False

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
        return "keV"

    @property
    def exposure(self):
        # default for FITS HEADER
        return self.control["integration_time"].min().to_value(u.s)

    @property
    def max_exposure(self):
        # default for FITS HEADER
        return self.control["integration_time"].max().to_value(u.s)

    def split_to_files(self):
        return [self]

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return (
            kwargs["level"] in [EnergyCalibration.LEVEL, "L2"]
            and service_type == 21
            and service_subtype == 6
            and ssid == 41
        )

    def get_additional_extensions(self):
        return []

    @classmethod
    def from_level1(cls, l1product, parent="", idlprocessor=None, ecc_manager=None):
        l2 = super().from_level1(l1product, parent=parent)[0]

        products = []

        date = l2.utc_timerange.start.datetime
        ob_elut, sci_channels = ELUTManager.instance.get_elut(date)
        ob_elut_end, sci_channels = ELUTManager.instance.get_elut(l2.utc_timerange.end.datetime)
        # ensure that the same ELUT is used for the whole time range
        if ob_elut.file != ob_elut_end.file:
            raise ValueError(
                f"ELUT change within energy calibration data time range: {ob_elut.file} to {ob_elut_end.file}"
            )

        for spec_idx, spec in enumerate(l2.data["counts"]):
            if spec.shape != (32, 12, 1024):
                raise ValueError(f"Unexpected shape {spec.shape} for counts in {l1product.name}")

            data = l2.data[:][[spec_idx]]
            control_index = l2.data["control_index"][spec_idx]
            control = l2.control[:][[control_index]]

            # as we separate each spectra in its own product we only have one entry in the control and therefore
            # the control_index should be always 0
            data["control_index"] = 0
            cal = EnergyCalibration(data=data, control=control)

            cal.control.add_column(
                Column(
                    name="ob_elut_name",
                    data=str(ob_elut.file).replace(".csv", ""),
                    description="Name of the ELUT active on instrument",
                )
            )

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

            with ecc_manager.context(date) as ecc_run_context:
                ecc_run_context_path, ecc_run_cfg = ecc_run_context

                cal.control.add_column(
                    Column(
                        name="ecc_config_name",
                        data=str(ecc_run_cfg.Name),
                        description="Name of the ECC configuration based on data date",
                    )
                )

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

                livetime = control["live_time"].to_value(u.s)

                cal.data.add_column(
                    Column(
                        name="live_time",
                        data=[livetime],
                        description="calibration spectra live time in seconds",
                    )
                )

                ecc_pf_df, idx_ecc = ecc_post_fit_on_fits(spec_all_erg, erg_path, livetime)
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
                cal.data.add_column(
                    Column(
                        name="offset_gain_goc",
                        data=[off_gain],
                        description="result of the ecc fitting: offset, gain, goc and post fitting",
                    )
                )

                # just keeping track of ECC + post fit results for now
                # off_gain_ecc = np.array(
                #     [
                #         4.0 * ecc_pf_df["Offset_ECC"].values.reshape(32, 12),
                #         1.0 / (4.0 * ecc_pf_df["Gain_ECC"].values.reshape(32, 12)),
                #         ecc_pf_df["goc"].values.reshape(32, 12),
                #     ]
                # )

                # cal.data.add_column(
                #     Column(
                #         name="ecc_only_offset_gain_goc",
                #         data=[off_gain_ecc],
                #         description="result of the ecc fitting only: offset, gain, goc",
                #     )
                # )

                cal.data.add_column(
                    Column(
                        name="ecc_error",
                        data=[
                            np.array(
                                [
                                    ecc_pf_df["err_P31"].values.reshape(32, 12),
                                    ecc_pf_df["err_dE31"].values.reshape(32, 12),
                                    ecc_pf_df["err_P81"].values.reshape(32, 12),
                                    ecc_pf_df["err_dE81"].values.reshape(32, 12),
                                ]
                            )
                        ],
                        description="error estimate from ECC: err_P31, err_dE31, err_P81, err_dE81",
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

                cal.data.add_column(
                    Column(name="gain_range_ok", data=gain_range_ok, description="is gain in expected range")
                )

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

                cal.data.add_column(
                    Column(
                        name="e_edges_actual",
                        data=[e_actual_ext],
                        description="actual energy edges fitted by ECC and post fitting",
                    )
                )  # noqa
                cal.data["e_edges_actual"].unit = u.keV

                # just keeping track of ECC + post fit results for now
                # gain_ecc = off_gain_ecc[1, :, :]
                # offset_ecc = off_gain_ecc[0, :, :]

                # # calculate the actual energy edges taking the applied ELUT into
                # # account for calibration of data recorded with the ELUT
                # e_actual_ecc = (ob_elut.adc - offset_ecc[..., None]) * gain_ecc[..., None]

                # e_actual_ext_ecc = np.pad(
                #     e_actual_ecc,
                #     # pad last axis by 1 on both sides
                #     pad_width=((0, 0), (0, 0), (1, 1)),
                #     mode="constant",
                #     # first pad with 0, last pad with inf
                #     constant_values=(0, np.inf),
                # )

                # cal.data.add_column(
                #     Column(
                #         name="ecc_only_e_edges_actual",
                #         data=[e_actual_ext_ecc],
                #         description="actual energy edges fitted by ECC only",
                #     )
                # )  # noqa
                # cal.data["ecc_only_e_edges_actual"].unit = u.keV

            # end of ECC context block
            del cal.data["counts_comp_err"]
            del cal.data["counts"]
            products.append(cal)
            # end of for each spectrum

        if len(products) > 1:
            pass
        return products

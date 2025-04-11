import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import numpy as np

import astropy.units as u
from astropy.table import QTable

from stixcore.ephemeris.manager import Spice
from stixcore.processing.SingleStep import SingleProcessingStepResult
from stixcore.processing.sswidl import SSWIDLTask
from stixcore.products import Product
from stixcore.products.product import GenericProduct, L2Mixin
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name, get_complete_file_name_and_path

__all__ = ["Ephemeris", "AspectIDLProcessing"]

logger = get_logger(__name__)


class AspectIDLProcessing(SSWIDLTask):
    """A IDL Task that will calculate the aspect solution based on HK product data input."""

    def __init__(self):
        script = """
            workdir = '{{ work_dir }}'
            print, workdir
            cd, workdir

            ; I/O directories:
            ; - location of some parameter files
            param_dir = workdir + d + 'SAS_param' + d
            calib_file = param_dir + 'SAS_calib_20211005.sav'
            aperfile = param_dir + 'apcoord_FM_circ.sav'
            simu_data_file = param_dir + 'SAS_simu.sav'

            hk_files = JSON_PARSE('{{ hk_files }}', /TOSTRUCT)

            data = []
            processed_files = []
            FOREACH hk_file, hk_files, file_index DO BEGIN
                catch, error
                if error ne 0 then begin
                    print, hk_file.parentfits, 'A IDL error occurred: ' + !error_state.msg
                    data_f.error = "FATAL_IDL_ERROR"
                    data = [data, data_f]
                    catch, /cancel
                    continue
                endif

                print, hk_file.parentfits
                print, ""
                flush, -1
                processed_files = [processed_files, hk_file.parentfits]
                data_f = []
                for i=0L, n_elements(hk_file.DATA.cha_diode0)-1 do begin
                    data_e = { stx_aspect_dto, $
                                cha_diode0: hk_file.DATA.cha_diode0[i], $
                                cha_diode1: hk_file.DATA.cha_diode1[i], $
                                chb_diode0: hk_file.DATA.chb_diode0[i], $
                                chb_diode1: hk_file.DATA.chb_diode1[i], $
                                time: hk_file.DATA.time[i], $
                                scet_time_c: hk_file.DATA.scet_time_c[i], $
                                scet_time_f: hk_file.DATA.scet_time_f[i], $
                                duration : hk_file.DATA.duration[i], $
                                spice_disc_size : hk_file.DATA.spice_disc_size[i], $
                                y_srf : hk_file.DATA.y_srf[i], $
                                z_srf : hk_file.DATA.z_srf[i], $
                                calib : hk_file.DATA.calib[i],  $
                                sas_ok : fix(hk_file.DATA.sas_ok[i]), $
                                error : hk_file.DATA.error[i], $
                                control_index : hk_file.DATA.control_index[i], $
                                parentfits : file_index $
                            }
                    data_f = [data_f, data_e]
                endfor

                ;fake error
                ;if file_index eq 2 then begin
                ;    zu = processed_files[1000]
                ;endif
                ; START ASPECT PROCESSING

                help, data_e, /str
                print, n_elements(data_f)
                flush, -1

                print,"Calibrating data..."
                flush, -1
                ; First, subtract dark currents and applies relative gains
                stx_calib_sas_data, data_f, calib_file

                ; copy result in a new object
                data_calib = data_f
                ; Added 2023-09-18: remove data points with some error detected during calibration
                stx_remove_bad_sas_data, data_calib

                ; Now automatically compute global calibration correction factor and applies it
                ; Note: this takes a bit of time
                stx_auto_scale_sas_data, data_calib, simu_data_file, aperfile

                cal_corr_factor = data_calib[0].calib
                data_f.CHA_DIODE0 *= cal_corr_factor
                data_f.CHA_DIODE1 *= cal_corr_factor
                data_f.CHB_DIODE0 *= cal_corr_factor
                data_f.CHB_DIODE1 *= cal_corr_factor

                print,"Computing aspect solution..."
                stx_derive_aspect_solution, data_f, simu_data_file, interpol_r=1, interpol_xy=1

                print,"END Computing aspect solution..."
                flush, -1
                ; END ASPECT PROCESSING

                data = [data, data_f]
            ENDFOREACH

            stx_gsw_version, version = idlgswversion

            undefine, hk_file, hk_files, data_e, i, di, data_f, d

"""
        super().__init__(script=script, work_dir="stix/idl/processing/aspect/", params={"hk_files": list()})

    def pack_params(self):
        """Preprocessing step applying json formatting to the input data.

        Returns
        -------
        dict
            the pre processed data
        """
        packed = self.params.copy()
        logger.info("calling IDL for hk files:")
        for f in packed["hk_files"]:
            logger.info(f"    {f['parentfits']}")
        packed["hk_files"] = json.dumps(packed["hk_files"])
        return packed

    def postprocessing(self, result, fits_processor):
        """Postprocessing step after the IDL tasks returned the aspect data.

        The aspect data and additional auxiliary data will be compiled
        into `Ephemeris` data product and written out to fits file.

        Parameters
        ----------
        result : dict
            the result of the IDL aspect solution
        fits_processor : FitsProcessor
            a fits processor to write out product as fits

        Returns
        -------
        list
            a list of all written fits files
        """
        files = []
        logger.info("returning from IDL")
        if "data" in result and "processed_files" in result:
            for file_idx, resfile in enumerate(result.processed_files):
                file_path = Path(resfile.decode())
                logger.info(f"IDL postprocessing HK file: {resfile}")
                if file_path.exists():
                    HK = Product(file_path)
                else:
                    HK = Product(get_complete_file_name_and_path(file_path))

                control = HK.control
                data = QTable()

                idldata = result.data[result.data["parentfits"] == file_idx]
                n = len(idldata)

                data["time"] = SCETime(coarse=idldata["scet_time_c"], fine=idldata["scet_time_f"])
                # data['timedel'] = SCETimeDelta.from_float(idldata["duration"] * u.s)
                # we have instantaneous data so the integration time is set to 0
                data["timedel"] = SCETimeDelta(coarse=0, fine=0)
                data["time_utc"] = [t.decode() for t in idldata["time"]]
                # [datetime.strptime(t.decode(), '%Y-%m-%dT%H:%M:%S.%f') for t in idldata['time']]
                data["control_index"] = idldata["control_index"]
                data["spice_disc_size"] = (idldata["spice_disc_size"] * u.arcsec).astype(np.float32)
                data["y_srf"] = (idldata["y_srf"] * u.arcsec).astype(np.float32)
                data["z_srf"] = (idldata["z_srf"] * u.arcsec).astype(np.float32)
                data["sas_ok"] = (idldata["sas_ok"]).astype(np.bool_)
                data["sas_ok"].description = "0: not usable, 1: good"
                data["sas_error"] = [e.decode() if hasattr(e, "decode") else e for e in idldata["error"]]

                data["solo_loc_carrington_lonlat"] = np.tile(np.array([0.0, 0.0]), (n, 1)).astype(np.float32) * u.deg
                data["solo_loc_carrington_dist"] = np.tile(np.array([0.0]), (n, 1)).astype(np.float32) * u.km
                data["solo_loc_heeq_zxy"] = np.tile(np.array([0.0, 0.0, 0.0]), (n, 1)).astype(np.float32) * u.km
                data["roll_angle_rpy"] = np.tile(np.array([0.0, 0.0, 0.0]), (n, 1)).astype(np.float32) * u.deg

                for idx, d in enumerate(data["time"]):
                    orient, dist, car, heeq = Spice.instance.get_auxiliary_positional_data(date=d)

                    data[idx]["solo_loc_carrington_lonlat"] = car.to("deg").astype(np.float32)
                    data[idx]["solo_loc_carrington_dist"] = dist.to("km").astype(np.float32)
                    data[idx]["solo_loc_heeq_zxy"] = heeq.to("km").astype(np.float32)
                    data[idx]["roll_angle_rpy"] = orient.to("deg").astype(np.float32)

                control["parent"] = get_complete_file_name(file_path.name)

                aux = Ephemeris(control=control, data=data, idb_versions=HK.idb_versions)

                aux.add_additional_header_keyword(
                    ("STX_GSW", result.idlgswversion[0].decode(),
                     "Version of STX-GSW that provided data"))
                aux.add_additional_header_keyword(
                    ("HISTORY", "aspect data processed by STX-GSW", ""))
                files.extend([SingleProcessingStepResult(aux.name, aux.level, aux.type,
                                                         aux.get_processing_version(), fop,
                                                         get_complete_file_name_and_path(file_path),
                                                         datetime.now())
                              for fop in fits_processor.write_fits(aux)])
        else:
            logger.error("IDL ERROR")

        return files


class Ephemeris(GenericProduct, L2Mixin):
    """Ephemeris data, including spacecraft attitude and coordinates as well as STIX
       pointing with respect to Sun center as derived from the STIX aspect system.

    In ANC product format.
    """

    PRODUCT_PROCESSING_VERSION = 2

    def __init__(
        self,
        *,
        service_type=0,
        service_subtype=0,
        ssid=1,
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
        self.name = "ephemeris"
        self.level = "ANC"
        self.type = "asp"
        self.ssid = 1
        self.service_subtype = 0
        self.service_type = 0

    @property
    def dmin(self):
        return np.nanmin([self.data["y_srf"].min().to_value("arcsec"), self.data["z_srf"].min().to_value("arcsec")])

    @property
    def dmax(self):
        return np.nanmax([self.data["y_srf"].max().to_value("arcsec"), self.data["z_srf"].max().to_value("arcsec")])

    @property
    def bunit(self):
        return "arcsec"

    @property
    def fits_daily_file(self):
        return True

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "ANC" and service_type == 0 and service_subtype == 0 and ssid == 1

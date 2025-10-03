from pathlib import Path
from datetime import datetime

import numpy as np
from stixpy.calibration.visibility import (
    calibrate_visibility,
    create_meta_pixels,
    create_visibility,
)
from stixpy.coordinates.transforms import get_hpc_info
from stixpy.net.client import STIXClient
from stixpy.product import Product as STIXPYProduct
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective
from sunpy.map import make_fitswcs_header
from sunpy.net import attrs as a
from sunpy.time import TimeRange
from xrayvision.clean import vis_clean

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Column, QTable
from astropy.time import Time

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice
from stixcore.products.level3.flarelistproduct import PeekPreviewImage
from stixcore.products.product import CountDataMixin, GenericProduct, L2Mixin, read_qtable
from stixcore.soop.manager import SOOPManager
from stixcore.time import SCETime, SCETimeRange
from stixcore.util.logging import get_logger
from stixcore.util.util import url_to_path

from stixpy.map.stix import STIXMap  # noqa

__all__ = [
    "FlarelistSDC",
    "FlarePositionMixin",
    "FlareSOOPMixin",
    "FlareList",
    "FlarelistSDCLocImg",
    "FlarePeekPreviewMixin",
    "FlarelistSC",
    "FlarelistSCLoc",
    "FlarelistSCLocImg",
]

logger = get_logger(__name__)


def make_stix_fitswcs_header(data, flare_position, *, scale, exposure, rotation_angle, energy_range):
    """Create a FITS WCS header for the given data."""
    header = make_fitswcs_header(
        data,
        flare_position,
        telescope="STIX",
        observatory="Solar Orbiter",
        scale=scale,
        exposure=exposure,
        rotation_angle=rotation_angle,
    )

    if energy_range is not None and header["wcsaxes"] == 2:
        # add energy range to WCS header
        # as a fake 3rd axis
        header["wcsaxes"] = 3
        header["CRVAL3"] = energy_range.mean().value
        header["CRPIX3"] = 1
        header["CDELT3"] = energy_range[1].value - energy_range[0].value
        header["CUNIT3"] = str(energy_range[0].unit)
        header["CTYPE3"] = "ENER"
        header["NAXIS"] = 3
        header["NAXIS3"] = 1

    return header


class FlarePositionMixin:
    """_summary_"""

    @classmethod
    def add_flare_position(
        cls,
        data,
        fido_client: STIXClient,
        *,
        filter_function=lambda x: True,
        peek_time_colname="peak_UTC",
        start_time_colname="start_UTC",
        end_time_colname="end_UTC",
        keep_all_flares=True,
        month=None,
    ):
        data["flare_position"] = [SkyCoord(0, 0, frame="icrs", unit="deg") for i in range(0, len(data))]

        data["anc_ephemeris_path"] = Column(" " * 500, dtype=str, description="TDB")
        data["cpd_path"] = Column(" " * 500, dtype=str, description="TDB")
        data["_position_status"] = Column(False, dtype=bool, description="TDB")
        data["_position_message"] = Column(" " * 500, dtype=str, description="TDB")
        to_remove = []
        pass_filter = 0
        no_ephemeris = 0
        no_cpd = 0
        many_cpd = 0
        one_cpd = 0
        total_flares = len(data)

        day_asp_ephemeris_cache = dict()

        for i, row in enumerate(data):
            if filter_function(row):
                pass_filter += 1
                peak_time = row[peek_time_colname]
                start_time = row[start_time_colname]
                end_time = row[end_time_colname]

                day = peak_time.to_datetime().date()

                if day in day_asp_ephemeris_cache:
                    anc_res = day_asp_ephemeris_cache[day]
                else:
                    anc_res = fido_client.search(
                        a.Time(peak_time, peak_time), a.Instrument.stix, a.stix.DataProduct.asp_ephemeris
                    )
                    if anc_res:
                        anc_res.filter_for_latest_version()
                    url_to_path(anc_res)
                    day_asp_ephemeris_cache[day] = anc_res

                if len(anc_res) < 1:
                    logger.warning(f"No ephemeris data found for flare at time {start_time} : {end_time}")
                    data[i]["_position_message"] = "no ephemeris data found"
                    no_ephemeris += 1
                    continue
                data[i]["anc_ephemeris_path"] = anc_res["path"][0]

                if start_time.datetime.hour < 2:
                    start_time = start_time - 2 * u.hour
                cpd_res = fido_client.search(
                    a.Time(start_time, end_time), a.Instrument.stix, a.stix.DataProduct.sci_xray_cpd
                )
                if cpd_res:
                    cpd_res.filter_for_latest_version()
                url_to_path(cpd_res)

                if len(cpd_res) < 1:
                    logger.warning(f"No CPD data found for flare at time {start_time} : {end_time}")
                    data[i]["_position_message"] = "no CPD data found"
                    no_cpd += 1
                    continue
                if len(cpd_res) > 1:
                    logger.debug(f"Many CPD data found for flare at time {start_time} : {end_time}")
                    # select the best available CPD data file
                    cpd_res["inc_peak"] = np.logical_and(
                        peak_time >= cpd_res["Start Time"], peak_time <= cpd_res["End Time"]
                    )
                    cpd_res["file_size"] = [path.stat().st_size for path in cpd_res["path"]]
                    cpd_res["exposure"] = 0.0
                    cpd_res["duration"] = 0.0
                    cpd_res["tbins"] = 0
                    cpd_res["ebins"] = 0
                    cpd_res["estart"] = 0.0
                    cpd_res["eend"] = 0.0
                    cpd_res["maxcount"] = 0
                    many_cpd += 1

                    for i, path in enumerate(cpd_res["path"]):
                        header = fits.getheader(path)
                        header_data = fits.getheader(path, "DATA")
                        energies = read_qtable(path, "ENERGIES")
                        cpd_res["maxcount"][i] = header["DATAMAX"]
                        cpd_res["exposure"][i] = header["XPOSURE"]
                        cpd_res["tbins"][i] = header_data["NAXIS2"]
                        cpd_res["ebins"][i] = len(energies)
                        cpd_res["estart"][i] = energies["e_low"][0].value
                        cpd_res["eend"][i] = (
                            energies["e_high"][-1].value if len(energies) < 31 else energies["e_high"][-2].value
                        )
                        cpd_res["duration"][i] = header["OBT_END"] - header["OBT_BEG"]

                    # TODO: add more criteria to select the best CPD file
                    cpd_res.sort(["tbins", "duration"])
                    # cpd_res.pprint()
                    best_cpd_idx = 0
                else:
                    one_cpd += 1
                    best_cpd_idx = 0
                data[i]["cpd_path"] = cpd_res["path"][best_cpd_idx]

                # do the calculations with stixpy

                data[i]["flare_position"] = SkyCoord(1, 1, frame="icrs", unit="deg")
                data[i]["_position_status"] = True
                data[i]["_position_message"] = "OK"

            else:
                to_remove.append(i)

        if not keep_all_flares:
            data.remove_rows(to_remove)

        logger.info(
            f"Flare position calculated for month {month} with {total_flares} flares, "
            f"passed filter: {pass_filter} no ephemeris data found for {no_ephemeris} "
            f"flares, no CPD data found for {no_cpd} flares, many CPD data found for "
            f"{many_cpd} flares, one CPD data found for {one_cpd} flares"
        )


class FlareSOOPMixin:
    """_summary_"""

    @classmethod
    def add_soop(
        self, data, *, peek_time_colname="peak_UTC", start_time_colname="start_UTC", end_time_colname="end_UTC"
    ):
        soop_encoded_type = list()
        soop_id = list()
        soop_type = list()

        for row in data:
            soops = SOOPManager.instance.find_soops(start=row[peek_time_colname])
            if soops:
                soop = soops[0]
                soop_encoded_type.append(soop.encodedSoopType)
                soop_id.append(soop.soopInstanceId)
                soop_type.append(soop.soopType)
            else:
                soop_encoded_type.append(None)
                soop_id.append(None)
                soop_type.append(None)

        data["soop_encoded_type"] = Column(soop_encoded_type, dtype=str, description="campaign ID")
        data["soop_id"] = Column(soop_id, dtype=str, description="SOOP ID")
        data["soop_type"] = Column(soop_type, dtype=str, description="name of the SOOP campaign")


class FlarePeekPreviewMixin:
    """Mixin class to add peek preview images to flare list products.
    This class provides a method to generate and add peek preview images
    to the flare list data. The images are generated based on the
    flare's peak time, start time, and end time, using the STIXPy library
    for visibility calculations and image reconstruction.
    The generated images are stored in the 'peek_preview_path' column of the data.
    The method also updates the status and message columns to indicate
    the success or failure of the image generation process.

    Currently the images are created for two energy ranges: 4-20 keV and 20-120 keV.
    """

    @classmethod
    def add_peek_preview(
        cls,
        data,
        energies,
        parent,
        fido_client: STIXClient,
        img_processor,
        *,
        peek_time_colname="peak_UTC",
        start_time_colname="start_UTC",
        end_time_colname="end_UTC",
        anc_ephemeris_path_colname="anc_ephemeris_path",
        cpd_path_colname="cpd_path",
        product_name_suffix="fl",
        keep_all_flares=True,
        month=None,
    ):
        data["peek_preview_path"] = Column(" " * 500, dtype=str, description="TDB")
        data["preview_start_UTC"] = [Time(d, format="isot", scale="utc") for d in data[peek_time_colname]]
        data["preview_end_UTC"] = [Time(d, format="isot", scale="utc") for d in data[peek_time_colname]]
        data["_peek_preview_status"] = Column(False, dtype=bool, description="TDB")
        data["_peek_preview_message"] = Column(" " * 500, dtype=str, description="TDB")
        to_remove = []
        products = []
        images = 0

        for i, row in enumerate(data):
            peak_time = row[peek_time_colname]
            row[start_time_colname]
            row[end_time_colname]

            anc_ephemeris_path = Path(row[anc_ephemeris_path_colname])
            cpd_path = Path(row[cpd_path_colname])

            status = False
            message = ""

            peek_preview_start = row[peek_time_colname]
            peek_preview_end = row[peek_time_colname]

            if anc_ephemeris_path.exists() and cpd_path.exists():
                try:
                    status = True
                    # do the imaging with stixpy

                    preview_data = data[i : i + 1]
                    del preview_data["peek_preview_path"]
                    del preview_data["_peek_preview_status"]
                    del preview_data["_peek_preview_message"]

                    peek_preview_start = row[peek_time_colname] - 10 * u.s
                    peek_preview_end = row[peek_time_colname] + 10 * u.s

                    preview_data["preview_start_UTC"] = peek_preview_start
                    preview_data["preview_end_UTC"] = peek_preview_end

                    cpd_sci = STIXPYProduct(cpd_path)
                    time_range_sci = [peek_preview_start, peek_preview_end]
                    maps = []
                    for energy_range in [[4, 20], [20, 120]] * u.keV:
                        # flare_position = preview_data['flare_position'][0]
                        # flare_position = [0, 0] * u.arcsec
                        comments = []
                        helio_frame = Helioprojective(observer="earth", obstime=peak_time)
                        flare_position = SkyCoord(0 * u.deg, 0 * u.deg, frame=helio_frame)

                        meta_pixels_sci = create_meta_pixels(
                            cpd_sci,
                            time_range=time_range_sci,
                            energy_range=energy_range,
                            flare_location=flare_position,
                            no_shadowing=True,
                        )
                        vis = create_visibility(meta_pixels_sci)
                        cal_vis = calibrate_visibility(vis, flare_location=flare_position)
                        isc_10_3 = [
                            3,
                            20,
                            22,
                            16,
                            14,
                            32,
                            21,
                            26,
                            4,
                            24,
                            8,
                            28,
                            15,
                            27,
                            31,
                            6,
                            30,
                            2,
                            25,
                            5,
                            23,
                            7,
                            29,
                            1,
                        ]
                        col_idx = np.argwhere(np.isin(cal_vis.meta["isc"], isc_10_3)).ravel()
                        cal_vis.meta["offset"] = flare_position
                        vis10_3 = cal_vis[col_idx]

                        imsize = [129, 129] * u.pixel  # number of pixels of the map to reconstruct
                        pixel = [2, 2] * u.arcsec / u.pixel  # pixel size in arcsec

                        vis_tr = TimeRange(vis.meta["time_range"])
                        roll, solo_xyz, pointing = get_hpc_info(vis_tr.start, vis_tr.end)
                        solo = HeliographicStonyhurst(*solo_xyz, obstime=vis_tr.center, representation_type="cartesian")

                        clean_map, model_map, resid_map = vis_clean(
                            vis10_3, imsize, pixel_size=pixel, gain=0.1, niter=200, clean_beam_width=20 * u.arcsec
                        )
                        comments.append(f"clean map with {len(col_idx)} visibilities")
                        comments.append(f"clean gain: {0.1}, niter: {200}, clean beam width: {20 * u.arcsec}")
                        comments.append(f"det: {', '.join(sorted(vis10_3.meta['vis_labels']))}")

                        map_with_erange = clean_map.data[np.newaxis, ...]
                        map_with_erange[0, :, :] = clean_map.data
                        fp_hp = flare_position.transform_to(Helioprojective(obstime=vis_tr.center, observer=solo))
                        header = make_stix_fitswcs_header(
                            map_with_erange,
                            fp_hp,
                            scale=pixel,
                            exposure=vis_tr.seconds,
                            rotation_angle=90 * u.deg + roll,
                            energy_range=energy_range,
                        )

                        header = fits.Header(header)
                        # Add comments
                        [header.add_comment(com) for com in comments]

                        header["IMG_METH"] = ("clean", "STIX image reconstruction method used")

                        maps.append((map_with_erange, header))

                    ppi = PeekPreviewImage(
                        control=QTable(),
                        data=preview_data,
                        month=month,
                        energy=energies,
                        maps=maps,
                        product_name_suffix=product_name_suffix,
                        parents=[parent, anc_ephemeris_path.name, cpd_path.name],
                    )

                    for f in img_processor.write_fits(ppi):
                        products.append(f)
                        images += len(ppi.maps)
                    message = "OK"
                except Exception as e:
                    logger.error(e, stack_info=True)
                    status = False
                    message = str(e)

            data[i]["preview_start_UTC"] = peek_preview_start
            data[i]["preview_end_UTC"] = peek_preview_end
            data[i]["peek_preview_path"] = "test"
            data[i]["_peek_preview_status"] = status
            data[i]["_peek_preview_message"] = message

        if not keep_all_flares:
            data.remove_rows(to_remove)

        logger.info(
            f"Flare images created for month {month} with {len(data)} flares, "
            f"{len(products)} peek previews created, with total {images} images"
        )

        return products


class FlareList(CountDataMixin, GenericProduct, L2Mixin):
    """FlareList product class.
    This class represents a flare list product in the STIX data processing pipeline.
    It inherits from GenericProduct and L2Mixin, and provides methods to handle flare data.
    It is used to store flare data, and can be enhanced
    with flare positions and SOOP information."""

    LEVEL = "L3"
    TYPE = "flarelist"

    def __init__(self, *, service_type=0, service_subtype=0, ssid, data, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=ssid, data=data, **kwargs)
        self.level = FlareList.LEVEL
        self.type = FlareList.TYPE
        self.service_subtype = 0
        self.service_type = 0
        self._parent = set()

    @property
    def parent(self):
        """Returns the parent(s) of the flare list product.

        Returns
        -------
        list
            A list of parent product names.
        """
        return list(self._parent)

    @parent.setter
    def parent(self, value):
        """Sets the parent of the flare list product.

        Parameters
        ----------
        value : str
            The name of the parent product to be added.
        """
        self._parent.add(value)

    def enhance_from_product(self, in_prod: GenericProduct):
        """Enhances the flare list product by adding more data.

        Parameters
        ----------
        in_prod : GenericProduct
            The input product from which to enhance the flare list product.
        """


class FlarelistSDC(FlareList, FlareSOOPMixin):
    """Flarelist product class for StixDataCenter flares.

    In L3 product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "sdc"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=2, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=2, data=data, **kwargs)

        self.name = FlarelistSDC.NAME
        self.ssid = 2

        self._start_datetime = datetime.combine(month, datetime.min.time())
        self._end_datetime = self.data["end_UTC"].max() if len(self.data) > 0 else self._start_datetime

    @property
    def utc_timerange(self):
        return TimeRange(self._start_datetime, self._end_datetime)

    @property
    def scet_timerange(self):
        tr = self.utc_timerange
        start = SCETime.from_string(Spice.instance.datetime_to_scet(tr.start)[2:])
        end = SCETime.from_string(Spice.instance.datetime_to_scet(tr.end)[2:])
        return SCETimeRange(start=start, end=end)

    def split_to_files(self):
        return [self]

    @property
    def dmin(self):
        return (self.data["lc_peak"].sum(axis=1)).min().value if len(self.data) > 0 else np.nan

    @property
    def dmax(self):
        return (self.data["lc_peak"].sum(axis=1)).max().value if len(self.data) > 0 else np.nan

    @property
    def exposure(self):
        return self.data["duration"].min().to_value("s") if len(self.data) > 0 else np.nan

    @property
    def max_exposure(self):
        return self.data["duration"].max().to_value("s") if len(self.data) > 0 else np.nan

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 2


class FlarelistSDCLoc(FlarelistSDC, FlarePositionMixin):
    """Flarelist product class for StixDataCenter flares.

    In ANC product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "sdcloc"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=3, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=3, data=data, month=month, **kwargs)

        self.name = FlarelistSDCLoc.NAME
        self.ssid = 3

    def enhance_from_product(self, in_prod: GenericProduct):
        pass

    @classmethod
    def filter_flare_function(cls, col):
        return col["lc_peak"][0].value > CONFIG.getint("Processing", "flarelist_sdc_min_count", fallback=1000)

    @classmethod
    def add_flare_position(cls, data, fido_client: STIXClient, *, month=None):
        super().add_flare_position(
            data,
            fido_client,
            filter_function=cls.filter_flare_function,
            peek_time_colname="peak_UTC",
            start_time_colname="start_UTC",
            end_time_colname="end_UTC",
            keep_all_flares=False,
            month=month,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 3


class FlarelistSDCLocImg(FlarelistSDCLoc, FlarePeekPreviewMixin):
    """Flarelist product class for StixDataCenter flares.

    In ANC product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "sdclocimg"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=4, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=4, data=data, month=month, **kwargs)

        self.name = FlarelistSDCLocImg.NAME
        self.ssid = 4

    def enhance_from_product(self, in_prod: GenericProduct):
        pass

    @classmethod
    def add_peek_preview(cls, data, energies, parent, fido_client: STIXClient, img_processor, *, month=None):
        super().add_peek_preview(
            data,
            energies,
            parent,
            fido_client,
            img_processor,
            peek_time_colname="peak_UTC",
            start_time_colname="start_UTC",
            end_time_colname="end_UTC",
            anc_ephemeris_path_colname="anc_ephemeris_path",
            cpd_path_colname="cpd_path",
            product_name_suffix=FlarelistSDC.NAME,
            keep_all_flares=False,
            month=month,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 4


class FlarelistSC(FlareList, FlareSOOPMixin):
    """Flarelist product class for STIXCore flares.

    In L3 product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "sc"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=6, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=6, data=data, **kwargs)

        self.name = FlarelistSC.NAME
        self.ssid = 6

        self._start_datetime = datetime.combine(month, datetime.min.time())
        self._end_datetime = self.data["end_UTC"].max() if len(self.data) > 0 else self._start_datetime

    @property
    def utc_timerange(self):
        return TimeRange(self._start_datetime, self._end_datetime)

    @property
    def scet_timerange(self):
        tr = self.utc_timerange
        start = SCETime.from_string(Spice.instance.datetime_to_scet(tr.start)[2:])
        end = SCETime.from_string(Spice.instance.datetime_to_scet(tr.end)[2:])
        return SCETimeRange(start=start, end=end)

    def split_to_files(self):
        return [self]

    @property
    def dmin(self):
        return (self.data["lc_peak"].sum(axis=1)).min().value if len(self.data) > 0 else np.nan

    @property
    def dmax(self):
        return (self.data["lc_peak"].sum(axis=1)).max().value if len(self.data) > 0 else np.nan

    @property
    def exposure(self):
        return self.data["duration"].min().to_value("s") if len(self.data) > 0 else np.nan

    @property
    def max_exposure(self):
        return self.data["duration"].max().to_value("s") if len(self.data) > 0 else np.nan

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 6


class FlarelistSCLoc(FlarelistSC, FlarePositionMixin):
    """Flarelist product class for STIXCore flares.

    In L3 product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "scloc"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=7, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=7, data=data, month=month, **kwargs)

        self.name = FlarelistSCLoc.NAME
        self.ssid = 7

    def enhance_from_product(self, in_prod: GenericProduct):
        pass

    @classmethod
    def filter_flare_function(cls, col):
        return col["lc_peak"][0].value > CONFIG.getint("Processing", "flarelist_sdc_min_count", fallback=1000)

    @classmethod
    def add_flare_position(cls, data, fido_client: STIXClient, *, month=None):
        super().add_flare_position(
            data,
            fido_client,
            filter_function=cls.filter_flare_function,
            peek_time_colname="peak_UTC",
            start_time_colname="start_UTC",
            end_time_colname="end_UTC",
            keep_all_flares=False,
            month=month,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 7


class FlarelistSCLocImg(FlarelistSCLoc, FlarePeekPreviewMixin):
    """Flarelist product class for StixCore flares.

    In ANC product format.
    """

    PRODUCT_PROCESSING_VERSION = 2
    NAME = "sclocimg"

    def __init__(self, *, service_type=0, service_subtype=0, ssid=8, data, month, **kwargs):
        super().__init__(service_type=0, service_subtype=0, ssid=8, data=data, month=month, **kwargs)

        self.name = FlarelistSCLocImg.NAME
        self.ssid = 8

    def enhance_from_product(self, in_prod: GenericProduct):
        pass

    @classmethod
    def add_peek_preview(cls, data, energies, parent, fido_client: STIXClient, img_processor, *, month=None):
        super().add_peek_preview(
            data,
            energies,
            parent,
            fido_client,
            img_processor,
            peek_time_colname="peak_UTC",
            start_time_colname="start_UTC",
            end_time_colname="end_UTC",
            anc_ephemeris_path_colname="anc_ephemeris_path",
            cpd_path_colname="cpd_path",
            product_name_suffix=FlarelistSC.NAME,
            keep_all_flares=False,
            month=month,
        )

    @classmethod
    def is_datasource_for(cls, *, service_type, service_subtype, ssid, **kwargs):
        return kwargs["level"] == "L3" and service_type == 0 and service_subtype == 0 and ssid == 8

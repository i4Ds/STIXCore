import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from stixdcpy.net import Request as stixdcpy_req
from sunpy.net import attrs as a

import astropy.units as u
from astropy.table import Column, QTable, vstack
from astropy.time import Time

from stixcore.config.config import CONFIG
from stixcore.products.level3.flarelist import FlarelistSC, FlarelistSDC
from stixcore.products.product import Product
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton
from stixcore.util.util import url_to_path

__all__ = ["FlareListManager", "SDCFlareListManager", "SCFlareListManager"]

logger = get_logger(__name__)


class FlareListManager:
    @property
    def flarelist(self):
        return self._flarelist

    @flarelist.setter
    def flarelist(self, value):
        self._flarelist = value

    @property
    def flarelistname(self):
        return type(self).__name__

    @property
    def productCls(self):
        return self._product_cls


class SCFlareListManager(FlareListManager, metaclass=Singleton):
    """Manages a local copy of the flarelist provided by STIXCore or runs the flare detection

    TODO
    """

    def __init__(self, file, fido_client, update=False):
        """Creates the manager by pointing to the flarelist file (csv) and setting the update
           strategy.

        Parameters
        ----------
        file : Path
            points to the csv file
        update : bool, optional
            Update strategy: is the flarelist file updated via API?, by default False
        """
        self.file = file
        self.update = update
        self._product_cls = FlarelistSC
        self.fido_client = fido_client
        self._flarelist = self.read_flarelist()

    def __str__(self) -> str:
        return f"{self.flarelistname}: file: {self.file} update: {self.update} size: {len(self.flarelist)}"

    def update_list(self):
        """Updates the flarelist file via api request.

        Will create a new file if not available or do a incremental update otherwise,
        using the last entry time stamp.
        """
        self.flarelist = SDCFlareListManager.read_flarelist(self.file, update=self.update)

    def read_flarelist(self):
        """Reads or creates the LUT of all STIXCore flares.

        On creation or update a flare detection is run on the STIXCore data
        to get the information and persists as a LUT locally.

        Parameters
        ----------
        file : Path
            path the to LUT file.
        update : bool, optional
            should the LUT be updated at start up?, by default False

        Returns
        -------
        Table
            the table of flares.
        """
        if self.update or not self.file.exists():
            last_date = datetime(2020, 1, 1, 0, 0, 0)
            last_date = datetime(2025, 4, 12, 0, 0, 0)
            # only run flare detection for time oldet than 7 days
            today = datetime.now() - timedelta(days=7)
            flare_df_lists = []
            if self.file.exists():
                old_list = pd.read_csv(self.file, keep_default_na=True, na_values=["None"])
                mds = old_list["start_UTC"].max()
                try:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S.%f")
                flare_df_lists = [old_list]
            last_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if not self.file.parent.exists():
                logger.info(f"path not found: {self.file.parent} creating dir")
                self.file.parent.mkdir(parents=True, exist_ok=True)

            try:
                while last_date < today:
                    # run flare detection for batches of 7 days
                    # 2 days overlap to ensure no flares are missed
                    start = last_date - timedelta(days=2)
                    end = last_date + timedelta(days=7)

                    ql_lc_files = self.fido_client.search(
                        a.Time(start, end), a.Instrument.stix, a.stix.DataProduct.ql_lightcurve
                    )
                    try:
                        if len(ql_lc_files) > 0:
                            ql_lc_files.filter_for_latest_version()
                            url_to_path(ql_lc_files)
                    except Exception as e:
                        logger.error(f"Error filtering for latest version of lightcurve files: {e}")
                        ql_lc_files = []

                    ql_bg_files = self.fido_client.search(
                        a.Time(start, end), a.Instrument.stix, a.stix.DataProduct.ql_background
                    )
                    if len(ql_bg_files) > 0:
                        ql_bg_files.filter_for_latest_version()
                        url_to_path(ql_bg_files)
                    logger.info(
                        f"flare detection chunk: {start.isoformat()}/{end.isoformat()} "
                        f"with {len(ql_lc_files)} lightcurve files and "
                        f"{len(ql_bg_files)} background files"
                    )
                    # flares = stixpy.detect_flares(start, end,
                    #                               ql_lc_files=ql_lc_files,
                    #                               ql_bg_files=ql_bg_files)

                    flares = []
                    if len(flares) > 0:
                        flare_df_lists.append(pd.DataFrame(flares))
                        logger.info(f"found {len(flares)} flares")
                    last_date += timedelta(days=7)

            except Exception:
                logger.error("FLARE DETECTION ERROR", exc_info=True)

            full_flare_list = flare_df_lists
            # full_flare_list = pd.concat(flare_df_lists)

            # full_flare_list.drop_duplicates(inplace=True)
            # full_flare_list.sort_values(by="peak_UTC", inplace=True)
            # full_flare_list.reset_index(inplace=True, drop=True)
            logger.info(f"write total {len(full_flare_list)} flares to local storage")
            # full_flare_list.to_csv(self.file, index_label=False)
        else:
            logger.info(f"read flare list from {self.file}")
            full_flare_list = pd.read_csv(self.file, keep_default_na=True, na_values=["None"])

        return full_flare_list

    @staticmethod
    def filter_flare_function(col):
        return col["lc_peak"][0].value > CONFIG.getint("Processing", "flarelist_sc_min_count", fallback=1000)

    def get_data(self, *, start, end, fido_client):
        month_data = self.flarelist[
            (self.flarelist["start_UTC"] >= start.isoformat()) & (self.flarelist["start_UTC"] < end.isoformat())
        ]

        if len(month_data) == 0:
            return None, None, None

        mt = QTable(month_data.to_numpy(), names=month_data.columns)
        data = QTable()
        control = QTable()
        energy = QTable()

        data["flare_id"] = Column(
            mt["flare_id"].astype(int), description=f"unique flare id for flarelist {self.flarelistname}"
        )
        data["start_UTC"] = Column(0, description="start time of flare")
        data["start_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["start_UTC"]]
        data["duration"] = Column(mt["duration"].astype(float) * u.s, description="duration of flare")
        data["end_UTC"] = Column(0, description="end time of flare")
        data["end_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["end_UTC"]]
        data["peak_UTC"] = Column(0, description="flare peak time")
        data["peak_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["peak_UTC"]]
        data["att_in"] = Column(mt["att_in"].astype(bool), description="was attenuator in during flare")
        data["bkg_baseline"] = Column(mt["LC0_BKG"] * u.ct, description="background baseline at 4-10 keV")
        data["GOES_class"] = Column(
            mt["GOES_class"].astype(str),
            description="GOES class of the GOES XRS data at time of flare"
            " - not derived from STIX data.  Do not use when "
            "flare isn't visible to Earth",
        )
        data["goes_min_class_est"] = Column(
            mt["goes_estimated_min_class"].astype(str), description="min GOES class estimate derived from STIX data"
        )
        data["goes_max_class_est"] = Column(
            mt["goes_estimated_max_class"].astype(str), description="max GOES class estimate derived from STIX data"
        )
        data["goes_mean_class_est"] = Column(
            mt["goes_estimated_mean_class"].astype(str), description="mean GOES class estimate derived from STIX data"
        )

        data["GOES_flux"] = Column(
            mt["GOES_flux"].astype(float) * u.W / u.m**2,
            description="GOES flux of the GOES XRS data at time of flare"
            "- not derived from STIX data. Do not use when the "
            "flare isn't visible to Earth",
        )
        data["goes_min_flux_est"] = Column(
            mt["goes_estimated_min_flux"].astype(float) * u.W / u.m**2,
            description="min GOES flux estimate derived from STIX data",
        )
        data["goes_max_flux_est"] = Column(
            mt["goes_estimated_max_flux"].astype(float) * u.W / u.m**2,
            description="max GOES flux estimate derived from STIX data",
        )
        data["goes_mean_flux_est"] = Column(
            mt["goes_estimated_mean_flux"].astype(float) * u.W / u.m**2,
            description="mean GOES flux estimate derived from STIX data",
        )

        # data['cfl_x'] = Column(mt['CFL_X_arcsec'].astype(float) * u.arcsec,
        #                        description="coarse flare location in x direction provided by"
        #                                    "onboard algorithm. (0,0) represents disk center")
        # data['cfl_y'] = Column(mt['CFL_Y_arcsec'].astype(float) * u.arcsec,
        #                        description="coarse flare location in y direction provided by"
        #                                    "onboard algorithm. (0,0) represents disk center")

        data["lc_peak"] = Column(
            (
                np.vstack(
                    (
                        mt["LC0_PEAK_COUNTS_4S"].value,
                        mt["LC1_PEAK_COUNTS_4S"].value,
                        mt["LC2_PEAK_COUNTS_4S"].value,
                        mt["LC3_PEAK_COUNTS_4S"].value,
                        mt["LC4_PEAK_COUNTS_4S"].value,
                    )
                ).T
                * u.ct
            ).astype(int),
            description="counts in 4s peak window from quicklook lightcurve",
            dtype=np.int64,
        )

        data["lc_bgk_peak"] = Column(
            (
                np.vstack(
                    (
                        mt["LC0_BKG_COUNTS_4S"].value,
                        mt["LC1_BKG_COUNTS_4S"].value,
                        mt["LC2_BKG_COUNTS_4S"].value,
                        mt["LC3_BKG_COUNTS_4S"].value,
                        mt["LC4_BKG_COUNTS_4S"].value,
                    )
                ).T
                * u.ct
            ).astype(int),
            description="background counts in 4s peak windowfrom quicklook lightcurve",
            dtype=np.int64,
        )

        data["energy_index"] = Column(0, description="energy band index", dtype=np.int8)

        data.add_index("flare_id")

        # add energy axis for the lightcurve peek time data for each flare
        # the energy bins are taken from the daily ql-lightcurve products
        # as the definition of the lc energy chanel's are will change only very seldom
        # the ql-lightcurve products assume a constant definition for an entire day.
        # So we do the lookup also just grouped by peak day in order to save file lookups

        energy_look_up = {}
        data["peak_day"] = [d.datetime.day for d in data["peak_UTC"]]
        data_by_day = data.group_by("peak_day")

        for day, flares in zip(data_by_day.groups.keys, data_by_day.groups):
            time = flares["peak_UTC"][0]
            lc_data = fido_client.search(a.Time(time, time), a.Instrument.stix, a.stix.DataProduct.ql_lightcurve)
            lc_data.filter_for_latest_version()
            url_to_path(lc_data)

            if len(lc_data) == 0:
                logger.warning(f"No lightcurve data found for flare at time {time}")
                continue
            lc = Product(lc_data["path"][0])

            energy_table_hash = frozenset(pd.core.util.hashing.hash_array(lc.energies.as_array()))

            # add the energy table to the energy table list if not already
            #  present and define a new index number
            if energy_table_hash not in energy_look_up:
                e_idx = len(energy_look_up.keys())
                energy_look_up[energy_table_hash] = e_idx
                lc.energies["index"] = Column(e_idx, description="energy edge table index", dtype=np.int8)
                energy = vstack([energy, lc.energies])
                if e_idx > 0:
                    logger.warning(f"multiple energy ql-lc tables found for month {start}")

            # add the energy index to the flare data to all flares of the same day
            # https://docs.astropy.org/en/latest/table/modify_table.html#caveats
            replace = data.loc[flares["flare_id"]]
            replace["energy_index"] = energy_look_up[energy_table_hash]
            data.loc[flares["flare_id"]] = replace

        del data["peak_day"]

        return data, control, energy


class SDCFlareListManager(FlareListManager, metaclass=Singleton):
    """Manages a local copy of the operational flarelist provided by stix data datacenter

    TODO
    """

    def __init__(self, file, update=False):
        """Creates the manager by pointing to the flarelist file (csv) and setting the update
           strategy.

        Parameters
        ----------
        file : Path
            points to the csv file
        update : bool, optional
            Update strategy: is the flarelist file updated via API?, by default False
        """
        self.file = file
        self.update = update
        self._flarelist = SDCFlareListManager.read_flarelist(self.file, self.update)
        self._product_cls = FlarelistSDC

    def __str__(self) -> str:
        return f"{self.flarelistname}: file: {self.file} update: {self.update} size: {len(self.flarelist)}"

    def update_list(self):
        """Updates the flarelist file via api request.

        Will create a new file if not available or do a incremental update otherwise,
        using the last entry time stamp.
        """
        self.flarelist = SDCFlareListManager.read_flarelist(self.file, update=self.update)

    @classmethod
    def read_flarelist(cls, file, update=False):
        """Reads or creates the LUT of all BSD RIDs and the request reason comment.

        On creation or update an api endpoint from the STIX data center is used
        to get the information and persists as a LUT locally.

        Parameters
        ----------
        file : Path
            path the to LUT file.
        update : bool, optional
            should the LUT be updated at start up?, by default False

        Returns
        -------
        Table
            the LUT od RIDs and request reasons.
        """
        if update or not file.exists():
            # the api is limited to batch sizes of a month. in order to get the full table we have
            # to ready each month after the start of STIX
            last_date = datetime(2020, 1, 1, 0, 0, 0)
            today = datetime.now()  # - timedelta(days=60)
            flare_df_lists = []
            if file.exists():
                old_list = pd.read_csv(file, keep_default_na=True, na_values=["None"])
                mds = old_list["start_UTC"].max()
                try:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S.%f")
                flare_df_lists = [old_list]
            last_date -= timedelta(days=60)
            if not file.parent.exists():
                logger.info(f"path not found to flare list file dir: {file.parent} creating dir")
                file.parent.mkdir(parents=True, exist_ok=True)

            try:
                while last_date < today:
                    last_date_1m = last_date + timedelta(days=30)
                    logger.info(f"download flare list chunk: {last_date.isoformat()}/{last_date_1m.isoformat()}")
                    flares = stixdcpy_req.fetch_flare_list(last_date.isoformat(), last_date_1m.isoformat())
                    last_date = last_date_1m
                    if len(flares) > 0:
                        flare_df_lists.append(pd.DataFrame(flares))
                        logger.info(f"found {len(flares)} flares")
                    # the stix datacenter API is throttled to 2 calls per second
                    time.sleep(0.5)
            except Exception:
                logger.error("FLARELIST API ERROR", exc_info=True)

            full_flare_list = pd.concat(flare_df_lists)

            full_flare_list.drop_duplicates(inplace=True)
            full_flare_list.sort_values(by="peak_UTC", inplace=True)
            full_flare_list.reset_index(inplace=True, drop=True)
            logger.info(f"write total {len(full_flare_list)} flares to local storage")
            full_flare_list.to_csv(file, index_label=False)
        else:
            logger.info(f"read flare list from {file}")
            full_flare_list = pd.read_csv(file, keep_default_na=True, na_values=["None"])

        return full_flare_list

    @staticmethod
    def filter_flare_function(col):
        return col["lc_peak"][0].value > CONFIG.getint("Processing", "flarelist_sdc_min_count", fallback=1000)

    def get_data(self, *, start, end, fido_client):
        month_data = self.flarelist[
            (self.flarelist["start_UTC"] >= start.isoformat()) & (self.flarelist["start_UTC"] < end.isoformat())
        ]

        if len(month_data) == 0:
            return None, None, None

        mt = QTable(month_data.to_numpy(), names=month_data.columns)
        data = QTable()
        control = QTable()
        energy = QTable()

        data["flare_id"] = Column(
            mt["flare_id"].astype(int), description=f"unique flare id for flarelist {self.flarelistname}"
        )
        data["start_UTC"] = Column(0, description="start time of flare")
        data["start_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["start_UTC"]]
        data["duration"] = Column(mt["duration"].astype(float) * u.s, description="duration of flare")
        data["end_UTC"] = Column(0, description="end time of flare")
        data["end_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["end_UTC"]]
        data["peak_UTC"] = Column(0, description="flare peak time")
        data["peak_UTC"] = [Time(d, format="isot", scale="utc") for d in mt["peak_UTC"]]
        data["att_in"] = Column(mt["att_in"].astype(bool), description="was attenuator in during flare")
        data["bkg_baseline"] = Column(mt["LC0_BKG"] * u.ct, description="background baseline at 4-10 keV")
        data["GOES_class"] = Column(
            mt["GOES_class"].astype(str),
            description="GOES class of the GOES XRS data at time of flare"
            " - not derived from STIX data.  Do not use when "
            "flare isn't visible to Earth",
        )
        data["goes_min_class_est"] = Column(
            mt["goes_estimated_min_class"].astype(str), description="min GOES class estimate derived from STIX data"
        )
        data["goes_max_class_est"] = Column(
            mt["goes_estimated_max_class"].astype(str), description="max GOES class estimate derived from STIX data"
        )
        data["goes_mean_class_est"] = Column(
            mt["goes_estimated_mean_class"].astype(str), description="mean GOES class estimate derived from STIX data"
        )

        data["GOES_flux"] = Column(
            mt["GOES_flux"].astype(float) * u.W / u.m**2,
            description="GOES flux of the GOES XRS data at time of flare"
            "- not derived from STIX data. Do not use when the "
            "flare isn't visible to Earth",
        )
        data["goes_min_flux_est"] = Column(
            mt["goes_estimated_min_flux"].astype(float) * u.W / u.m**2,
            description="min GOES flux estimate derived from STIX data",
        )
        data["goes_max_flux_est"] = Column(
            mt["goes_estimated_max_flux"].astype(float) * u.W / u.m**2,
            description="max GOES flux estimate derived from STIX data",
        )
        data["goes_mean_flux_est"] = Column(
            mt["goes_estimated_mean_flux"].astype(float) * u.W / u.m**2,
            description="mean GOES flux estimate derived from STIX data",
        )

        # data['cfl_x'] = Column(mt['CFL_X_arcsec'].astype(float) * u.arcsec,
        #                        description="coarse flare location in x direction provided by"
        #                                    "onboard algorithm. (0,0) represents disk center")
        # data['cfl_y'] = Column(mt['CFL_Y_arcsec'].astype(float) * u.arcsec,
        #                        description="coarse flare location in y direction provided by"
        #                                    "onboard algorithm. (0,0) represents disk center")

        data["lc_peak"] = Column(
            (
                np.vstack(
                    (
                        mt["LC0_PEAK_COUNTS_4S"].value,
                        mt["LC1_PEAK_COUNTS_4S"].value,
                        mt["LC2_PEAK_COUNTS_4S"].value,
                        mt["LC3_PEAK_COUNTS_4S"].value,
                        mt["LC4_PEAK_COUNTS_4S"].value,
                    )
                ).T
                * u.ct
            ).astype(int),
            description="counts in 4s peak window from quicklook lightcurve",
            dtype=np.int64,
        )

        data["lc_bgk_peak"] = Column(
            (
                np.vstack(
                    (
                        mt["LC0_BKG_COUNTS_4S"].value,
                        mt["LC1_BKG_COUNTS_4S"].value,
                        mt["LC2_BKG_COUNTS_4S"].value,
                        mt["LC3_BKG_COUNTS_4S"].value,
                        mt["LC4_BKG_COUNTS_4S"].value,
                    )
                ).T
                * u.ct
            ).astype(int),
            description="background counts in 4s peak windowfrom quicklook lightcurve",
            dtype=np.int64,
        )

        data["energy_index"] = Column(0, description="energy band index", dtype=np.int8)

        data.add_index("flare_id")

        # add energy axis for the lightcurve peek time data for each flare
        # the energy bins are taken from the daily ql-lightcurve products
        # as the definition of the lc energy chanel's are will change only very seldom
        # the ql-lightcurve products assume a constant definition for an entire day.
        # So we do the lookup also just grouped by peak day in order to save file lookups

        energy_look_up = {}
        data["peak_day"] = [d.datetime.day for d in data["peak_UTC"]]
        data_by_day = data.group_by("peak_day")

        for day, flares in zip(data_by_day.groups.keys, data_by_day.groups):
            time = flares["peak_UTC"][0]
            lc_data = fido_client.search(a.Time(time, time), a.Instrument.stix, a.stix.DataProduct.ql_lightcurve)
            lc_data.filter_for_latest_version()
            url_to_path(lc_data)

            if len(lc_data) == 0:
                logger.warning(f"No lightcurve data found for flare at time {time}")
                continue
            lc = Product(lc_data["path"][0])

            energy_table_hash = frozenset(pd.core.util.hashing.hash_array(lc.energies.as_array()))

            # add the energy table to the energy table list if not already
            #  present and define a new index number
            if energy_table_hash not in energy_look_up:
                e_idx = len(energy_look_up.keys())
                energy_look_up[energy_table_hash] = e_idx
                lc.energies["index"] = Column(e_idx, description="energy edge table index", dtype=np.int8)
                energy = vstack([energy, lc.energies])
                if e_idx > 0:
                    logger.warning(f"multiple energy ql-lc tables found for month {start}")

            # add the energy index to the flare data to all flares of the same day
            # https://docs.astropy.org/en/latest/table/modify_table.html#caveats
            replace = data.loc[flares["flare_id"]]
            replace["energy_index"] = energy_look_up[energy_table_hash]
            data.loc[flares["flare_id"]] = replace

        del data["peak_day"]

        return data, control, energy


if "pytest" in sys.modules:
    # only set the global in test scenario
    from stixcore.data.test import test_data

    SDCFlareListManager.instance = SDCFlareListManager(test_data.rid_lut.RID_LUT, update=False)

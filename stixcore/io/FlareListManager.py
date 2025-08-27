import sys
import time
from datetime import datetime, timedelta

import pandas as pd
from stixdcpy.net import Request as stixdcpy_req

from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ["FlareListManager", "SDCFlareListManager"]

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


class SDCFlareListManager(FlareListManager, metaclass=Singleton):
    """Manages a local copy of the operational flarelist provided by stix data datacenter

    TODO
    """

    def __init__(self, file, update=False):
        """Creates the manager by pointing to the flarelist file (csv) and setting the update strategy.

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
                old_list = pd.read_csv(file)
                mds = old_list["start_UTC"].max()
                try:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S.%f")
                flare_df_lists = [old_list]
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
            full_flare_list = pd.read_csv(file)

        return full_flare_list


if "pytest" in sys.modules:
    # only set the global in test scenario
    from stixcore.data.test import test_data

    SDCFlareListManager.instance = SDCFlareListManager(test_data.rid_lut.RID_LUT, update=False)

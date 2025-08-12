import sys
import time
import tempfile
import urllib.request
from datetime import date, datetime, timedelta

import numpy as np

from astropy.io import ascii
from astropy.table import Table
from astropy.table.operations import unique, vstack

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ["RidLutManager"]

logger = get_logger(__name__)


class RidLutManager(metaclass=Singleton):
    """Manages metadata for BSD requests

    The rid is used for a lookup in a csv table file where additional data
    connected to a BSD request is stored. Such as a description of the request
    purpose or state dependent configurations that are not part of the TM data.
    Most important the trigger scaling factor that was used if the trigger scaling
    schema is active.

    The data of th LUT is required over the the API endpoint:
    https://datacenter.stix.i4ds.net/api/bsd/info/
    """

    def __init__(self, file, update=False):
        """Creates the manager by pointing to the LUT files and setting the update strategy.

        Parameters
        ----------
        file : Path
            points to the LUT file
        update : bool, optional
            Update strategy: is the LUT file updated via API?, by default False
        """
        self.file = file
        self.update = update
        self.rid_lut = RidLutManager.read_rid_lut(self.file, self.update)

    def __str__(self) -> str:
        return f"file: {self.file} update: {self.update} size: {len(self.rid_lut)}"

    def update_lut(self):
        """Updates the LUT file via api request.

        Will create a new file if not available or do a incremental update otherwise,
        using the last entry time stamp.
        """
        self.rid_lut = RidLutManager.read_rid_lut(self.file, update=self.update)

    def get_reason(self, rid):
        """Gets the verbal description of the request purpose by combining several descriptive columns.

        Parameters
        ----------
        rid : int
            the BSD request id

        Returns
        -------
        str
            verbal description of the request purpose
        """
        try:
            request = self.rid_lut.loc[rid]
            reason = " ".join(np.atleast_1d(request["description"]))
            return reason
        except IndexError:
            logger.warning("can't get request purpose: no request founds for rid: {rid}")
            return ""

    def get_scaling_factor(self, rid):
        """Gets the trigger descaling factor connected to the BSD request.

        Parameters
        ----------
        rid : int
            the BSD request id

        Returns
        -------
        int
            the proposed trigger descaling factor to use for the BSD processing

        Raises
        ------
        ValueError
            if no or to many entries found for the given rid
        """
        try:
            request = self.rid_lut.loc[rid]
        except KeyError:
            raise ValueError("can't get scaling factor: no request founds for rid: {rid}")
        scaling_factor = np.atleast_1d(request["scaling_factor"])
        if len(scaling_factor) > 1:
            raise ValueError("can't get scaling factor: to many request founds for rid: {rid}")
        scf = scaling_factor[0].strip()
        return 30 if scf == "" else int(float(scf))

    @classmethod
    def read_rid_lut(cls, file, update=False):
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
        converters = {
            "_id": np.uint,
            "unique_id": np.uint,
            "start_utc": datetime,
            "duration": np.uint,
            "type": str,
            "subject": str,
            "purpose": str,
            "scaling_factor": str,
            "ior_id": str,
            "comment": str,
        }

        if update or not file.exists():
            rid_lut = Table(names=converters.keys(), dtype=converters.values())
            # the api is limited to batch sizes of a month. in order to get the full table we have
            # to ready each month after the start of STIX
            last_date = date(2019, 1, 1)
            today = date.today()
            if file.exists():
                rid_lut = ascii.read(file, delimiter=",", converters=converters, guess=False, quotechar='"')
                mds = rid_lut["start_utc"].max()
                try:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S").date()
                except ValueError:
                    last_date = datetime.strptime(mds, "%Y-%m-%dT%H:%M:%S.%f").date()

            if not file.parent.exists():
                logger.info(f"path not found to rid lut file dir: {file.parent} creating dir")
                file.parent.mkdir(parents=True, exist_ok=True)
            rid_lut_file_update_url = CONFIG.get("Publish", "rid_lut_file_update_url")

            try:
                while last_date < today:
                    last_date_1m = last_date + timedelta(days=30)
                    ldf = last_date.strftime("%Y%m%d")
                    ld1mf = last_date_1m.strftime("%Y%m%d")
                    update_url = f"{rid_lut_file_update_url}{ldf}/{ld1mf}"
                    logger.info(f"download publish lut file: {update_url}")
                    last_date = last_date_1m
                    updatefile = tempfile.NamedTemporaryFile().name
                    urllib.request.urlretrieve(update_url, updatefile)
                    update_lut = ascii.read(
                        updatefile, delimiter=",", converters=converters, guess=False, quotechar='"'
                    )

                    if len(update_lut) < 1:
                        continue
                    logger.info(f"found {len(update_lut)} entries")
                    rid_lut = vstack([rid_lut, update_lut])
                    # the stix datacenter API is throttled to 2 calls per second
                    time.sleep(0.5)
            except Exception:
                logger.warning("RID API ERROR", exc_info=True)

            rid_lut = unique(rid_lut, silent=True)
            ascii.write(rid_lut, file, overwrite=True, delimiter=",", quotechar='"')
            logger.info(f"write total {len(rid_lut)} entries to local storage")
        else:
            logger.info(f"read rid-lut from {file}")
            rid_lut = ascii.read(file, delimiter=",", converters=converters)

        rid_lut["description"] = [", ".join(r.values()) for r in rid_lut["subject", "purpose", "comment"].filled()]
        rid_lut.add_index("unique_id")

        return rid_lut


if "pytest" in sys.modules:
    # only set the global in test scenario
    from stixcore.data.test import test_data

    RidLutManager.instance = RidLutManager(test_data.rid_lut.RID_LUT, update=False)

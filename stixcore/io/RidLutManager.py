import re
import sys
import time
import tempfile
import urllib.request
from datetime import date, datetime, timedelta
from collections import namedtuple

import numpy as np

import astropy.units as u
from astropy.io import ascii
from astropy.table import Table
from astropy.table.operations import unique, vstack
from astropy.time import Time

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger
from stixcore.util.singleton import Singleton

__all__ = ["RidLutManager", "BackgroundCandidate", "search_background_candidates"]

logger = get_logger(__name__)

#: Keywords (case-insensitive) that mark a BSD request as a background/quiet
#: observation in the descriptive columns of the RID LUT.
DEFAULT_BKG_KEYWORDS = ("bkg", "quiet", "background", "non-flaring")

#: Negative keywords: a candidate whose descriptive text contains any of these is
#: rejected (e.g. "elevated" background is not a clean quiet baseline).
DEFAULT_BKG_EXCLUDE_KEYWORDS = ("elevated",)

#: Matches a specific flare id referenced in a comment, e.g. "for Flare 2309081508".
#: Such rows are flare data requests, not dedicated backgrounds.
_FLARE_REF_RE = re.compile(r"flare\s*\d{3,}")

#: A single background-request candidate from the RID LUT. ``start``/``end``/``mid``
#: are `~astropy.time.Time`, ``side`` is ``"past"``/``"future"`` relative to the query
#: time, and ``is_background`` is True when ``purpose == "Background"`` (a clean
#: background request, preferred over subject-only keyword matches).
BackgroundCandidate = namedtuple("BackgroundCandidate", ["rid", "start", "end", "mid", "side", "is_background"])


def _col_as_lower_str(tbl, name):
    """Return column ``name`` of ``tbl`` as a lower-cased ``str`` numpy array."""
    col = tbl[name]
    try:
        col = col.filled("")
    except (AttributeError, TypeError):
        pass
    return np.char.lower(np.asarray(col, dtype=str))


def search_background_candidates(
    rid_lut,
    time,
    *,
    window_past,
    window_future,
    keywords=DEFAULT_BKG_KEYWORDS,
    exclude_keywords=DEFAULT_BKG_EXCLUDE_KEYWORDS,
    exclude_flare_comment=True,
    purpose_penalty=1.0 * u.day,
):
    """Find background-request candidates in a RID LUT near ``time``.

    Rows are recognised as background requests by a case-insensitive keyword
    match (``keywords``) over the ``subject``/``purpose``/``comment`` columns,
    then filtered and ranked:

    * **exclude keywords** — a row whose text contains any of ``exclude_keywords``
      (default ``"elevated"``) is dropped.
    * **flare-id comment** — if ``exclude_flare_comment`` a row whose comment
      references a specific flare id (e.g. "for Flare 2309081508") is dropped.
    * **ranking** — **nearest-in-time first** by ``|time - start|`` (past preferred
      on a tie), but a request with ``purpose == "Background"`` is preferred over a
      subject-only keyword match unless the latter is more than ``purpose_penalty``
      closer. This is implemented as an *effective* distance
      ``|time - start| + purpose_penalty`` for non-Background rows.

    ``window_past`` / ``window_future`` remain separate bounds on how far a
    candidate's start may lie in each direction.

    Parameters
    ----------
    rid_lut : `~astropy.table.Table`
        The RID LUT (as produced by `RidLutManager.read_rid_lut`).
    time : `~astropy.time.Time` or str
        The query time (e.g. a flare peak).
    window_past, window_future : `~astropy.units.Quantity`
        How far back / forward from ``time`` a candidate's start may lie.
    keywords, exclude_keywords : tuple of str, optional
        Positive / negative case-insensitive keywords.
    exclude_flare_comment : bool, optional
        Drop rows whose comment references a specific flare id.
    purpose_penalty : `~astropy.units.Quantity`, optional
        Distance penalty added to non-``Background``-purpose candidates so a clean
        Background request wins unless a subject-only match is clearly closer.

    Returns
    -------
    list of BackgroundCandidate
        Ranked by ascending effective distance (past preferred on a tie). Empty
        if the LUT has no matching rows in range.
    """
    if rid_lut is None or len(rid_lut) == 0:
        return []
    t = time if isinstance(time, Time) else Time(time)

    subj = _col_as_lower_str(rid_lut, "subject")
    purp = _col_as_lower_str(rid_lut, "purpose")
    comm = _col_as_lower_str(rid_lut, "comment")
    haystack = np.char.add(np.char.add(subj, " "), np.char.add(purp, np.char.add(" ", comm)))

    mask = np.zeros(len(rid_lut), dtype=bool)
    for kw in keywords:
        mask |= np.char.find(haystack, kw.lower()) >= 0
    for kw in exclude_keywords:  # negative keywords drop the row
        mask &= np.char.find(haystack, kw.lower()) < 0
    if not np.any(mask):
        return []

    sub = rid_lut[mask]
    purp_sub = purp[mask]
    comm_sub = comm[mask]
    starts = Time(np.asarray(sub["start_utc"], dtype=str), format="isot", scale="utc")
    durations = np.asarray(sub["duration"], dtype=float) * u.s
    ends = starts + durations
    mids = starts + durations / 2
    rids = np.asarray(sub["unique_id"]).astype(np.int64)

    wp = window_past.to_value(u.day)
    wf = window_future.to_value(u.day)
    penalty = purpose_penalty.to_value(u.day)
    kept = []
    for i in range(len(sub)):
        if exclude_flare_comment and _FLARE_REF_RE.search(comm_sub[i]):
            continue
        d = (t - starts[i]).to_value(u.day)  # > 0 starts before ``time`` (past), < 0 after (future)
        if d >= 0:
            if d > wp:
                continue
            side = "past"
        else:
            if -d > wf:
                continue
            side = "future"
        is_bg = purp_sub[i].strip() == "background"
        effective = abs(d) + (0.0 if is_bg else penalty)  # prefer Background at equal-ish distance
        kept.append(
            (
                effective,
                0 if side == "past" else 1,
                BackgroundCandidate(int(rids[i]), starts[i], ends[i], mids[i], side, is_bg),
            )
        )

    kept.sort(key=lambda x: (x[0], x[1]))
    return [c for _, _, c in kept]


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

    def find_background_candidates(self, time, *, window_past, window_future, keywords=DEFAULT_BKG_KEYWORDS):
        """Find background-request candidates near ``time`` (see
        :func:`search_background_candidates`)."""
        return search_background_candidates(
            self.rid_lut, time, window_past=window_past, window_future=window_future, keywords=keywords
        )

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

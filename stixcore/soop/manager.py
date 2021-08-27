import os
import re
import json
import logging
from enum import Enum
from pathlib import Path
from collections.abc import Iterable

import dateutil.parser
from intervaltree import IntervalTree

from stixcore.util.logging import get_logger

__all__ = ['SOOPManager', 'SoopObservationType', 'KeywordSet',
           'HeaderKeyword', 'SoopObservation', 'SOOP']


logger = get_logger(__name__, level=logging.DEBUG)


class HeaderKeyword:
    """Keyword object for FITS file header entries."""

    def __init__(self, *, name, value="", comment=""):
        """Creates a HeaderKeyword object.

        Parameters
        ----------
        name : `str`
            the keyword name
        value : `str`, optional
            the value (gets converted to `str`), by default ""
        comment : `str`, optional
            the description (gets converted to `str`), by default ""
        """
        self.name = str(name).upper()
        self._comment = set(list(comment) if isinstance(comment, set) else [str(comment)])
        self._value = set(list(value) if isinstance(value, set) else [str(value)])

    @property
    def value(self):
        """Get the value for the keyword.

        Returns
        -------
        `str`
            a concatanated string list of all unique values
        """
        return ";".join(sorted(self._value))

    @property
    def comment(self):
        """Get the comment for the keyword.

        Returns
        -------
        `str`
            a concatanated string list of all unique comments
        """
        return ";".join(sorted(self._comment))

    def __eq__(self, other):
        if not isinstance(other, HeaderKeyword):
            return False
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __add__(self, other):
        """Combine two HeaderKeyword by concanating value and comment if the name is the same.

        Parameters
        ----------
        other : `HeaderKeyword`
            combine the current keyword with this one

        Returns
        -------
        `HeaderKeyword`
            a new keyword object with combined values

        Raises
        ------
        ValueError
            if the other object is not of same type and addresses the same name
        """
        if self == other:
            comment = self._comment.union(other._comment)
            value = self._value.union(other._value)

            return HeaderKeyword(name=self.name, value=value, comment=comment)
        else:
            raise ValueError("Keyword must address the same name")


class KeywordSet():
    """Helper object to collect and combine `HeaderKeyword`."""

    def __init__(self, s=None):
        """Create a KeywordSet collection.

        Parameters
        ----------
        s : `Iterable`, optional
            a list of `HeaderKeyword` objects, by default None
        """
        self.dic = dict()
        if s is not None:
            self.append(s)

    def add(self, element):
        """Add a new `HeaderKeyword` to the collection.

        If a same `HeaderKeyword` is allready present than the new keywords
        gets combined into the old.

        Parameters
        ----------
        element : `HeaderKeyword`
            the new keyword to add
        """
        if element in self.dic:
            self.dic[element] += element
        else:
            self.dic[element] = element

    def get(self, element):
        """Get the given keyword from the collection.

        Parameters
        ----------
        element : `HeaderKeyword`
            a prototype keyword that should be return with the same name.

        Returns
        -------
        `HeaderKeyword`
            The found keword with the same name.
        """
        return self.dic[element]

    def append(self, elements):
        """Add a a list of `HeaderKeyword` to the collection.

        If a same `HeaderKeyword` is allready present than the new keywords
        gets combined into the old.

        Parameters
        ----------
        element : Iterable<`HeaderKeyword`>
            the new keywords to add
        """
        if isinstance(elements, Iterable):
            for e in elements:
                self.add(e)
        else:
            self.add(e)

    def to_list(self):
        """Get all keywords.

        Returns
        -------
        `list`
            list of all keywords
        """
        return [e for e in self.dic.values()]


class SoopObservationType(Enum):
    """Enum of possible observation types defines in LTP.

    This is used for Filtering.
    """
    ALL = ""
    """ Use `ALL` to address all types."""

    STIX_BASIC = "STIX_BASIC"
    """Basic operation products like HK, QL ..."""

    STIX_ANALYSIS = "STIX_ANALYSIS"
    """Bulk science data products: detector, aspect and calibration data"""


class SOOP:
    """A SOOP entry from the LTP plans."""

    def __init__(self, jsonobj):
        """Create a new SOOP object based on the generic json data.

        see: https://issues.cosmos.esa.int/solarorbiterwiki/pages/viewpage.action?pageId=44991195

        Parameters
        ----------
        jsonobj : `Object`
            the allready parsed generic object.
        """
        self.encodedSoopType = jsonobj['encodedSoopType']
        self.soopInstanceId = jsonobj['soopInstanceId']
        self.soopType = jsonobj['soopType']
        self.startDate = dateutil.parser.parse(jsonobj['startDate'])
        self.endDate = dateutil.parser.parse(jsonobj['endDate'])

    def to_fits_keywords(self):
        """Generate the fits header keywords derived from this SOOP.

        Returns
        -------
        `list`
            list of `HeaderKeyword`
        """
        return list([HeaderKeyword(name="TARGET", value="TBC",
                                   comment="Type of target from planning"),
                    HeaderKeyword(name="SOOPTYPE", value=self.encodedSoopType,
                                  comment="Campaign ID(s) that the data belong to"),
                    HeaderKeyword(name="SOOPNAME", value=self.soopType,
                                  comment="Name of the SOOP Campaign that the data belong to")])


class SoopObservation:
    """A observation entry from the LTP plans."""

    def __init__(self, jsonobj):
        """Create a new SoopObservation object based on the generic json data.

        see: https://issues.cosmos.esa.int/solarorbiterwiki/pages/viewpage.action?pageId=44991195

        Parameters
        ----------
        jsonobj : `Object`
            the allready parsed generic object.
        """
        self.comment = jsonobj['comment']
        self.compositeId = jsonobj['compositeId']
        self.socIds = jsonobj['socIds']
        self.name = jsonobj['name']
        self.type = SoopObservationType[jsonobj['name']]
        self.experiment = jsonobj['experiment']
        self.socIds = jsonobj['socIds']
        self.startDate = dateutil.parser.parse(jsonobj['startDate'])
        self.endDate = dateutil.parser.parse(jsonobj['endDate'])

    def to_fits_keywords(self):
        """Generate the fits header keywords derived from this observation.

        Returns
        -------
        `list`
            list of `HeaderKeyword`
        """
        return list([HeaderKeyword(name="OBS_ID", value=";".join(self.socIds),
                                   comment="Unique ID of the individual observation")])


class SOOPManager():
    """Manages LTP files provided by GFTS"""

    SOOP_FILE_FILTER = "SSTX_observation_timeline_export_*.json"
    SOOP_FILE_REGEX = re.compile(r'.*SSTX_observation_timeline_export_.*.json$')

    def __init__(self, data_root):
        """Create the manager for a given data path root.

        All existing files will be index and the dir is observed.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`
            Path to the directory with all LTP files.
        """
        self.filecounter = 0
        self.data_root = data_root

    @property
    def data_root(self):
        """Get the data path root directory.

        Returns
        -------
        `pathlib.Path`
            path of the root directory
        """
        return self._data_root

    @data_root.setter
    def data_root(self, value):
        """Set the data path root.

        Parameters
        ----------
        data_root : `str` or `pathlib.Path`
            Path to the directory with all LTP files.
        """
        path = Path(value)
        if not path.exists():
            raise ValueError(f'path not found: {value}')

        self._data_root = path

        self.soops = IntervalTree()
        self.observations = IntervalTree()

        files = sorted(list(self._data_root.glob(SOOPManager.SOOP_FILE_FILTER)),
                       key=os.path.basename)

        if len(files) == 0:
            raise ValueError(f'No current SOOP files found at: {self._data_root}')

        for sfile in files:
            self.add_soop_file_to_index(sfile)

    def find_soops(self, *, start, end=None):
        """Search for all SOOPs in the index.

        Parameters
        ----------
        start : `datetime`
            start time to look for overlapping SOOPs in utc time
        end : `datetime`, optional
            end time to look for overlapping SOOPs in utc time, by default None ()

        Returns
        -------
        `list`
            list of found `SOOP` in all indexed LTP overlapping the given timeperiod/point
        """
        intervals = set()
        if end is None:
            intervals = self.soops.at(start)
        else:
            intervals = self.soops.overlap(start, end)

        return list([o.data for o in intervals])

    def find_observations(self, *, start, end=None, otype=SoopObservationType.ALL):
        """Search for all observations in the index.

        Parameters
        ----------
        start : `datetime`
            start time to look for overlapping observations in utc time
        end : `datetime`, optional
            end time to look for overlapping observations in utc time, by default None ()
        otype : `SoopObservationType`, optional
            filter for specific type, by default SoopObservationType.ALL

        Returns
        -------
        `list`
            list of found `SOOPObservation` in all indexed LTP overlapping the given
            timeperiod/point and matching the SoopObservationType.
        """
        intervals = set()
        if end is None:
            intervals = self.observations.at(start)
        else:
            intervals = self.observations.overlap(start, end)

        if len(intervals) > 0 and otype != SoopObservationType.ALL:
            intervals = set([o for o in intervals if o.data.type == otype])

        return list([o.data for o in intervals])

    def get_keywords(self, *, start, end=None, otype=SoopObservationType.ALL):
        """Searches for corresponding entries (SOOPs and Observations) in the index LTPs.

        Based on all found entries for the filter parameters a list of
        HeaderKeyword is generated combining all avaliable information.

       Parameters
        ----------
        start : `datetime`
            start time to look for overlapping observations and SOOPs in utc time
        end : `datetime`, optional
            end time to look for overlapping observations and SOOPs in utc time, by default None ()
        otype : `SoopObservationType`, optional
            filter for specific type of observations, by default SoopObservationType.ALL
        Returns
        -------
        `list`
            a list of `HeaderKeyword`

        Raises
        ------
        ValueError
            if no SOOPs or Observations where found in the index LTPs for the given filter settings.
        """
        kwset = KeywordSet()

        soops = self.find_soops(start=start, end=end)
        if len(soops) == 0:
            raise ValueError(f"No soops found for time: {start} - {end}")
        for soop in soops:
            kwset.append(soop.to_fits_keywords())

        obss = self.find_observations(start=start, end=end, otype=otype)
        if len(obss) == 0:
            raise ValueError(f"No observations found for time: {start} - {end} : {otype}")
        for obs in obss:
            kwset.append(obs.to_fits_keywords())

        return kwset.to_list()

    def add_soop_file_to_index(self, path):
        logger.info(f"Read SOOP file: {path}")
        with open(path) as f:
            ltp_data = json.load(f)
            for jsond in ltp_data["soops"]:
                soop = SOOP(jsond)
                self.soops.addi(soop.startDate, soop.endDate, soop)
            for jsond in ltp_data["observations"]:
                obs = SoopObservation(jsond)
                self.observations.addi(obs.startDate, obs.endDate, obs)

            self.filecounter += 1

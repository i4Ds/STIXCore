import numpy as np

import astropy.units as u

from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Time

SPICE_TIME = Time(meta_kernel_path=test_data.ephemeris.META_KERNEL_TIME)

__all__ = ['SCETime', 'SCETimeRange']


class SCETime:
    """
    SolarOrbiter Spacecraft Elapse Time (SCET) or Onboard Time (OBT).

    The mission clock time compose of a coarse time in seconds in 32bit field and fine time in 16bit
    field fractions of second 1s/(2**16 -1) can be represented as a single 48bit field or a float.
    The top most bit is used to indicate time sync issues.

    Attributes
    ----------
    time_sync : bool
        Time synchronisation status
    coarse : int
        Coarse time stamp (seconds)
    fine : int
        Fine time stamp fraction of seconds 1/2**31 -1
    """
    def __init__(self, coarse, fine):
        """
        Create a new datetime using the given coarse and fine values.

        Parameters
        ----------
        coarse : `int`
            Coarse time stamp (seconds)
        fine : `int`
            Fine time stamp fraction of seconds 1/2**16 -1
        """
        # Convention if top bit is set means times are not synchronised
        if coarse > (2**32)-1:
            ValueError('Course time cannot exceed 2**32-1')
        if fine > (2**16)-1:
            ValueError('Course time cannot exceed 2**16-1')
        self.time_sync = (coarse >> 31) != 1
        self.coarse = coarse if self.time_sync else coarse ^ 2**31
        self.fine = fine

    def to_datetime(self):
        """
        Return a python datetime object.

        Returns
        -------
        `datetime.datetime`
            The corresponding UTC datetime object.
        """
        with SPICE_TIME as time:
            utc = time.scet_to_datetime(f'{self.coarse}:{self.fine}')
            return utc

    @classmethod
    def min_time(cls):
        return SCETime(0, 0)

    @classmethod
    def max_time(cls):
        return SCETime((2**32)-1, (2**16)-1)

    @classmethod
    def from_float(cls, scet_float):
        sub_seconds, seconds = np.modf(scet_float.to_value('s'))
        fine = int((2**16 - 1) * sub_seconds)
        return SCETime(coarse=int(seconds), fine=fine)

    @classmethod
    def from_string(cls, scet_str):
        coarse, fine = [int(p) for p in scet_str.split('f')]
        return SCETime(coarse=coarse, fine=fine)

    def __add__(self, other):
        delta_coarse, new_fine = divmod(self.fine + other.fine, 2**16)
        new_coarse = self.coarse + other.coarse + delta_coarse
        return SCETime(coarse=new_coarse, fine=new_fine)

    def __sub__(self, other):
        delta_coarse, new_fine = divmod(self.fine - other.fine, 2 ** 16)
        new_coarse = self.coarse - other.coarse + delta_coarse
        return SCETime(coarse=new_coarse, fine=new_fine)

    def __truediv__(self, other):
        return SCETime.from_float(self.as_float() / other)

    # TODO check v spice
    def as_float(self):
        return self.coarse + (self.fine / (2**16 - 1)) << u.s

    def __repr__(self):
        return f'{self.__class__.__name__}(coarse={self.coarse}, fine={self.fine})'

    def __str__(self):
        return f'{self.coarse:010d}f{self.fine:05d}'

    def __gt__(self, other):
        if self.coarse > other.coarse:
            return True
        if self.coarse == other.coarse and self.fine > other.fine:
            return True
        return False

    def __ge__(self, other):
        if self.coarse > other.coarse:
            return True
        if self.coarse == other.coarse and self.fine >= other.fine:
            return True
        return False

    def __lt__(self, other):
        if self.coarse < other.coarse:
            return True
        if self.coarse == other.coarse and self.fine < other.fine:
            return True
        return False

    def __le__(self, other):
        if self.coarse < other.coarse:
            return True
        if self.coarse == other.coarse and self.fine <= other.fine:
            return True
        return False

    def __eq__(self, other):
        return self.coarse == other.coarse and self.fine == other.fine


class SCETimeRange:
    """
    SolarOrbiter Spacecraft Elapse Time (SCET) Range with start and end time.

    Attributes
    ----------
    start : `SCETime`
        start time of the range
    end : `SCETime`
        end time of the range
    """
    def __init__(self, *, start=SCETime.max_time(), end=SCETime.min_time()):
        self.start = start
        self.end = end

    def expand(self, time):
        """Enlarge the time range to include the given time.

        Parameters
        ----------
        time : `SCETime` or `SCETimeRange`
            The new time the range should include or an other time range.

        Raises
        ------
        ValueError
            if the given time is from a other class.
        """
        if isinstance(time, SCETime):
            self.start = min(self.start, time)
            self.end = max(self.end, time)
        elif isinstance(time, SCETimeRange):
            self.start = min(self.start, time.start)
            self.end = max(self.end, time.end)
        else:
            raise ValueError("time must be 'SCETime' or 'SCETimeRange'")

    def __repr__(self):
        return f'{self.__class__.__name__}(start={str(self.start)}, end={str(self.end)})'

    def __str__(self):
        return (f'{str(self.start)} to ' +
                f'{str(self.end)}')

    def __contains__(self, item):
        if isinstance(item, SCETime):
            return self.start <= item and self.end >= item
        elif isinstance(item, SCETimeRange):
            return self.start <= item.start and self.end >= item.end
        else:
            raise ValueError("time must be 'SCETime' or 'SCETimeRange'")

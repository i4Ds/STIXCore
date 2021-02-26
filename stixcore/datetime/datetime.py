import numpy as np

import astropy.units as u

from stixcore.data import test
from stixcore.ephemeris.manager import Time

mk_path = test.TEST_DATA_FILES['ephemeris']['test_time_20201001_V01.mk']

SPICE_TIME = Time(meta_kernel_path=mk_path)

__all__ = ['DateTime']


class DateTime:
    """
    SolarOrbiter / STIX Datetime

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
            Fine time stamp fraction of seconds 1/2**31 -1
        """
        # Convention if top bit is set means times are not synchronised
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
    def from_float(cls, scet_float):
        sub_seconds, seconds = np.modf(scet_float.to_value('s'))
        fine = int((2**16 - 1) * sub_seconds)
        return DateTime(coarse=int(seconds), fine=fine)

    @classmethod
    def from_string(cls, scet_str):
        coarse, fine = [int(p) for p in scet_str.split(':')]
        return DateTime(coarse=coarse, fine=fine)

    def __add__(self, other):
        delta_coarse, new_fine = divmod(self.fine + other.fine, 2**16)
        new_coarse = self.coarse + other.coarse + delta_coarse
        return DateTime(coarse=new_coarse, fine=new_fine)

    def __sub__(self, other):
        delta_coarse, new_fine = divmod(self.fine - other.fine, 2 ** 16)
        new_coarse = self.coarse - other.coarse + delta_coarse
        return DateTime(coarse=new_coarse, fine=new_fine)

    def __truediv__(self, other):
        return DateTime.from_float(self.as_float()/other)

    # TODO check v spice
    def as_float(self):
        return self.coarse + (self.fine / (2**16 - 1)) << u.s

    def __repr__(self):
        return f'{self.__class__.__name__}(coarse={self.coarse}, fine={self.fine})'

    def __str__(self):
        return f'{self.coarse:010d}:{self.fine:05d}'

    def __gt__(self, other):
        if(self.coarse > other.coarse):
            return True
        if(self.coarse == other.coarse and self.fine > other.fine):
            return True
        return False

    def __ge__(self, other):
        if(self.coarse > other.coarse):
            return True
        if(self.coarse == other.coarse and self.fine >= other.fine):
            return True
        return False

    def __lt__(self, other):
        if(self.coarse < other.coarse):
            return True
        if(self.coarse == other.coarse and self.fine < other.fine):
            return True
        return False

    def __le__(self, other):
        if(self.coarse < other.coarse):
            return True
        if(self.coarse == other.coarse and self.fine <= other.fine):
            return True
        return False

    def __eq__(self, other):
        return self.coarse == other.coarse and self.fine == other.fine

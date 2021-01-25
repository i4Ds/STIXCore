
from stixcore.data import test
from stixcore.ephemeris.manager import Time

mk_path = test.test_dir / 'mk' / 'test_time_20201001_V01.mk'

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
    def __init__(self, *, coarse, fine):
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

    # TODO check v spice
    def as_float(self):
        return self.coarse + (self.fine / (2**16 - 1))

    def __repr__(self):
        return f'{self.__class__.__name__}(coarse={self.coarse}, fine={self.fine})'

    def __str__(self):
        return f'{self.coarse:010d}:{self.fine:05d}'

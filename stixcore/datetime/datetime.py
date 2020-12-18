from stixcore.ephemeris.manager import Time

SPICE_TIME = Time(meta_kernel_path='')


class sDateTime:
    def __init__(self, *, coarse, fine):
        # Convention if top bit is set means time is not
        self.time_sync = (coarse >> 31) != 1
        self.coarse = coarse if self.time_sync else coarse ^ 2**31
        self.fine = fine

    def __repr__(self):
        return f'{self.__class__.__name__}(coarse={self.coarse}, fine={self.fine})'

    def __str__(self):
        return f'{self.coarse:010d}:{self.fine:05d}'


class DateTime:
    def __init__(self, *, coarse, fine):
        # Convention if top bit is set means time is not
        self.time_sync = (coarse >> 31) != 1
        self.coarse = coarse if self.time_sync else coarse ^ 2**31
        self.fine = fine

    def to_utc(self):
        with SPICE_TIME as time:
            utc = time.scet_to_datetime(f'{self.coarse}:{self.fine}')
        return utc

    @staticmethod
    def from_scet(coarse, fine):
        return DateTime(scet_coarse=coarse, scet_fine=fine)

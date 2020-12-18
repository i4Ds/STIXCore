from datetime import datetime as pydatetime

__all__ = ['DateTime']

# TODO to fully implement with tests and documentation


class DateTime:
    def __init__(self, **kwargs):
        self.scet_coarse = None
        self.scet_fine = None

        if ('scet_coarse', 'scet_fine') in kwargs:
            if isinstance(kwargs['scet_coarse'], int):
                self.scet_coarse = kwargs['scet_coarse']
            else:
                raise ValueError(f"scet_coarse must be int")

            if isinstance(kwargs['scet_fine'], int):
                self.scet_fine = kwargs['scet_fine']
            else:
                raise ValueError(f"scet_fine must be int")

    def as_utc(self):
        # TODO replace that with real time convertion
        return pydatetime.now()

    @staticmethod
    def from_scet(coarse, fine):
        return DateTime(scet_coarse=coarse, scet_fine=fine)

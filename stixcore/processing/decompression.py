"""Processing module for applying the skm decompression for configured parameters."""
from stixcore.calibration.compression import decompress as algo_decompress

__all__ = ['CompressedParameter', 'decompress']


class CompressedParameter():
    """A class to combine the raw and decompressed values and settings of a parameter."""

    _raw = None
    _decompressed = None
    _skm = None
    _error = None

    def __init__(self, *, raw, decompressed, error, skm):
        """Create a CompressedParameter object.

        Parameters
        ----------
        raw : `int`|`list`
            The raw values.
        decompressed : `int`|`list`
            The decompressed values.
        error : `int`|`list`
            The estimated error of the decompression.
        skm : `tuple`
            (s, k, m) settings for the decompression algorithm.
        """
        self.raw = raw
        self.decompressed = decompressed
        self.skm = skm
        self.error = error

    def __repr__(self):
        return f'CompressedParameter(raw={self.raw}, decompressed={self.decompressed}, \
        error={self.error}, skm={self.skm})'

    def __str__(self):
        return f'CompressedParameter(raw: len({len(self.raw)}), decompressed: \
        len({len(self.decompressed)}), error: len({len(self.error)}), skm={self.skm})'

    @property
    def raw(self):
        """Raw values before decompression.

        Returns
        -------
        `int`|`list`
        """
        return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    @property
    def decompressed(self):
        """Decompression values.

        Returns
        -------
        `int`|`list`
        """
        return self._decompressed

    @decompressed.setter
    def decompressed(self, value):
        self._decompressed = value

    @property
    def skm(self):
        """Decompression settings.

        Returns
        -------
        `tuple`
            (s, k, m) settings for the decompression algorithm.
        """
        return self._skm

    @skm.setter
    def skm(self, value):
        self._skm = value

    @property
    def error(self):
        """Estimated error of the decompression.

        Returns
        -------
        `int`|`list`
        """
        return self._error

    @error.setter
    def error(self, value):
        self._error = value


def apply_decompress(raw, skm):
    """Wrap the docempression algorithm into a callback.

    Parameters
    ----------
    raw : `int`|`list`
        will be the old parameter value (input)
    skm : `list`
        list of compression settings [s, k, m]

    Returns
    -------
    [type]
        [description]
    """
    decompressed, error = algo_decompress(raw, s=skm[0], k=skm[1], m=skm[2], return_variance=True)
    return CompressedParameter(raw=raw, decompressed=decompressed, error=error, skm=skm)


def decompress(packet):
    """Apply parameter decompression for the entire packet.

    Gets all parameters to decompress from configuration.

    Parameters
    ----------
    packet : `GenericTMPacket`
        The TM packet

    Returns
    -------
    `int`
        How many times the decompression algorithm was called.
    """
    decompression_parameter = packet.get_decompression_parameter()
    if not decompression_parameter:
        return 0
    c = 0
    for param_name, (sn, kn, mn) in decompression_parameter.items():
        skm = (sn if isinstance(sn, int) else packet.data.get(sn),  # option to configure exceptions
               kn if isinstance(kn, int) else packet.data.get(kn),
               mn if isinstance(mn, int) else packet.data.get(mn))
        c += packet.data.apply(param_name, apply_decompress, skm)
    return c

"""Processing module for applying the skm decompression for configured parameters."""
import numpy as np

from stixcore.calibration.compression import decompress as algo_decompress

__all__ = ['CompressedParameter', 'decompress']


class CompressedParameter:
    """A class to combine the raw and decompressed values and settings of a parameter.

    Attributes
    ----------
    raw : `int`|`list`
        The raw values before the decompression.
    decompressed : `int`|`list`
        The decompressed values.
    error : `int`|`list`
        The estimated error of the decompression.
    skm : `numpy.array`
        [s, k, m] settings for the decompression algorithm.
    skm_meta : `array`
        [s_nix, k_nix, m_nix] NIX names for the decompression settings.
    """

    def __init__(self, *, raw, decompressed, error, skm, meta):
        """Create a CompressedParameter object.

        Parameters
        ----------
        raw : `int`|`list`
            The raw values.
        decompressed : `int`|`list`
            The decompressed values.
        error : `int`|`list`
            The estimated error of the decompression.
        skm : `numpy.array`
            [s, k, m] settings for the decompression algorithm.
        skm_meta : `array`
            [s_nix, k_nix, m_nix] NIX names for the decompression settings.
        """
        self.raw = raw
        self.decompressed = decompressed
        self.skm = skm
        self.error = error
        self.meta = meta

    def __repr__(self):
        return f'{self.__class__.__name__}(raw={self.raw}, decompressed={self.decompressed}, \
        error={self.error}, skm={self.skm})'

    def __str__(self):
        return f'{self.__class__.__name__}(raw: len({len(self.raw)}), decompressed: \
        len({len(self.decompressed)}), error: len({len(self.error)}), skm={self.skm})'


def apply_decompress(raw, skmp):
    """Wrap the docempression algorithm into a callback.

    Parameters
    ----------
    raw : `int`|`list`
        will be the old parameter value (input)
    skm : `numpy.array`
        array of compression settings [s, k, m]

    Returns
    -------
    [type]
        [description]
    """
    skm, meta = skmp
    decompressed, error = algo_decompress(raw, s=skm[0], k=skm[1], m=skm[2], return_variance=True)
    return CompressedParameter(raw=raw, decompressed=decompressed, error=error, skm=skm, meta=meta)


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
    for param_name, [sn, kn, mn] in decompression_parameter.items():
        # option to configure exceptions
        skm = np.array([sn if isinstance(sn, int) else packet.data.get(sn),
                        kn if isinstance(kn, int) else packet.data.get(kn),
                        mn if isinstance(mn, int) else packet.data.get(mn)], np.ubyte).T
        c += packet.data.apply(param_name, apply_decompress, (skm, [sn, kn, mn]))
    return c

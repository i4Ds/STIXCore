"""Processing module for applying the skm decompression for configured parameters."""
from stixcore.calibration.compression import decompress as algo_decompress
from stixcore.tmtc.parser import Parameter

__all__ = ['CompressedParameter', 'decompress']


class CompressedParameter(Parameter):
    """A class to combine the raw and decompressed values and settings of a parameter.

    Attributes
    ----------
    decompressed : `int`|`list`
        The decompressed values.
    error : `int`|`list`
        The estimated error of the decompression.
    skm : `tuple`
        (s, k, m) settings for the decompression algorithm.
    """

    def __init__(self, *, name, value, idb_info, decompressed, error, skm):
        """Create a CompressedParameter object.

        Parameters
        ----------
        value : `int`|`list`
            The compressed values.
        decompressed : `int`|`list`
            The decompressed values.
        error : `int`|`list`
            The estimated error of the decompression.
        skm : `numpy.array`
            [s, k, m] settings for the decompression algorithm.
        """
        super(CompressedParameter, self).__init__(name=name, value=value, idb_info=idb_info)
        self.decompressed = decompressed
        self.skm = skm
        self.error = error

    def __repr__(self):
        return f'{self.__class__.__name__}(raw={self.value}, decompressed={self.decompressed}, \
        error={self.error}, skm={self.skm})'

    def __str__(self):
        return f'{self.__class__.__name__}(raw: len({len(self.value)}), decompressed: \
        len({len(self.decompressed)}), error: len({len(self.error)}), skm={self.skm})'


def apply_decompress(raw, skm):
    """Wrap the decompression algorithm into a callback.

    Parameters
    ----------
    raw : `stixcore.tmtc.parser.Parameter`
        will be the old parameter value (input)
    skmp : `list[stixcore.tmtc.parser.Parameter]`
        list of compression settings [s, k, m]

    Returns
    -------
    CompressedParameter
        A uncompressed version of the parameter
    """
    decompressed, error = algo_decompress(raw.value, s=skm[0].value, k=skm[1].value, m=skm[2].value,
                                          return_variance=True)
    return CompressedParameter(name=raw.name, idb_info=raw.idb_info, value=raw.value,
                               decompressed=decompressed, error=error, skm=skm)


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
        c += packet.data.apply(param_name, apply_decompress,  skm)
    return c

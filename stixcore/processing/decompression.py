"""Processing module for applying the skm decompression for configured parameters."""
from stixcore.calibration.compression import decompress as algo_decompress
from stixcore.tmtc.parameter import CompressedParameter

__all__ = ['decompress']


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
    try:
        decompressed, error = algo_decompress(raw.value, s=skm[0].value, k=skm[1].value,
                                              m=skm[2].value, return_variance=True)
    except AttributeError:
        # If the compression scheme parameters are overridden they will be int no parameters
        decompressed, error = algo_decompress(raw.value, s=skm[0], k=skm[1], m=skm[2],
                                              return_variance=True)
    return CompressedParameter(name=raw.name, idb_info=raw.idb_info, value=raw.value,
                               decompressed=decompressed, error=error, skm=skm, order=raw.order)


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
        skm = (sn if isinstance(sn, int) else packet.data.get(sn),
               kn if isinstance(kn, int) else packet.data.get(kn),
               mn if isinstance(mn, int) else packet.data.get(mn))

        c += packet.data.apply(param_name, apply_decompress,  skm)
    return c

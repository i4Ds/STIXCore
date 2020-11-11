from collections import defaultdict

from bitstring import ConstBitStream


def parse_binary(binary, structure):
    """
    Parse binary data using given structure.

    Parameters
    ----------
    binary : bytes or hexstring
        Input binary data
    structure : dict
        Name and data type mapping e.g. `{'myparam': uint:8}`

    Returns
    -------
    dict
        The parsed data fields and bitstream
    """
    bitstream = ConstBitStream(binary)
    return parse_bitstream(bitstream, structure)


def parse_bitstream(bitstream, structure):
    """
    Parse data from bitstream structure.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`

    structure : dict
        Name and data type mapping e.g. `{'myparam': uint:8}`

    Returns
    -------
    dict
        The parsed data fields and bitstream
    """
    # TODO check if faster to use ', '.join(format.values())
    parsed = {name: bitstream.read(format) for name, format in structure.items()}
    return {'fields': parsed, 'bitstream': bitstream}


def parse_repeated(bitstream, structure, num_repeats):
    """
    Parse repeated structures from a bitstream.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`
        Bitstream to parse structures from
    structure : dict
        Name and data type mapping e.g. `{'myparam': uint:8}`
    num_repeats : int
        Number of repeats

    Returns
    -------
    dict
        The repeated fields are returned as a dictionary the field name as keys and values of the
        list of extracted data
    """
    out = defaultdict(list)
    [[out[name].append(bitstream.readlist(format)) for name, format in structure.items()]
     for i in range(num_repeats)]
    return {'fields': out, 'bitstream': bitstream}

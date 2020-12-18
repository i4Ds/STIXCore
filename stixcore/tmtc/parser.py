from collections import defaultdict

from bitstring import ConstBitStream

from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class PacketData:
    """Generic class for organizing all parameters of the TM packet data."""

    @classmethod
    def parameter_dict_2_PacketData(cls, d):
        """Convert the nested dictionary of the parameter parsing into a PacketData object.

        Parameters
        ----------
        d : `dict`
            The parse result of the TM data block.

        Returns
        -------
        `PacketData`
            Generic data structure.
        """
        # instance of class list
        if isinstance(d, list):
            d = [PacketData.parameter_dict_2_PacketData(x) for x in d]

        # if d is not a instance of dict then
        # directly object is returned
        if not isinstance(d, dict):
            return d

        # constructor of the class passed to obj
        obj = PacketData()

        for k in d:
            obj.__dict__[k] = PacketData.parameter_dict_2_PacketData(d[k])

        return obj


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


def _parse_tree(bitstream, parent, fields):
    """Recursive parsing of TM data.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`
        Input binary data
    parent : `~stixcore/idb/idb/IdbPacketTree`
        the dynamic parse tree defined by the IDB
    fields : `dict`
        The parsed parameters - mutable out data.
    """
    if not parent:
        return
    counter = parent.counter

    for i in range(0, counter):
        for pnode in parent.children:
            # TODO test if next info is avaialable (length)
            if (pnode.parameter.is_variable() and (pnode.parameter.VPD_OFFSET < 0)):
                bitstream.pos += int(pnode.parameter.VPD_OFFSET)
            try:
                raw_val, gr_val = (bitstream.read(pnode.parameter.bin_format), dict())
            except Exception as e:
                print(e)
                raise e
            if pnode.children:
                num_children = raw_val
                is_valid = False
                if isinstance(num_children, int):
                    if num_children > 0:
                        pnode.counter = num_children
                        is_valid = True
                        _parse_tree(bitstream, pnode, gr_val)
                if not is_valid:
                    if pnode.name != 'NIXD0159':
                        # repeater NIXD0159 can be zero according to STIX ICD-0812-ESC Table 93 P123
                        logger.warning(f'Repeater {pnode.name}  has an invalid value: {raw_val}')

            if pnode.name in fields:
                oldEntry = fields[pnode.name]
                if not isinstance(oldEntry, list):
                    oldEntry = [oldEntry]
                oldEntry.append(gr_val if len(gr_val) > 0 else raw_val)
                fields[pnode.name] = oldEntry
            else:
                fields[pnode.name] = gr_val if len(gr_val) > 0 else raw_val


def parse_variable(bitstream, tree):
    """Parse binary data using given structure.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`
        Input binary data
    tree : `~stixcore/idb/idb/IdbPacketTree`
        the dynamic parse tree defined by the IDB
    Returns
    -------
    `PacketData`
        The parsed (nested) telemetry packet parameters.
    """
    fields = dict()
    _parse_tree(bitstream, tree, fields)
    return PacketData.parameter_dict_2_PacketData(fields)


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

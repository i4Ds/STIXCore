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
            # if k=='NIX00260': print('NIX00260')
            obj.__dict__[k] = PacketData.parameter_dict_2_PacketData(d[k])

        return obj

    def set(self, path, value):
        obj = self
        for p in path[0:-1]:
            if isinstance(p, str):
                obj = getattr(obj, p)
            else:
                obj = obj[0]

        last = path[-1]
        if isinstance(last, str):
            setattr(obj, last, value)
        else:
            obj[last] = value

    def get_first(self, nix, path=None):
        if path is None:
            path = []
        v = self._get_first(nix, path)
        return v

    def _get_first(self, nix, path):
        if hasattr(self, nix):
            path.append(nix)
            return getattr(self, nix)

        for attr, value in self.__dict__.items():
            if isinstance(value, PacketData):
                v = value._get_first(nix, path)
                if v is not None:
                    path.insert(0, attr)
                    return v
            elif isinstance(value, list):
                for index, list_elem in enumerate(value):
                    if isinstance(list_elem, PacketData):
                        v = list_elem._get_first(nix, path)
                        if v is not None:
                            path.insert(0, index)
                            path.insert(0, attr)
                            return v
        # if(path): path.pop(0)
        return None

    def work(self, nix, callback, args, addnix=None):
        write_nix = addnix if addnix else nix
        return self._work(nix, callback, args, write_nix, 0)

    def _work(self, nix, callback, args, write_nix, counter):
        if hasattr(self, nix):
            # print(f"found: {nix}")
            val = getattr(self, nix)
            call_val = callback(val, args)
            setattr(self, write_nix, call_val)
            counter += 1

        for attr, value in self.__dict__.items():
            if isinstance(value, PacketData):
                # print(f"work:  {attr}", file=sys.stderr)
                counter = value._work(nix, callback, args, write_nix, counter)
            elif isinstance(value, list):
                for list_elem in value:
                    if isinstance(list_elem, PacketData):
                        # print(f"work:  {attr}", file=sys.stderr)
                        counter = list_elem._work(nix, callback, args, write_nix, counter)

        return counter


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

            # dynamic packets might jump back or forward
            if (pnode.parameter.is_variable()):
                if (pnode.parameter.VPD_OFFSET != 0):
                    bitstream.pos += int(pnode.parameter.VPD_OFFSET)
            # static packets: each parameter describes its own absolute position
            else:
                bitstream.pos = pnode.parameter.PLF_OFFBY * 8 + pnode.parameter.PLF_OFFBI

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

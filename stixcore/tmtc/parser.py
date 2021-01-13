from collections import defaultdict

import numpy as np
from bitstring import ConstBitStream

from stixcore.util.logging import get_logger

logger = get_logger(__name__)

SUBPACKET_NIX = "NIX00403"


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

    def flatten(self):
        """Flatten the tree structure after parsing into a structure with all NIXs on the root level.

        Unpacks/decodes the NIXD0159 counts from TM(21,6,20) by eliminating this repeater level.

        Returns
        -------
        `PacketData`
            a flattend copy
        """
        new_root = PacketData()
        self._flatten(new_root)
        return new_root

    @staticmethod
    def unpack_NIX00065(param):
        NIX00065 = []
        for index, list_elem in enumerate(param):
            if isinstance(list_elem, PacketData):
                c_v = list_elem.NIX00065
                if isinstance(c_v, int):
                    NIX00065.append(c_v)
                elif isinstance(c_v, list) and len(c_v) == 2:
                    NIX00065.append(int.from_bytes((c_v[0]+1).to_bytes(2, 'big')
                                    + c_v[1].to_bytes(1, 'big'), 'big'))
                else:
                    raise ValueError(f'Continuation bits value of {len(c_v)} \
                    not allowed (0, 1, 2)')
            else:
                NIX00065.append(1)
        return (NIX00065, "NIX00065")

    def _flatten(self, new_root):
        for attr, value in self.__dict__.items():
            if isinstance(value, PacketData):
                # just a single subpacket
                if attr == SUBPACKET_NIX:
                    subpacket = PacketData()
                    value._flatten(subpacket)
                    setattr(new_root, SUBPACKET_NIX, [subpacket])
                else:
                    value._flatten(new_root)
            elif isinstance(value, list):
                if attr == "NIXD0159":
                    value, attr = PacketData.unpack_NIX00065(value)
                # multible subpackets
                if attr == SUBPACKET_NIX:
                    subpackets = []
                    for index, list_elem in enumerate(value):
                        subpacket = PacketData()
                        list_elem._flatten(subpacket)
                        subpackets.append(subpacket)
                    setattr(new_root, SUBPACKET_NIX, subpackets)
                    # skip the following loop
                    value = []

                for index, list_elem in enumerate(value):
                    if isinstance(list_elem, PacketData):
                        list_elem._flatten(new_root)
                    else:
                        if hasattr(new_root, attr):
                            old_val = getattr(new_root, attr)
                            if not isinstance(old_val, list):
                                old_val = [old_val, np.array(value)]
                            else:
                                old_val.append(np.array(value))
                            setattr(new_root, attr, old_val)

                        else:
                            setattr(new_root, attr, np.array(value))
                        break
            else:
                if hasattr(new_root, attr):
                    # TODO check if this is possible
                    raise Exception("add value not to repeater NIX")
                else:
                    setattr(new_root, attr, value)

    def set(self, nix, value):
        """Set the paremeter vale by the NIX name.

        Parameters
        ----------
        nix : `str`
            The NIX name of the parameter.
        value : 'any'
            The new value.
        """
        setattr(self, nix, value)

    def get(self, nix):
        """Get the parameter value for a given NIX name.

        Parameters
        ----------
        nix : `str`
            The NIX name of the parameter.

        Returns
        -------
        'any'
            The found value.
        """
        return getattr(self, nix) if hasattr(self, nix) else None

    def apply(self, nix, callback, args, addnix=None):
        """Apply a processing method to a parameter.

        Overids the value after processing or creates a new one.

        Parameters
        ----------
         nix : `str`
            The NIX name of the parameter.
        callback : `callable`
            the callback to by applayed to each data entry of the parameter.
        args : `any`
            will be passed on to the callback
        addnix : `str`, optional
            A NIX name where to overide or create a new parameter. by default None

        Returns
        -------
        'int'
            How many times the callback was invoked?
        """
        write_nix = addnix if addnix else nix
        counter = 0
        val = self.get(nix)
        if val is not None:
            if isinstance(val, list):
                w_val = [callback(v, args) for v in val]
                counter = len(w_val)
            else:
                w_val = callback(val, args)
                counter += 1
            self.set(write_nix, w_val)

        if self.has_subpackets():
            for subpacket in self.get_subpackets():
                counter += subpacket.apply(nix, callback, args, addnix)

        return counter

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__.keys()})'

    def has_subpackets(self):
        return hasattr(self, SUBPACKET_NIX)

    def get_subpackets(self):
        return getattr(self, SUBPACKET_NIX) if self.has_subpackets() else []


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
    return PacketData.parameter_dict_2_PacketData(fields).flatten()


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

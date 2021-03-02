from itertools import chain
from collections import defaultdict

import numpy as np
from bitstring import ConstBitStream

from stixcore.util.logging import get_logger

logger = get_logger(__name__)

__all__ = ['PacketData', 'Parameter']

SUBPACKET_NIX = "NIX00403"


class PacketData:
    """Generic class for organizing all parameters of the TM packet data."""

    @classmethod
    def parameter_list_2_PacketData(cls, parameters):
        """Convert the list nested parameters into a PacketData object.

        Unpacks/decodes the NIXD0159 counts from TM(21,6,20) by eliminating this repeater level.

        Parameters
        ----------
        parameters : `list`
            The parse result of the TM data block.

        Returns
        -------
        `PacketData`
            Generic data structure.
        """
        # if d is not a instance of dict then
        # directly object is returned
        if not isinstance(parameters, list):
            return parameters

        # constructor of the class passed to obj
        obj = PacketData()

        for parameter in parameters:
            parameter = parameter.merge_children()
            if parameter.name == 'NIXD0159':
                obj.__dict__[parameter.name] = PacketData.unpack_NIX00065(parameter)
            else:
                obj.__dict__[parameter.name] = parameter

        return obj

    def merge(self):
        """
        Merge sub structures

        Returns
        -------

        """
        for param in list(self.__dict__.values()):
            if param.children:
                for child in param.children:
                    child.flatten(root=self)
                    getattr(self, child.name).children = None

        return self

    @staticmethod
    def unpack_NIX00065(param):
        """Unpack the NIX00065 values.

        Continuation bits (NIXD0159) define number of subsequent bytes used to define counts for
        given Detector / Pixel / Energy combination, i.e. value 0 denotes no following bytes and
        count equal to 1, value 1 denotes 1 byte for “Counts” parameter with value between 2-255
        and continuation bits equal to 2 are used for 2 successive bytes for “Counts” parameter
        with value between 256 and 65535.

        Parameters
        ----------
        param : ´dict´
            parse entry

        Returns
        -------
        `tuple` (name, value)
            the name of the parameter to replace
            the unpacked value

        Raises
        ------
        ValueError
            if unpacking schema is not supported
        """
        NIX00065 = None
        if param.value == 0:
            NIX00065 = 1
        elif param.value == 1:
            NIX00065 = param.children[0].value
        elif param.value == 2:
            tmp = list(chain.from_iterable(*param.children[0].value))
            hb = tmp[0]
            lb = tmp[1]
            NIX00065 = (hb << 8) + lb
        else:
            raise ValueError(f'Continuation bits value of {param.value} \
            not allowed (0, 1, 2)')

        param = Parameter(name='NIX00065', value=NIX00065, idb_info='')
        return param

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

    def get(self, nix, aslist=False):
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
        attr = getattr(self, nix, None)
        if aslist:
            return [attr] if attr is not None else []
        return attr

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
                counter = len(w_val.value)
            else:
                w_val = callback(val, args)
                counter += 1
            self.set(write_nix, w_val)

        # if self.has_subpackets():
        #     for subpacket in self.get_subpackets():
        #         counter += subpacket.apply(nix, callback, args, addnix)

        return counter

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__.keys()})'

    def has_subpackets(self):
        return hasattr(self, SUBPACKET_NIX)

    def get_subpackets(self):
        return getattr(self, SUBPACKET_NIX, [])


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


class Parameter:
    """Generic Parameter Class

    Attributes
    _________
    name : `str`
        Parameter name
    value :
        The value of the parameter
    children : `list` optional
        Children of this parameter
    """
    def __init__(self, name, value, idb_info, children=None):
        self.name = name
        self.value = value
        self.idb_info = idb_info
        self.children = children

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name}, value={self.value}, ' \
               f'children={len(self.children) if self.children else None})'

    def merge_children(self):
        """
        Merge children into single parameter if same NIX

        Returns
        -------
        Parameter
            A new parameter with where are children are merged.
        """
        if self.children:
            if all([False if child.children else True for child in self.children]):
                params = {child.name: child for child in self.children}
                values = defaultdict(list)
                for param in self.children:
                    if isinstance(param.value, list):
                        values[param.name].extend(param.value)
                    else:
                        values[param.name].append(param.value)

                children = [Parameter(name, values[name], param.idb_info)
                            for name, param in params.items()]

                return Parameter(self.name, self.value, self.idb_info, children=children)
            else:
                return Parameter(self.name, self.value, self.idb_info,
                                 children=[c.merge_children() for c in self.children])
        else:
            return Parameter(self.name, self.value, self.idb_info)

    def flatten(self, root):
        """
        Flatten repeated substructures into root object

        Parameters
        ----------
        root : `object`

        Returns
        -------

        """
        if self.children:
            for child in self.children:
                if child.children:
                    child.flatten(root=root)
                else:
                    if hasattr(root, child.name):
                        cur = getattr(root, child.name)
                        if isinstance(child.value, list):
                            cur.value.extend(child.value)
                        else:
                            cur.value.append(child.value)
                    else:
                        if not isinstance(child.value, list):
                            child.value = [child.value]
                        setattr(root, child.name, child)
            self.children = None

        if hasattr(root, self.name):
            old = getattr(root, self.name)
            if not isinstance(old.value, list):
                old.value = [old.value]

            if isinstance(self.value, list):
                old.value.extend(self.value)
            else:
                old.value.append(self.value)
        else:
            self.value = [self.value]
            setattr(root, self.name, self)


def _parse_tree(bitstream, parent, fields):
    """Recursive parsing of TM data.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`
        Input binary data
    parent : `~stixcore/idb/idb/IDBPacketTree`
        the dynamic parse tree defined by the IDB
    fields : `list[stixcore.tmtc.parser.Parameter]`
        The parsed parameters - mutable out data.
    """
    if not parent:
        return
    counter = parent.counter

    for i in range(0, counter):
        for pnode in parent.children:

            # dynamic packets might jump back or forward
            if pnode.parameter.is_variable():
                if pnode.parameter.VPD_OFFSET != 0:
                    bitstream.pos += int(pnode.parameter.VPD_OFFSET)
            # static packets: each parameter describes its own absolute position
            else:
                bitstream.pos = pnode.parameter.PLF_OFFBY * 8 + pnode.parameter.PLF_OFFBI

            try:
                raw_val, children = (bitstream.read(pnode.parameter.bin_format), [])
            except Exception as e:
                raise e
            if pnode.children:
                num_children = raw_val
                is_valid = False
                if isinstance(num_children, int):
                    if num_children > 0:
                        pnode.counter = num_children
                        is_valid = True
                        _parse_tree(bitstream, pnode, children)
                if not is_valid:
                    if pnode.name != 'NIXD0159':
                        # repeater NIXD0159 can be zero according to STIX ICD-0812-ESC Table 93 P123
                        logger.warning(f'Repeater {pnode.name}  has an invalid value: {raw_val}')

            # entry = {'name': pnode.name, "value": raw_val}
            # if children:
            #     entry["children"] = children
            entry = Parameter(name=pnode.name, value=raw_val, idb_info=pnode.parameter,
                              children=children)
            fields.append(entry)


def parse_variable(bitstream, tree):
    """Parse binary data using given structure.

    Parameters
    ----------
    bitstream : `bitstream.ConstBitstream`
        Input binary data
    tree : `~stixcore/idb/idb/IDBPacketTree`
        the dynamic parse tree defined by the IDB
    Returns
    -------
    `PacketData`
        The parsed (nested) telemetry packet parameters.
    """
    fields = []
    _parse_tree(bitstream, tree, fields)
    pd = PacketData.parameter_list_2_PacketData(fields)
    merged = pd.merge()
    return merged


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


def split_into_length(ar, splits):
    """Split a list into n chunks with a given length each

    Parameters
    ----------
    ar :  `list`
        List like input to split up
    splits : `list`
        list of n length

    Returns
    -------
    `list`
        a list of numpy.array chunks

    Raises
    ------
    ValueError
        if the sum of all length in splits under or overflows
    """
    parts = []
    start = 0
    if isinstance(splits, int):
        splits = [splits]

    for split in splits:
        parts.append(np.array(ar[start: start + split]))
        start += split
    if start < len(ar):
        raise ValueError("Length does not match to short")
    elif start > len(ar):
        raise ValueError("Length does not match to long")
    return parts

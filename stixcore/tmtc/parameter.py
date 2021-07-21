from collections import defaultdict

import astropy.units as u

__all__ = ['Parameter', 'EngineeringParameter', 'CompressedParameter']


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
        if self.name == 'NIXD0159':
            return self.unpack_NIX00065()
        elif self.children:
            names = (c.name for c in self.children)
            if all([False if child.children else True for child in self.children]) \
                    and 'NIXD0159' not in names:
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

    def unpack_NIX00065(self):
        """Unpack the NIX00065 values.

        Continuation bits (NIXD0159) define number of subsequent bytes used to define counts for
        given Detector / Pixel / Energy combination, i.e. value 0 denotes no following bytes and
        count equal to 1, value 1 denotes 1 byte for “Counts” parameter with value between 2-255
        and continuation bits equal to 2 are used for 2 successive bytes for “Counts” parameter
        with value between 256 and 65535.

        Parameters
        ----------
        param : ´Parameter´
            The NIXD00159 parameter

        Returns
        -------
        `Parameter`
            The unpacked value

        Raises
        ------
        ValueError
            if unpacking schema is not supported
        """
        NIX00065 = None
        if self.value == 0:
            NIX00065 = 1
            child_idb_info = None
        elif self.value == 1:
            NIX00065 = self.children[0].value
            child_idb_info = self.children[0].idb_info
        elif self.value == 2:
            high_bit, low_bit = [c.value for c in self.children]
            NIX00065 = (high_bit << 8) + low_bit
            child_idb_info = self.children[0].idb_info
        else:
            raise ValueError(f'Continuation bits value of {self.value} \
            not allowed (0, 1, 2)')

        param = Parameter(name=self.name, value=self.value, idb_info=self.idb_info,
                          children=[Parameter(name='NIX00065', value=NIX00065,
                                              idb_info=child_idb_info)])
        return param


class EngineeringParameter(Parameter):
    """A class to combine the raw and engineering values and settings of a parameter.

    Attributes
    ----------
    value : `int`|`list`
        The original or raw values before the calibration.
    engineering : `int`|`list`
        The Engineering values.
    unit : `str`
        The unit for the engineering values
    """

    def __init__(self, *, name, value, idb_info, engineering, unit):
        """Create a EngineeringParameter object.

        Parameters
        ----------
        value : `int`|`list`
            The raw values.
        engineering : `int`|`list`
            The engineering values.
        """
        super(EngineeringParameter, self).__init__(name=name, value=value, idb_info=idb_info)

        self.unit = unit
        convert = False

        if unit == 'degC':
            unit = 'deg_C'
            convert = 'K'

        if unit is not None and unit != '' and engineering is not None:
            try:
                engineering = engineering * u.Unit(unit)
                if convert == 'K':
                    engineering = engineering.to(convert, equivalencies=u.temperature())
            except ValueError:
                raise NotImplementedError(f"Add unit support: for {unit}")

        self.engineering = engineering

    def __repr__(self):
        return f'{self.__class__.__name__}(value={self.value}, engineering={self.engineering}, ' + \
               f'unit={self.unit})'

    def __str__(self):
        return f'{self.__class__.__name__}(value: len({len(self.value)}), engineering: ' + \
               f'len({len(self.engineering)}), unit={self.unit})'


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

"""
Array like time objects
"""

import operator

import numpy as np
from sunpy.time.timerange import TimeRange

import astropy.units as u
from astropy.time.core import Time
from astropy.utils import ShapedLikeNDArray
from astropy.utils.data_info import MixinInfo

from stixcore.data.test import test_data
from stixcore.ephemeris.manager import Time as SpiceTime

SPICE_TIME = SpiceTime(meta_kernel_path=test_data.ephemeris.META_KERNEL_TIME)

__all__ = ['SCETBase', 'SCETime', 'SCETimeDelta', 'SCETimeRange']

# SOLO convention top bit is for time sync
MAX_COARSE = 2**32 - 1
MAX_FINE = 2**16 - 1


class TimeInfo(MixinInfo):
    attr_names = MixinInfo.attr_names
    _supports_indexing = True

    def get_sortable_arrays(self):
        pass

    #
    # @property
    # def unit(self):
    #     return None

    def new_like(self, cols, length, metadata_conflicts='warn', name=None):
        """
        Return a new instance of this class which is consistent with the
        input ``cols`` and has ``length`` rows.

        This is intended for creating an empty column object whose elements can
        be set in-place for table operations like join or vstack.

        Parameters
        ----------
        cols : list
            List of input columns
        length : int
            Length of the output column object
        metadata_conflicts : str ('warn'|'error'|'silent')
            How to handle metadata conflicts
        name : str
            Output column name

        Returns
        -------
        col : object
            New instance of this class consistent with ``cols``
        """
        # Get merged info attributes like shape, dtype, format, description, etc.
        attrs = self.merge_cols_attributes(cols, metadata_conflicts, name, ('meta', 'description'))
        attrs.pop('dtype')  # Not relevant for Time

        # cols[0]
        # for col in cols[1:]:
        #     # This is the method used by __setitem__ to ensure that the right side
        #     # has a consistent location (and coerce data if necessary, but that does
        #     # not happen in this case since `col` is already a Time object).  If this
        #     # passes then any subsequent table operations via setitem will work.
        #     try:
        #         col0._make_value_equivalent(slice(None), col)
        #     except ValueError:
        #         raise ValueError('input columns have inconsistent locations')

        # Make a new Time object with the desired shape and attributes
        shape = (length,) + attrs.pop('shape')
        coarse = np.zeros(shape, dtype=np.int32)
        fine = np.zeros(shape, dtype=np.int32)
        out = self._parent_cls(coarse, fine)

        # Set remaining info attributes
        for attr, value in attrs.items():
            setattr(out.info, attr, value)

        return out


class TimeDetlaInfo(TimeInfo):
    pass
    # _represent_as_dict_attrs = ('seconds')
    # _represent_as_dict_primary_data = 'seconds'
    #
    # attrs_names = MixinInfo.attr_names | {'serialize_method'}
    #
    # def _represent_as_dict(self, attrs=None):
    #     out = super()._represent_as_dict()
    #
    #     col = self._parent
    #
    #     # If the serialize method for this context (e.g. 'fits' or 'ecsv') is
    #     # 'data_mask', that means to serialize using an explicit mask column.
    #     method = self.serialize_method[self._serialize_context]
    #
    # def __init__(self, bound=False):
    #     super().__init__(bound)
    #
    #     if bound:
    #         self.serialize_method = {'fits': 'seconds',
    #                                  None: 'seconds'}


class SCETBase(ShapedLikeNDArray):
    """Base time class from which SCETime and SCETimeDelta inherit."""
    _astropy_column_attrs = None

    # Make sure reverse (radd, rsub, ...) magic methods are called over others
    __array_priority__ = 20000

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], cls):
            self = args[0].copy()
        else:
            self = super().__new__(cls)
        return self

    def __init__(self, coarse, fine):
        self.coarse = coarse
        self.fine = fine

    @property
    def shape(self):
        return self.coarse.shape

    def _apply(self, method, *args, **kwargs):

        if callable(method):
            apply_method = lambda array: method(array, *args, **kwargs)  # noqa: E731

        else:
            if method == 'replicate':
                apply_method = None
            else:
                apply_method = operator.methodcaller(method, *args, **kwargs)

        coarse, fine = self.coarse, self.fine
        if apply_method:
            coarse = apply_method(coarse)
            fine = apply_method(fine)

        out = self.__class__(coarse, fine)
        if 'info' in self.__dict__:
            out.info = self.info

        return out

    # TODO check v spice
    def as_float(self):
        """
        Return a float representation of the SCET and time e.g. coarse+fine*(1/MAX_FINE)

        Returns
        -------

        """
        return self.coarse + (self.fine / MAX_FINE) << u.s

    def min(self, axis=None, out=None, keepdims=False):
        """
        Return the minimum of time or minimum time along an axis

        Parameters
        ----------
        axis :
        out :

        keepdims : `boolean`
            Keep the dimension of the original array

        Returns
        -------

        """
        return self[self._advanced_index(self.argmin(axis), axis, keepdims)]

    def max(self, axis=None, out=None, keepdims=False):
        """
        Return the maximum time or maximum time along an axis

        Parameters
        ----------
        axis :

        out :

        keepdims : `boolean`
            Keep the dimension of the original array

        Returns
        -------

        """
        return self[self._advanced_index(self.argmax(axis), axis, keepdims)]

    def argmin(self, axis=None, out=None):
        """
        Return indices of the minimum values along the given axis

        Parameters
        ----------
        axis
        out

        Returns
        -------

        """
        float_rep = self.as_float()
        return float_rep.argmin(axis, out)

    def argmax(self, axis=None, out=None):
        """
        Return indices of the maximum values along the given axis

        Parameters
        ----------
        axis :
        out :

        Returns
        -------

        """
        float_rep = self.as_float()
        return float_rep.argmax(axis, out)

    def _advanced_index(self, indices, axis=None, keepdims=False):
        """Turn argmin, argmax output into an advanced index.

        Argmin, argmax output contains indices along a given axis in an array
        shaped like the other dimensions.  To use this to get values at the
        correct location, a list is constructed in which the other axes are
        indexed sequentially.  For ``keepdims`` is ``True``, the net result is
        the same as constructing an index grid with ``np.ogrid`` and then
        replacing the ``axis`` item with ``indices`` with its shaped expanded
        at ``axis``. For ``keepdims`` is ``False``, the result is the same but
        with the ``axis`` dimension removed from all list entries.

        For ``axis`` is ``None``, this calls :func:`~numpy.unravel_index`.

        Parameters
        ----------
        indices : array
            Output of argmin or argmax.
        axis : int or None
            axis along which argmin or argmax was used.
        keepdims : bool
            Whether to construct indices that keep or remove the axis along
            which argmin or argmax was used.  Default: ``False``.

        Returns
        -------
        advanced_index : list of arrays
            Suitable for use as an advanced index.
        """
        if axis is None:
            return np.unravel_index(indices, self.shape)

        ndim = self.ndim
        if axis < 0:
            axis = axis + ndim

        if keepdims and indices.ndim < self.ndim:
            indices = np.expand_dims(indices, axis)

        index = [indices
                 if i == axis
                 else np.arange(s).reshape(
                     (1,) * (i if keepdims or i < axis else i - 1)
                     + (s,)
                     + (1,) * (ndim - i - (1 if keepdims or i > axis else 2))
                 )
                 for i, s in enumerate(self.shape)]

        return tuple(index)

    def __setitem__(self, item, value):
        self.coarse[item] = value.coarse
        self.fine[item] = value.fine

    def __repr__(self):
        return f'{self.__class__.__name__}(coarse={self.coarse}, fine={self.fine})'


class SCETime(SCETBase):
    """
    SolarOrbiter Spacecraft Elapse Time (SCET) or Onboard Time (OBT).

    The mission clock time is compose of a coarse time in seconds in 32bit field and fine time
    in 16bit field fractions of second 1s/(2**16 -1) can be represented as a single 48bit field
    or a float.

    The top most bit is used to indicate time sync issues.

    Attributes
    ----------
    time_sync : bool
        Time synchronisation status
    coarse : int
        Coarse time stamp (seconds)
    fine : int
        Fine time stamp fraction of seconds 1/2**31 -1

    Examples
    --------
    SCETimes can be created in a number of ways from scaler values,

    >>> SCETime(123, 456)
    SCETime(coarse=123, fine=456)

    combinations of scalers and array-like,

    >>> SCETime(123, [1,2,3,4,5])
    SCETime(coarse=[123 123 123 123 123], fine=[1 2 3 4 5])

    or from seconds with the understanding this are from the epoch of the start or the SCET

    >>> SCETime.from_float(123.345*u.s)
    SCETime(coarse=123, fine=22610)

    """
    info = TimeInfo()

    def __init__(self, coarse, fine=0):
        """
        Create a new datetime using the given coarse and fine values.

        Parameters
        ----------
        coarse : `int` or `SCETime`
            Coarse time stamp (seconds) or eixting `SCETime`
        fine : `int`
            Fine time stamp fraction of seconds 1/2**16 -1
        """
        if not isinstance(coarse, SCETime):
            coarse, fine = np.broadcast_arrays(coarse, fine)
            if not np.issubdtype(coarse.dtype, np.integer) \
                    or not np.issubdtype(fine.dtype, np.integer):
                raise ValueError('Coarse and fine times must be integers')
            # Convention if top bit is set means times are not synchronised
            time_sync = (coarse >> 31) != 1
            # coarse = np.where(time_sync, coarse, coarse ^ 2 ** 31)

            # Check limits
            if np.any(np.logical_or(coarse < 0, coarse > MAX_COARSE)):
                raise ValueError(f'Coarse time must be in range (0 to {MAX_COARSE})')
            if np.any(np.logical_or(fine < 0, fine > MAX_FINE)):
                raise ValueError(f'Fine time must be in range (0 to {MAX_FINE})')

            # Can store as uints
            coarse = coarse.astype(np.uint32)
            fine = fine.astype(np.uint16)
            super().__init__(coarse, fine)
            self.time_sync = time_sync

    @classmethod
    def from_float(cls, scet_float):
        """
        Create an SCETime from a float representation of seconds since epoch

        Parameters
        ----------
        scet_float : `astropy.units.Quantity`
            The scet float representation

        Returns
        -------
        `SCETime`
            The SCETime object
        """
        sub_seconds, seconds = np.modf(scet_float.to_value('s'))
        coarse = seconds.astype(int)
        fine = np.round(MAX_FINE * sub_seconds).astype(np.int)
        return SCETime(coarse, fine)

    @classmethod
    def from_string(cls, scet_str, sep=':'):
        """
        Create an SCETime for a string representation e.g. `'123456:789'`

        Parameters
        ----------
        scet_str : `array_like`
            Time/s in SCET string format
        Returns
        -------
        `SCETime`
            The SCETime object
        """
        if isinstance(scet_str, str):
            scet_str = [scet_str]
        coarse, fine = zip(*[list(map(int, ts.split(sep))) for ts in scet_str])
        return SCETime(coarse=coarse, fine=fine)

    def to_datetime(self):
        """
        Return a python datetime object.

        Returns
        -------
        `datetime.datetime`
            The corresponding UTC datetime object.
        """
        with SPICE_TIME as time:
            try:
                utc = [time.scet_to_datetime(t.to_string()) for t in self]
            except TypeError:
                utc = time.scet_to_datetime(self.to_string())

            return utc

    def to_time(self):
        return Time(self.to_datetime())

    def to_string(self, full=True, sep=':'):
        if self.size == 1:
            if full:
                return f'{self.coarse:010d}{sep}{self.fine:05d}'
            return f'{self.coarse:010d}'

    @staticmethod
    def min_time():
        """
        The minimum possible time value

        """
        return SCETime(0, 0)

    @staticmethod
    def max_time():
        """
        The maximum possible time value

        """
        return SCETime(MAX_COARSE, MAX_FINE)

    def __add__(self, other):
        """
        Can only add a SCETimeDeltas or Quantities that can be converted to seconds

        It doesn't make sense to add two 'times'
        """
        if not isinstance(other, (u.Quantity, SCETimeDelta)):
            raise TypeError('Only Quantities and SCETimeDeltas can be added to SCETimes')

        if isinstance(other, u.Quantity):
            other = SCETimeDelta.from_float(other.to(u.s))

        delta_coarse, new_fine = divmod(self.fine + other.fine, MAX_FINE + 1)
        new_coarse = (self.coarse.astype(np.int64) + other.coarse.astype(np.int64)
                      + delta_coarse.astype(np.int64))
        return SCETime(coarse=new_coarse, fine=new_fine)

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        """
        Can subtract one time from another, can subtract a time delta or Quantity from an SCETime
        """
        if not isinstance(other, (SCETime, SCETimeDelta, u.Quantity)):
            raise TypeError('Only quantities, SCETime and SCETimeDelta '
                            'objects can be subtracted from SCETimes')

        if isinstance(other, SCETime):
            coarse = self.coarse.astype(np.int32) - other.coarse.astype(np.int32)
            fine = self.fine.astype(np.int32) - other.fine.astype(np.int32)
            return SCETimeDelta(coarse, fine)

        if isinstance(other, u.Quantity):
            other = SCETimeDelta.from_float(other)

        delta_coarse, new_fine = np.divmod(self.fine - other.fine, MAX_FINE+1)
        new_coarse = self.coarse - other.coarse + delta_coarse
        return SCETime(coarse=new_coarse, fine=new_fine)

    def __str__(self):
        return f'{self.coarse}:{self.fine}'

    def _comparison_operator(self, other, op):
        if other.__class__ is not self.__class__:
            return NotImplemented

        return op(self.as_float() - other.as_float(), 0)

    def __gt__(self, other):
        return self._comparison_operator(other, operator.gt)

    def __ge__(self, other):
        return self._comparison_operator(other, operator.ge)

    def __lt__(self, other):
        return self._comparison_operator(other, operator.lt)

    def __le__(self, other):
        return self._comparison_operator(other, operator.le)

    def __eq__(self, other):
        return self._comparison_operator(other, operator.eq)

    def __ne__(self, other):
        return self._comparison_operator(other, operator.ne)


class SCETimeDelta(SCETBase):
    """
    SCET time delta objects which can be created.

    Attributes
    ----------
    coarse : int
        Coarse time stamp (seconds)
    fine : int
        Fine time stamp fraction of seconds 1/(2**16-1)

    Examples
    --------
    SCETimeDeltas can be created from directly

    >>> SCETimeDelta(1, 2)
    SCETimeDelta(coarse=1, fine=2)

    or as as result of subtracting two times

    >>> SCETime(9, 8) - SCETime(5, 10)
    SCETimeDelta(coarse=4, fine=-2)

    """
    info = TimeDetlaInfo()

    def __init__(self, coarse, fine=0):
        """
        Create a new delta time using the given coarse and fine values.

        Parameters
        ----------
        coarse : `int`
            Coarse time stamp (seconds)
        fine : `int`
            Fine time stamp fraction of seconds 1/2**16 -1
        """
        if not isinstance(coarse, SCETimeDelta):
            if isinstance(coarse, u.Quantity):
                coarse, fine = self._convert_float(coarse)
            coarse, fine = np.broadcast_arrays(coarse, fine)
            if not np.issubdtype(coarse.dtype, np.integer) \
                    or not np.issubdtype(fine.dtype, np.integer):
                raise ValueError('Coarse and fine times must be integers')
            if np.any(np.abs(coarse) > MAX_COARSE):
                raise ValueError('Course time must be in the range -2**31-1 to 2**31-1')
            if np.any(np.abs(fine) > MAX_FINE):
                raise ValueError('Fine time must be in the range -2**16-1 to 2**16-1')
            # Fine needs to be 32bit as due to SO convention coarse has max of 2**31 which works
            # for a signed 32 bit in but but fine uses all 16bit as a uint as has to be 32 as a int
            super().__init__(coarse.astype(np.int32), fine.astype(np.int32))

    # @property
    # def seconds(self):
    #     return self.as_float()

    @classmethod
    def from_float(cls, scet_float):
        """
        Create a

        Parameters
        ----------
        scet_float

        Returns
        -------

        """
        coarse, fine = cls._convert_float(scet_float)
        return cls(coarse, fine)

    @staticmethod
    def _convert_float(scet_float):
        scet_float = scet_float.to_value('s')
        sub_seconds, seconds = np.modf(scet_float)
        fine = np.round(MAX_FINE * sub_seconds).astype(int)
        return seconds.astype(int), fine

    def __add__(self, other):
        # If other is a Time then use SCETime.__add__ to do the calculation.
        if isinstance(other, SCETime):
            return other.__add__(self)

        if not isinstance(other, SCETimeDelta):
            other = SCETimeDelta(other)

        delta_coarse, new_fine = divmod(self.fine + other.fine, MAX_FINE)
        new_coarse = self.coarse + other.coarse + delta_coarse
        new_fine = self.fine + other.fine
        return SCETimeDelta(new_coarse, new_fine)

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        if isinstance(other, (u.Quantity, SCETimeDelta)):
            try:
                other = SCETimeDelta(other)
            except Exception:
                raise TypeError(f'{other.__class__.__name__} could not '
                                f'be converted to {self.__class__.__name__}')

            sign = 1
            if self.fine < other.fine:
                sign = -1
            delta_coarse, new_fine = divmod(self.fine - other.fine, sign*MAX_FINE)
            new_coarse = self.coarse - other.coarse + delta_coarse
            new_fine = self.fine - other.fine
            return SCETimeDelta(new_coarse, new_fine)
        else:
            raise TypeError(f'Unsupported operation for types {self.__class__.__name__} '
                            f'and {other.__class__.__name__}')

    def __rsub__(self, other):
        out = self.__sub__(other)
        return -out

    def __neg__(self):
        new = self.copy()
        new.coarse = -new.coarse
        new.fine = -new.fine
        return new

    def __truediv__(self, other):
        res = self.as_float()/other
        return SCETimeDelta.from_float(res)

    def __mul__(self, other):
        res = self.as_float() * other
        return SCETimeDelta.from_float(res)

    def __str__(self):
        return f'{self.coarse}, {self.fine}'

    def __eq__(self, other):
        if not isinstance(other, (SCETimeDelta, u.Quantity)):
            return False

        if isinstance(other, u.Quantity):
            other = SCETimeDelta(other)

        return self.coarse == other.coarse and self.fine == other.fine


class SCETimeRange:
    """
    SolarOrbiter Spacecraft Elapse Time (SCET) Range with start and end time.

    Attributes
    ----------
    start : `SCETime`
        start time of the range
    end : `SCETime`
        end time of the range
    """
    def __init__(self, *, start=SCETime.max_time(), end=SCETime.min_time()):
        if not isinstance(start, SCETime) or not isinstance(end, SCETime):
            raise TypeError('Must be SCETime')

        self.start = start.min()
        self.end = end.max()

    def expand(self, time):
        """
        Enlarge the time range to include the given time.

        Parameters
        ----------
        time : `SCETime` or `SCETimeRange`
            The new time the range should include or an other time range.

        Raises
        ------
        ValueError
            if the given time is from a other class.
        """
        if isinstance(time, SCETime):
            self.start = min(self.start, time.min())
            self.end = max(self.end, time.max())
        elif isinstance(time, SCETimeRange):
            self.start = min(self.start, time.start)
            self.end = max(self.end, time.end)
        else:
            raise ValueError("time must be 'SCETime' or 'SCETimeRange'")

    def to_timerange(self):
        return TimeRange(self.start.to_time(), self.end.to_time())

    @property
    def avg(self):
        return self.start + (self.end-self.start)/2

    def __repr__(self):
        return f'{self.__class__.__name__}(start={str(self.start)}, end={str(self.end)})'

    def __str__(self):
        return (f'{str(self.start)} to ' +
                f'{str(self.end)}')

    def __contains__(self, item):
        if isinstance(item, SCETime):
            return self.start <= item <= self.end
        elif isinstance(item, SCETimeRange):
            return self.start <= item.start and self.end >= item.end
        else:
            raise ValueError("time must be 'SCETime' or 'SCETimeRange'")

from pathlib import Path
from datetime import datetime
from itertools import chain

import numpy as np
from sunpy.util.datatype_factory_base import (
    BasicRegistrationFactory,
    MultipleMatchError,
    NoMatchError,
)

import astropy.units as u
from astropy.io import fits
from astropy.table.operations import unique, vstack
from astropy.table.table import QTable
from astropy.time import Time

import stixcore.processing.decompression as decompression
import stixcore.processing.engineering as engineering
from stixcore.idb.manager import IDBManager
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import PacketSequence
from stixcore.util.util import get_incomplete_file_name

__all__ = ['GenericProduct', 'ProductFactory', 'Product', 'ControlSci',
           'EnergyChannelsMixin', 'read_qtable',
           'Control', 'Data', 'L1Mixin', 'L2Mixin']

from collections import defaultdict

from stixcore.products.common import _get_energies_from_mask, get_min_uint
from stixcore.util.logging import get_logger

logger = get_logger(__name__)

BITS_TO_UINT = {
    8: np.ubyte,
    16: np.uint16,
    32: np.uint32,
    64: np.uint64
}

# date when the min integration time was changed from 1.0s to 0.5s needed to fix count and time
# offset issue
MIN_INT_TIME_CHANGE = datetime(2021, 9, 6, 13)


def read_qtable(file, hdu, hdul=None):
    """
    Read a fits file into a QTable and maintain dtypes of columns with units

    Hack to work around QTable not respecting the dtype in fits file see
    https://github.com/astropy/astropy/issues/12494

    Parameters
    ----------
    file : `str` or `pathlib.Path`
        Fits file
    hdu : `str`
        The name of the extension
    hdul : optional `astropy.io.fits.HDUList`
        The HDU list for the fits file

    Returns
    -------
    `astropy.table.QTable`
        The corrected QTable with correct data types
    """
    qtable = QTable.read(file, hdu)
    if hdul is None:
        hdul = fits.open(file)

    for col in hdul[hdu].data.columns:
        if col.unit:
            logger.debug(f'Unit present dtype correction needed for {col}')
            dtype = col.dtype

            if col.bzero:
                logger.debug(f'Unit present dtype and bzero correction needed for {col}')
                bits = np.log2(col.bzero)
                if bits.is_integer():
                    dtype = BITS_TO_UINT[int(bits+1)]

            if hasattr(dtype, 'subdtype'):
                dtype = dtype.base

            qtable[col.name] = qtable[col.name].astype(dtype)

    return qtable


class AddParametersMixin:
    def add_basic(self, *, name, nix, packets, attr=None, dtype=None, reshape=False):
        r"""
        Add parameter taken directly from the packets.
        Parameters
        ----------
        name : `str`
            Name of the column for the data
        nix : `str`
            Parameters id
        packets : `
        attr : `str`
            The attribute e.g `raw`, or `error`
        dtype :
            The type to store the data as
        reshape: `bool`
            Reshape to a `1, X` array


        Returns
        -------
        None
        """
        value = packets.get_value(nix, attr=attr)
        if reshape is True:
            value = value.reshape(1, -1)
        self[name] = value if dtype is None else value.astype(dtype)
        self.add_meta(name=name, nix=nix, packets=packets, add_curtx=(attr == "value"))

    def add_data(self, name, data_meta):
        r"""
        Add the given data and metadata to table.

        Parameters
        ----------
        name : `str`
            Name of the column for the data
        data_meta : `tuple`
            Data, meta data pair

        Returns
        -------
        None

        """
        data, meta = data_meta
        self[name] = data
        self[name].meta = meta

    def add_meta(self, *, name, nix, packets, add_curtx=False):
        r"""
        Add metadata extracted from packets.

        Parameters
        ----------
        name : `str`
            Name of the column for the data
        nix : `str`
            Parameters ID e.g. 'NIX00404'
        packets :
            The packets
        add_curtx


        Returns
        -------

        """
        param = packets.get(nix)
        idb_info = param[0].idb_info
        meta = {'NIXS': nix}
        if add_curtx:
            meta['PCF_CURTX'] = idb_info.PCF_CURTX
        self[name].meta = meta


class BaseProduct:
    """
    Base TMProduct that all other product inherit from contains the registry for the factory pattern
    """

    PRODUCT_PROCESSING_VERSION = 2

    _registry = {}

    def __init_subclass__(cls, **kwargs):
        """__init_subclass__ hook initializes all of the subclasses of a given class.

        So for each subclass, it will call this block of code on import.
        This registers each subclass in a dict that has the `is_datasource_for` attribute.
        This is then passed into the factory so we can register them.
        """
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'is_datasource_for'):
            cls._registry[cls] = cls.is_datasource_for

    @property
    def fits_daily_file(self):
        raise NotImplementedError("SubClass of BaseProduct should implement")

    def get_processing_version(self):
        version = self.__class__.PRODUCT_PROCESSING_VERSION\
            if hasattr(self.__class__, 'PRODUCT_PROCESSING_VERSION') else 1
        return max(version, BaseProduct.PRODUCT_PROCESSING_VERSION)

    @classmethod
    def get_cls_processing_version(cls):
        version = cls.PRODUCT_PROCESSING_VERSION\
            if hasattr(cls, 'PRODUCT_PROCESSING_VERSION') else 1
        return max(version, BaseProduct.PRODUCT_PROCESSING_VERSION)


class ProductFactory(BasicRegistrationFactory):
    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            if isinstance(args[0], (str, Path)):
                file_path = Path(args[0])
                pri_header = fits.getheader(file_path)

                service_type = int(pri_header.get('stype')) if 'stype' in pri_header else 0
                service_subtype = int(pri_header.get('sstype')) if 'sstype' in pri_header else 0
                parent = pri_header.get('parent').split(';') if 'parent' in pri_header else ''
                raw = pri_header.get('raw_file').split(';') if 'raw_file' in pri_header else ''
                comment = [c for c in pri_header.get('comment', [])]
                history = [h for h in pri_header.get('history', [])]

                try:
                    ssid = int(pri_header.get('ssid'))
                except (ValueError, TypeError):
                    ssid = None
                level = pri_header.get('Level')
                pri_header.get('TIMESYS')

                hdul = fits.open(file_path)
                control = read_qtable(file_path, hdu='CONTROL', hdul=hdul)

                # Weird issue where time_stamp wasn't a proper table column?
                if 'time_stamp' in control.colnames:
                    ts = control['time_stamp'].value
                    control.remove_column('time_stamp')
                    control['time_stamp'] = ts * u.s

                data = read_qtable(file_path, hdu='DATA', hdul=hdul)

                if level == 'LL01':
                    # TODO remova that hack in favor for a proper header information after
                    # https://github.com/i4Ds/STIXCore/issues/224 is solved
                    if "lightcurve" in args[0]:
                        service_type = 21
                        service_subtype = 6
                        ssid = 30
                    if "flareflag" in args[0]:
                        service_type = 21
                        service_subtype = 6
                        ssid = 34

                if level not in ['LB', 'LL01']:
                    data['timedel'] = SCETimeDelta(data['timedel'])
                    offset = SCETime.from_float(pri_header['OBT_BEG']*u.s)

                    try:
                        control['time_stamp'] = SCETime.from_float(control['time_stamp'])
                    except KeyError:
                        pass

                    data['time'] = offset + data['time']

                energies = None
                if level == 'L1':
                    try:
                        energies = read_qtable(file_path, hdu='ENERGIES')
                    except KeyError:
                        logger.debug(f"no ENERGIES data found in FITS: {file_path}")
                idb_versions = defaultdict(SCETimeRange)
                if level in ('L0', 'L1'):
                    try:
                        idbt = read_qtable(file_path, hdu='IDB_VERSIONS')
                        for row in idbt.iterrows():
                            idb_versions[row[0]] = SCETimeRange(start=SCETime.from_float(row[1]),
                                                                end=SCETime.from_float(row[2]))
                    except KeyError:
                        logger.warn(f"no IDB data found in FITS: {file_path}")

                Product = self._check_registered_widget(level=level, service_type=service_type,
                                                        service_subtype=service_subtype,
                                                        ssid=ssid, control=control,
                                                        data=data, energies=energies)

                p = Product(level=level, service_type=service_type,
                            service_subtype=service_subtype,
                            ssid=ssid, control=control,
                            data=data, energies=energies, idb_versions=idb_versions,
                            raw=raw, parent=parent, comment=comment, history=history)

                if hasattr(p, 'get_additional_extensions') and data is not None:
                    for _, name in p.get_additional_extensions():
                        # read the additional extension data
                        if name.upper() in hdul:
                            data_ext = read_qtable(file_path, hdu=name.upper(), hdul=hdul)
                            if 'timedel' in data_ext.colnames:
                                data_ext['timedel'] = SCETimeDelta(data_ext['timedel'])
                            if 'time' in data_ext.colnames:
                                data_ext['time'] = offset + data_ext['time']
                            setattr(p, name, data_ext)

                # store the old fits header for later reuse
                if isinstance(p, (L1Mixin, L2Mixin)):
                    p.fits_header = pri_header

                return p

    def _check_registered_widget(self, *args, **kwargs):
        """
        Implementation of a basic check to see if arguments match a widget.
        """
        candidate_widget_types = list()

        for key in self.registry:

            # Call the registered validation function for each registered class
            if self.registry[key](*args, **kwargs):
                candidate_widget_types.append(key)

        n_matches = len(candidate_widget_types)

        if n_matches == 0:
            if self.default_widget_type is None:
                raise NoMatchError("No types match specified arguments and no default is set.")
            else:
                candidate_widget_types = [self.default_widget_type]
        elif n_matches > 1:
            raise MultipleMatchError("Too many candidate types identified ({})."
                                     "Specify enough keywords to guarantee unique type "
                                     "identification.".format(n_matches))

        # Only one is found
        WidgetType = candidate_widget_types[0]

        return WidgetType


class Control(QTable, AddParametersMixin):

    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate the start time of each for the number of samples in that packet
        base_coarse, base_fine = zip(*[([ct] * ns, [ft] * ns) for ns, ct, ft in
                                       self[['num_samples', 'scet_coarse', 'scet_fine']]])
        base_coarse = np.hstack(base_coarse)
        base_fine = np.hstack(base_fine)
        bases = SCETime(base_coarse, base_fine)

        # Create start time for each time bin by multiplying the duration by the sample number
        deltas = SCETimeDelta(np.hstack([(np.arange(ns) * it)
                              for ns, it in self[['num_samples', 'integration_time']]]))

        # Create integration time for each sample in each packet by replicating the duration for
        # number of sample in each packet
        durations = SCETimeDelta(np.hstack([np.ones(num_sample) * int_time for num_sample, int_time
                                            in self[['num_samples', 'integration_time']]]))

        # Add the delta time to base times and convert to bin centers
        times = bases + deltas + (durations / 2)

        # Create a time range object covering the total observation time
        tr = SCETimeRange(start=bases[0], end=bases[-1]+deltas[-1] + durations[-1])

        return times, durations, tr

    @classmethod
    def from_packets(cls, packets, NIX00405_offset=0):
        """
        Common generator method the create and prepare the control table.

        Parameters
        ----------
        packets : `PacketSequence`
            The set of packets of same data type for what the data product will be created.
        NIX00405_offset : int, optional
            NIX00405 (integration time) is used in two ways (X * 0.1s) or ((X+1) * 0.1s),
            by default 0

        Returns
        -------
        `Control`
            The Control object for the data product.
        """
        # Header
        control = cls()
        # self.energy_bin_mask = None
        # self.samples = None
        control.add_basic(name='scet_coarse', nix='NIX00445', packets=packets, dtype=np.uint32)
        # Not all QL data have fine time in TM default to 0 if no present
        try:
            control.add_basic(name='scet_fine', nix='NIX00446', packets=packets)
        except AttributeError:
            control['scet_fine'] = np.zeros_like(control['scet_coarse'], np.uint32)

        try:
            control['integration_time'] = (packets.get_value('NIX00405') + (NIX00405_offset * u.s))
            control.add_meta(name='integration_time', nix='NIX00405', packets=packets)
        except AttributeError:
            control['integration_time'] = np.zeros_like(control['scet_coarse'], float) * u.s

        # control = unique(control)
        control['index'] = np.arange(len(control)).astype(get_min_uint(len(control)))

        return control


class ControlSci(QTable, AddParametersMixin):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate packet time for each sample
        base_times = Time(list(chain(
            *[[SCETime(coarse=self["scet_coarse"][i], fine=self["scet_fine"][i])]
              * n for i, n in enumerate(self['num_samples'])])))
        # For each sample generate sample number and multiply by duration and apply unit
        start_delta = np.hstack(
            [(np.arange(ns) * it) for ns, it in self[['num_samples', 'integration_time']]])
        # hstack op loses unit
        start_delta = start_delta.value * self['integration_time'].unit

        duration = np.hstack([np.ones(num_sample) * int_time for num_sample, int_time in
                              self[['num_samples', 'integration_time']]])
        duration = duration.value * self['integration_time'].unit

        # TODO Write out and simplify
        end_delta = start_delta + duration

        # Add the delta time to base times and convert to relative from start time
        times = base_times + start_delta + (end_delta - start_delta) / 2
        # times -= times[0]
        return times, duration

    @classmethod
    def from_packets(cls, packets):

        control = cls()

        control.add_basic(name='tc_packet_id_ref', nix='NIX00001', packets=packets, dtype=np.uint16)
        control.add_basic(name='tc_packet_seq_control', nix='NIX00002', packets=packets,
                          dtype=np.uint16)
        control.add_basic(name='request_id', nix='NIX00037', packets=packets,
                          dtype=np.uint32)
        control.add_basic(name='time_stamp', nix='NIX00402', packets=packets)
        if np.any(control['time_stamp'] > 2 ** 32 - 1):
            coarse = control['time_stamp'] >> 16
            fine = control['time_stamp'] & (1 << 16) - 1
        else:
            coarse = control['time_stamp']
            fine = 0
        control['time_stamp'] = SCETime(coarse, fine)
        try:
            control['num_substructures'] = np.array(packets.get_value('NIX00403'),
                                                    np.uint16).reshape(1, -1)
            control.add_meta(name='num_substructures', nix='NIX00403', packets=packets)
        except AttributeError:
            logger.debug('NIX00403 not found')

        control = unique(control)

        return control


class Data(QTable, AddParametersMixin):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    @classmethod
    def from_packets(cls, packets):
        raise NotImplementedError


class GenericProduct(BaseProduct):
    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        """
        Generic product composed of control and data

        Parameters
        ----------
        control : stix_parser.products.quicklook.Control
            Table containing control information
        data : stix_parser.products.quicklook.Data
            Table containing data
        """

        self.__dict__.update(kwargs)
        self.control = control
        self.data = data
        self.idb_versions = idb_versions
        self.level = kwargs.get('level')
        self.service_subtype = service_subtype
        self.service_type = service_type
        self.ssid = ssid
        self.type = 'ql'

        self.comment = kwargs.get('comment', [])
        self.history = kwargs.get('history', [])

    @property
    def scet_timerange(self):
        return SCETimeRange(start=self.data['time'][0]-self.data['timedel'][0]/2,
                            end=self.data['time'][-1]+self.data['timedel'][-1]/2)

    @property
    def raw(self):
        return np.unique(self.control['raw_file']).tolist()

    @property
    def parent(self):
        return np.unique(self.control['parent']).tolist()

    @property
    def dmin(self):
        # default for FITS HEADER
        return 0.0

    @property
    def dmax(self):
        # default for FITS HEADER
        return 0.0

    @property
    def bunit(self):
        # default for FITS HEADER
        return ' '

    @property
    def exposure(self):
        # default for FITS HEADER
        return 0.0

    @property
    def max_exposure(self):
        # default for FITS HEADER
        return 0.0

    def find_parent_products(self, root):
        """
        Conveniant way to get access to the parent products.

        Performs a (inefficient) file search in the given root dir for files with the parent name.
        Not recursive the only the direct parent is returned.

        Parameters
        ----------
        root : `Path`
            The fits root dir in which to find the parent file

        Returns
        -------
        `Product`
            A list of parent Producs (normally just one).
        """
        return [Product(f) for f in self.find_parent_files(root)]

    def find_parent_files(self, root):
        """
        Conveniant way to get access to the parent files.

        Performs a (inefficient) file search in the given root dir for files with the parent name.
        Not recursive the only the direct parent is returned.

        Parameters
        ----------
        root : `Path`
            The fits root dir in which to find the parent file

        Returns
        -------
        `Path`
            A list of parent files pathes (normally just one).
        """
        if self.level == 'LB':
            return []
        p_level = 'LB'
        if self.level == 'L2':
            p_level = 'L1'
        elif self.level == 'L1':
            p_level = 'L0'

        level_dir = Path(root) / p_level

        files = []
        for pfile in self.parent:
            files.extend(list(level_dir.rglob(pfile)))
            # or the file is still labeled as incomplete see
            # https://github.com/i4Ds/STIXCore/issues/350
            files.extend(list(level_dir.rglob(get_incomplete_file_name(pfile))))
        return files

    def __add__(self, other):
        """
        Combine two products stacking data along columns and removing duplicated data using time as
        the primary key.

        Parameters
        ----------
        other : A subclass of stix_parser.products.quicklook.QLProduct

        Returns
        -------
        A subclass of stix_parser.products.quicklook.QLProduct
            The combined data product
        """
        if not isinstance(other, type(self)):
            raise TypeError(f'Products must of same type not {type(self)} and {type(other)}')

        # make a deep copy of the data and control
        other_control = other.control[:]
        other_data = other.data[:]
        self_control = self.control[:]
        self_data = self.data[:]

        # keep old index in a unique way
        other_control['old_index'] = [f"o{i}" for i in other_control['index']]
        self_control['old_index'] = [f"s{i}" for i in self_control['index']]
        other_data['old_index'] = [f"o{i}" for i in other_data['control_index']]
        self_data['old_index'] = [f"s{i}" for i in self_data['control_index']]

        if (self.service_type, self.service_subtype) == (3, 25):
            self_data['time'] = SCETime(self_control['scet_coarse'], self_control['scet_fine'])
            other_data['time'] = SCETime(other_control['scet_coarse'], other_control['scet_fine'])

        logger.debug('len self: %d, len other %d', len(self_data), len(other_data))
        control = vstack((self_control, other_control))
        data = vstack((self_data, other_data))
        logger.debug('len stacked %d', len(data))

        # Fits write we do np.around(time - start_time).as_float().to(u.cs)).astype("uint32"))
        # So need to do something similar here to avoid comparing un-rounded value to rounded values
        data['time_float'] = np.around((data['time'] - data['time'].min()).as_float().to('cs'))

        # remove duplicate data based on time bin and sort the data
        data = unique(data, keys=['time_float'])
        data.sort(['time_float'])

        logger.debug('len unique %d', len(data))

        data.remove_column('time_float')

        # update the control index in data to a new unique sequence
        newids = dict()

        for row in data:
            oid = row['old_index']

            if oid not in newids:
                newids[oid] = len(newids)
            nid = newids[oid]
            row['control_index'] = nid

        del_rows = list()

        # update the index in control as used in data
        for idx, row in enumerate(control):
            oid = row['old_index']

            if oid not in newids:
                del_rows.append(idx)
                continue
            nid = newids[oid]
            row['index'] = nid

        # when a old index from the control is not used any more in data it will be deleted
        control.remove_rows(del_rows)

        # sort the data by index (driven by the time in data)
        control.sort('index')

        del control['old_index']
        del data['old_index']

        # fake a deep clone of the timings
        idb_versions = defaultdict(SCETimeRange)
        for idb_key, date_range in self.idb_versions.items():
            idb_versions[idb_key] = SCETimeRange(start=SCETime(date_range.start.coarse,
                                                               date_range.start.fine),
                                                 end=SCETime(date_range.end.coarse,
                                                             date_range.end.fine))

        for idb_key, date_range in other.idb_versions.items():
            idb_versions[idb_key].expand(date_range)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data,
                          idb_versions=idb_versions, level=self.level)

    def __repr__(self):
        return f'<{self.__class__.__name__}' \
               f'{self.name}, {self.level}\n' \
               f'{self.scet_timerange.start} to {self.scet_timerange.end}, ' \
               f'{len(self.control)}, {len(self.data)}'

    def split_to_files(self):
        if self.level == 'L0':
            scedays = self.data["time"].get_scedays()
            days = np.unique(scedays)

            for day in days:
                ds = day
                de = day + 1
                i = np.where((scedays >= ds) & (scedays < de))

                if len(i[0]) > 0:
                    data = self.data[i]

                    control_indices = np.unique(data['control_index'])
                    control = self.control[np.isin(self.control['index'], control_indices)]
                    control_index_min = control_indices.min()

                    data['control_index'] = data['control_index'] - control_index_min
                    control['index'] = control['index'] - control_index_min
                    out = type(self)(service_type=self.service_type,
                                     service_subtype=self.service_subtype, ssid=self.ssid,
                                     control=control, data=data, idb_versions=self.idb_versions,
                                     level=self.level, comment=self.comment, history=self.history)

                    # TODO N.H. check if always right or better recreate
                    if hasattr(self, "fits_header") and self.fits_header is not None:
                        out.fits_header = self.fits_header

                    yield out
        else:  # L1+
            utc_timerange = self.scet_timerange.to_timerange()

            for day in utc_timerange.get_dates():
                ds = day
                de = day + 1 * u.day
                utc_times = self.data['time'].to_time()
                i = np.where((utc_times >= ds) & (utc_times < de))

                if len(i[0]) > 0:
                    data = self.data[i]

                    control_indices = np.unique(data['control_index'])
                    control = self.control[np.isin(self.control['index'], control_indices)]
                    control_index_min = control_indices.min()

                    data['control_index'] = data['control_index'] - control_index_min
                    control['index'] = control['index'] - control_index_min
                    out = type(self)(service_type=self.service_type,
                                     service_subtype=self.service_subtype, ssid=self.ssid,
                                     control=control, data=data, idb_versions=self.idb_versions,
                                     level=self.level, comment=self.comment, history=self.history)

                    # TODO N.H. check if always right or better recreate
                    if hasattr(self, "fits_header") and self.fits_header is not None:
                        out.fits_header = self.fits_header

                    yield out

    @classmethod
    def getLeveL0Packets(cls, levelb):
        packets = [Packet(d) for d in levelb.data['data']]
        # packets = []
        # for i, d in enumerate(levelb.data['data']):
        #    try:
        #        packets.append(Packet(d))
        #    except Exception:
        #        logger.warning(f"corrupt package {i}")
        #        pass

        idb_versions = defaultdict(SCETimeRange)

        for i, packet in enumerate(packets):
            decompression.decompress(packet)
            engineering.raw_to_engineering(packet)
            idb_versions[packet.get_idb().version].expand(packet.data_header.datetime)

        packets = PacketSequence(packets)

        return packets, idb_versions

    @classmethod
    def from_levelb(cls, levelb, *, parent=''):
        pass

    @property
    def fits_daily_file(self):
        return False


class CountDataMixin:
    @property
    def dmin(self):
        return self.data['counts'].min().value

    @property
    def dmax(self):
        return self.data['counts'].max().value

    @property
    def bunit(self):
        return 'counts'

    @property
    def exposure(self):
        return self.data['timedel'].as_float().min().to_value('s')

    @property
    def max_exposure(self):
        return self.data['timedel'].as_float().max().to_value('s')


class EnergyChannelsMixin:
    def get_energies(self):
        """Return energy channels for this TM data. Or default ones.

        Returns
        -------
        tuple
            Lower and high energy edges
        """
        try:
            date = SCETime.from_float(self.control['time_stamp'][0]).to_datetime()
        except KeyError:
            date = SCETime(self.control['scet_coarse'][0], 0).to_datetime()
        if 'energy_bin_edge_mask' in self.control.colnames:
            low, high = _get_energies_from_mask(date, self.control['energy_bin_edge_mask'][0])
        # elif 'energy_bin_mask' in self.control.colnames:
        #     energies = _get_energies_from_mask(self.control['energy_bin_mask'][0])
        else:
            low, high = _get_energies_from_mask(date)

        return low * u.keV, high * u.keV, range(len(low))


class FitsHeaderMixin:
    @property
    def fits_header(self):
        return self._fits_header if hasattr(self, '_fits_header') else None

    @fits_header.setter
    def fits_header(self, val):
        if not isinstance(val, fits.Header):
            raise ValueError("fits_header should be of type fits.Header")
        self._fits_header = val.copy(strip=True)

    def get_additional_header_keywords(self):
        return self._additional_header_keywords if hasattr(self, '_additional_header_keywords')\
            else None

    def add_additional_header_keyword(self, keyword):
        if not hasattr(self, '_additional_header_keywords'):
            setattr(self, '_additional_header_keywords', list())
        self._additional_header_keywords.append(keyword)

    def add_additional_header_keywords(self, keywords):
        if not hasattr(self, '_additional_header_keywords'):
            setattr(self, '_additional_header_keywords', list())
        for keyword in keywords:
            self._additional_header_keywords.append(keyword)


class L1Mixin(FitsHeaderMixin):
    @property
    def utc_timerange(self):
        return self.scet_timerange.to_timerange()

    @classmethod
    def from_level0(cls, l0product, parent=''):
        l1 = cls(service_type=l0product.service_type,
                 service_subtype=l0product.service_subtype,
                 ssid=l0product.ssid,
                 control=l0product.control,
                 data=l0product.data,
                 idb_versions=l0product.idb_versions,
                 comment=l0product.comment,
                 history=l0product.history)

        if l1.service_type == 21 and l1.service_subtype == 6 and l1.ssid in (20, 21, 22, 23, 24):
            # Time and count arrays were off by one so need to shift before FSW version
            # https://github.com/i4Ds/STIXCore/issues/286
            idbs = sorted([tuple(map(int, k.split('.'))) for k in l0product.idb_versions.keys()])
            if len(idbs) > 1:
                raise ValueError('More than one IDB found in BSD product')

            # Problem only occurred before 2.26.36 version of FSW problem was fixed in
            if idbs[0] < (2, 26, 36) and len(l1.data) > 1:
                # Check if request was at min configured time resolution
                if (l1.utc_timerange.start.datetime < MIN_INT_TIME_CHANGE
                    and l1.data['timedel'].as_float().min() == 1*u.s) or \
                        (l1.utc_timerange.start.datetime >= MIN_INT_TIME_CHANGE
                         and l1.data['timedel'].as_float().min() == 0.5*u.s):
                    l1.data['timedel'][1:-1] = l1.data['timedel'][:-2]
                    l1.data = l1.data[1:-1]
                    l1.history.append('Time and count arrays were shifted to fix offset')
                else:
                    l1.comment.append('Time and count arrays offset not fixed'
                                      ' as possibly summed on board')

        l1.control.replace_column('parent', [parent] * len(l1.control))
        l1.level = 'L1'
        engineering.raw_to_engineering_product(l1, IDBManager.instance)
        return l1


class L2Mixin(FitsHeaderMixin):
    @property
    def utc_timerange(self):
        return self.scet_timerange.to_timerange()

    @classmethod
    def get_additional_extensions(cls):
        """
        Get the additional extensions that should be added to the L2 fits file product.

        Returns
        -------
        list
            List of additional extensions to add to the L2 product.
        """
        return []

    @classmethod
    def from_level1(cls, l1product, parent='', idlprocessor=None):
        l2 = cls(service_type=l1product.service_type,
                 service_subtype=l1product.service_subtype,
                 ssid=l1product.ssid,
                 control=l1product.control,
                 data=l1product.data,
                 idb_versions=l1product.idb_versions)

        l2.control.replace_column('parent', [parent.name if isinstance(parent, Path) else parent]
                                  * len(l2.control))
        l2.level = 'L2'
        l2.fits_header = l1product.fits_header

        return [l2]


class L3Mixin(FitsHeaderMixin):
    pass


class DefaultProduct(GenericProduct, L1Mixin, L2Mixin):
    """
    Default product use when not QL or BSD.
    """
    service_name_map = {
        1: 'tc-verify',
        5: 'events',
        6: 'memory',
        9: 'time',
        17: 'conn-test',
        20: 'info-dist',
        22: 'context',
        236: 'config',
        237: 'params',
        238: 'archive',
        239: 'diagnostics',
        300: 'low-latency'
    }

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = f'{service_subtype}-{ssid}' if ssid else f'{service_subtype}'
        self.level = kwargs.get('level', 'L0')
        self.type = f'{self.service_name_map[service_type]}'

    @classmethod
    def from_levelb(cls, levelb, parent):
        packets, idb_versions = GenericProduct.getLeveL0Packets(levelb)

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = np.arange(len(control)).astype(get_min_uint(len(control)))

        control['raw_file'] = levelb.control['raw_file']
        control['packet'] = levelb.control['packet']
        control['parent'] = parent

        # Create array of times as dt from date_obs
        times = SCETime(control['scet_coarse'], control['scet_fine'])

        # Data
        data = Data()
        data['time'] = times
        data['timedel'] = SCETimeDelta(0, 0)

        reshape_nixs = {'NIX00103', 'NIX00104'}
        reshape = False
        if reshape_nixs.intersection(packets.data[0].__dict__.keys()):
            reshape = True
        for nix, param in packets.data[0].__dict__.items():

            name = param.idb_info.get_product_attribute_name()
            data.add_basic(name=name, nix=nix, attr='value', packets=packets, reshape=reshape)

        data['control_index'] = np.arange(len(control)).astype(get_min_uint(len(control)))

        return cls(service_type=packets.service_type,
                   service_subtype=packets.service_subtype,
                   ssid=packets.ssid,
                   control=control,
                   data=data,
                   idb_versions=idb_versions,
                   packets=packets)


Product = ProductFactory(registry=BaseProduct._registry, default_widget_type=DefaultProduct)

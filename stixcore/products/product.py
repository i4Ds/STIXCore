from pathlib import Path
from binascii import unhexlify
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
from stixcore.time import SCETime, SCETimeDelta, SCETimeRange
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import GenericPacket, PacketSequence

__all__ = ['BaseProduct', 'ProductFactory', 'Product', 'ControlSci', 'Control', 'Data']

from collections import defaultdict

from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class AddParametersMixin:
    def add_basic(self, *, name, nix, packets, attr=None, dtype=None, reshape=False):
        value = packets.get_value(nix, attr=attr)
        if reshape is True:
            value = value.reshape(1, -1)
        self[name] = value if dtype is None else value.astype(dtype)
        self.add_meta(name=name, nix=nix, packets=packets, add_curtx=(attr == "value"))

    def add_data(self, name, data_meta):
        data, meta = data_meta
        self[name] = data
        self[name].meta = meta

    def add_meta(self, *, name, nix, packets, add_curtx=False):
        param = packets.get(nix)
        idb_info = param[0].idb_info
        meta = {'NIXS': nix}
        if add_curtx:
            meta['PCF_CURTX'] = idb_info.PCF_CURTX
        self[name].meta = meta


class BaseProduct:
    """
    Base QLProduct that all other product inherit from contains the registry for the factory pattern
    """

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

    @classmethod
    def from_levelb(cls, levelb):

        packets = [Packet(unhexlify(d)) for d in levelb.data['data']]

        idb_versions = defaultdict(SCETimeRange)

        for i, packet in enumerate(packets):
            decompression.decompress(packet)
            engineering.raw_to_engineering(packet)
            idb_versions[packet.get_idb().version].expand(packet.data_header.datetime)

        packets = PacketSequence(packets)
        return packets, idb_versions


class ProductFactory(BasicRegistrationFactory):
    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            if isinstance(args[0], (str, Path)):
                file_path = Path(args[0])
                header = fits.getheader(file_path)
                service_type = int(header.get('stype'))
                service_subtype = int(header.get('sstype'))
                try:
                    ssid = int(header.get('ssid'))
                except ValueError:
                    ssid = None
                level = header.get('Level')
                header.get('TIMESYS')
                control = QTable.read(file_path, hdu='CONTROL')
                # Weird issue where time_stamp wasn't a proper table column?
                if 'time_stamp' in control.colnames:
                    ts = control['time_stamp'].value
                    control.remove_column('time_stamp')
                    control['time_stamp'] = ts * u.s
                data = QTable.read(file_path, hdu='DATA')

                scet_timerange = SCETimeRange(start=SCETime.from_string(header.get('OBT_BEG')),
                                              end=SCETime.from_string(header.get('OBT_END')))

                if level != 'LB':

                    data['timedel'] = SCETimeDelta(data['timedel'])
                    try:
                        offset = SCETime.from_string(header['OBT_BEG'], sep=':')
                    except ValueError:
                        offset = SCETime.from_string(header['OBT_BEG'], sep='f')
                    try:
                        control['time_stamp'] = SCETime.from_float(control['time_stamp'])
                    except KeyError:
                        pass

                    data['time'] = data['time'] + offset

                energies = None
                if level == 'L1':
                    try:
                        energies = QTable.read(file_path, hdu='ENERGIES')
                    except KeyError:
                        logger.warn(f"no ENERGIES data found in FITS: {file_path}")
                idb_versions = defaultdict(SCETimeRange)
                if level in ('L0', 'L1'):
                    try:
                        idbt = QTable.read(file_path, hdu='IDB_VERSIONS')
                        for row in idbt.iterrows():
                            idb_versions[row[0]] = SCETimeRange(start=SCETime.from_float(row[1]),
                                                                end=SCETime.from_float(row[2]))
                    except KeyError:
                        logger.warn(f"no IDB data found in FITS: {file_path}")

                Product = self._check_registered_widget(level=level, service_type=service_type,
                                                        service_subtype=service_subtype,
                                                        ssid=ssid, control=control,
                                                        data=data, energies=energies)

                return Product(level=level, service_type=service_type,
                               service_subtype=service_subtype,
                               ssid=ssid, control=control,
                               data=data, energies=energies, idb_versions=idb_versions,
                               scet_timerange=scet_timerange)

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
            control['integration_time'] = np.zeros_like(control['scet_coarse'], np.float) * u.s

        # control = unique(control)
        control['index'] = np.arange(len(control))

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

        control.add_basic(name='tc_packet_id_ref', nix='NIX00001', packets=packets, dtype=np.int32)
        control.add_basic(name='tc_packet_seq_control', nix='NIX00002', packets=packets,
                          dtype=np.int32)
        control.add_basic(name='request_id', nix='NIX00037', packets=packets,
                          dtype=np.int32)
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
                                                    np.int32).reshape(1, -1)
            control.add_meta(name='num_substructures', nix='NIX00403', packets=packets)
        except AttributeError:
            logger.debug('NIX00403 not found')

        return control


class Data(QTable, AddParametersMixin):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    @classmethod
    def from_packets(cls, packets):
        raise NotImplementedError


class GP(BaseProduct):
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
        self.service_type = service_type
        self.service_subtype = service_subtype
        self.ssid = ssid
        self.type = 'ql'
        self.control = control
        self.data = data
        self.idb_versions = idb_versions
        self.scet_timerange = kwargs.get('scet_timerange')

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

        # TODO reindex and update data control_index
        other_control = other.control[:]
        other_data = other.data[:]
        other_control['index'] = other.control['index'] + self.control['index'].max() + 1
        control = vstack((self.control, other_control))
        # control = unique(control, keys=['scet_coarse', 'scet_fine'])
        # control = control.group_by(['scet_coarse', 'scet_fine'])

        other_data['control_index'] = other.data['control_index'] + self.control['index'].max() + 1

        logger.debug('len self: %d, len other %d', len(self.data), len(other_data))

        data = vstack((self.data, other_data))

        logger.debug('len stacked %d', len(data))

        data['time_float'] = data['time'].as_float()

        data = unique(data, keys=['time_float'])

        logger.debug('len unique %d', len(data))

        data.remove_column('time_float')

        unique_control_inds = np.unique(data['control_index'])
        control = control[np.nonzero(control['index'][:, None] == unique_control_inds)[1]]

        for idb_key, date_range in other.idb_versions.items():
            self.idb_versions[idb_key].expand(date_range)

        scet_timerange = SCETimeRange(start=data['time'][0]-data['timedel'][0]/2,
                                      end=data['time'][-1]+data['timedel'][-1]/2)

        return type(self)(service_type=self.service_type, service_subtype=self.service_subtype,
                          ssid=self.ssid, control=control, data=data,
                          idb_versions=self.idb_versions, scet_timerange=scet_timerange)

    def __repr__(self):
        return f'<{self.__class__.__name__}, {self.scet_timerange.start} to ' \
               f'{self.scet_timerange.start}, {len(self.control)}, {len(self.data)}>'

    def to_days(self):
        start_day = int((self.scet_timerange.start.as_float() / u.d).decompose().value)
        end_day = int((self.scet_timerange.end.as_float() / u.d).decompose().value)
        if start_day == end_day:
            end_day += 1
        days = range(start_day, end_day)
        # days = set([(t.year, t.month, t.day) for t in self.data['time'].to_datetime()])
        # date_ranges = [(datetime(*day), datetime(*day) + timedelta(days=1)) for day in days]
        for day in days:
            ds = SCETime(((day * u.day).to_value('s')).astype(int), 0)
            de = SCETime((((day + 1) * u.day).to_value('s')).astype(int), 0)
            i = np.where((self.data['time'] >= ds) & (self.data['time'] < de))

            scet_timerange = SCETimeRange(start=ds, end=de)

            if i[0].size > 0:
                data = self.data[i]
                control_indices = np.unique(data['control_index'])
                control = self.control[np.isin(self.control['index'], control_indices)]
                control_index_min = control_indices.min()

                data['control_index'] = data['control_index'] - control_index_min
                control['index'] = control['index'] - control_index_min

                out = type(self)(service_type=self.service_type,
                                 service_subtype=self.service_subtype,
                                 ssid=self.ssid, control=control, data=data,
                                 idb_versions=self.idb_versions, scet_timerange=scet_timerange)
                out.level = self.level
                yield out


class GenericProduct(GP):
    """
    Mini house keeping reported during start up of the flight software.
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
        239: 'diagnostics'
    }

    def __init__(self, *, service_type, service_subtype, ssid, control, data,
                 idb_versions=defaultdict(SCETimeRange), **kwargs):
        super().__init__(service_type=service_type, service_subtype=service_subtype,
                         ssid=ssid, control=control, data=data, idb_versions=idb_versions, **kwargs)
        self.name = f'{service_subtype}-{ssid}' if ssid else f'{service_subtype}'
        self.level = 'L0'
        self.type = f'{service_type}'

    @property
    def utc_timerange(self):
        return self.scet_timerange.to_timerange()

    @classmethod
    def from_levelb(cls, levelb):
        packets, idb_versions = super().from_levelb(levelb)

        service_type = packets.get('service_type')[0]
        service_subtype = packets.get('service_subtype')[0]
        ssid = packets.get('pi1_val')[0]

        control = Control()
        control['scet_coarse'] = packets.get('scet_coarse')
        control['scet_fine'] = packets.get('scet_fine')
        control['integration_time'] = 0
        control['index'] = range(len(control))

        # Create array of times as dt from date_obs
        times = SCETime(control['scet_coarse'], control['scet_fine'])
        scet_timerange = SCETimeRange(start=times[0], end=times[-1])

        # Data
        data = Data()
        data['time'] = times
        data['timedel'] = SCETimeDelta(0, 0)

        reshape = True if {'NIX00103', 'NIX00104'}.intersection(
            packets.data[0].__dict__.keys()) else False
        for nix, param in packets.data[0].__dict__.items():

            name = param.idb_info.PCF_DESCR.upper().replace(' ', '_')
            data.add_basic(name=name, nix=nix, attr='value', packets=packets, reshape=reshape)

        data['control_index'] = range(len(control))

        return cls(service_type=service_type, service_subtype=service_subtype, ssid=ssid,
                   control=control, data=data, idb_versions=idb_versions,
                   scet_timerange=scet_timerange)

    @classmethod
    def from_level0(cls, l0product, idbm=GenericPacket.idb_manager):
        l1 = cls(service_type=l0product.service_type,
                 service_subtype=l0product.service_subtype,
                 ssid=l0product.ssid,
                 control=l0product.control,
                 data=l0product.data,
                 idb_versions=l0product.idb_versions,
                 scet_timerange=l0product.scet_timerange)

        engineering.raw_to_engineering_product(l1, idbm)
        l1.level = 'L1'
        return l1


Product = ProductFactory(registry=BaseProduct._registry, default_widget_type=GenericProduct)

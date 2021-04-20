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
from astropy.table.table import QTable
from astropy.time import Time

import stixcore.processing.decompression as decompression
import stixcore.processing.engineering as engineering
from stixcore.datetime.datetime import SCETime, SCETimeRange
from stixcore.tmtc.packet_factory import Packet
from stixcore.tmtc.packets import PacketSequence

__all__ = ['BaseProduct', 'ProductFactory', 'Product', 'ControlSci', 'Control', 'Data']

from collections import defaultdict

from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class AddParametersMixin:
    def add_basic(self, *, name, nix, packets, attr=None, dtype=None):
        value = packets.get_value(nix, attr=attr)
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
                ssid = int(header.get('ssid'))
                level = header.get('Level')
                timesys = header.get('TIMESYS')
                control = QTable.read(file_path, hdu='CONTROL')
                data = QTable.read(file_path, hdu='DATA')

                if level != 'LB':
                    if timesys == 'OBT':
                        obs_beg = SCETime.from_string(header['OBT_BEG'])
                        offset = obs_beg.as_float()
                    elif timesys == 'UTC':
                        offset = Time(header['OBT_BEG'])
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
                               data=data, energies=energies, idb_versions=idb_versions)

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


Product = ProductFactory(registry=BaseProduct._registry)


class Control(QTable, AddParametersMixin):

    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate integration time for each sample in each packet
        base_times = np.hstack(
            [np.full(ns, SCETime(coarse=ct, fine=ft).as_float().value)
             for ns, ct, ft in self[['num_samples', 'scet_coarse', 'scet_fine']]]) * u.s
        start_delta = np.hstack(
            [(np.arange(ns) * it) for ns, it in self[['num_samples', 'integration_time']]])

        durations = np.hstack([np.ones(num_sample) * int_time for num_sample, int_time in
                              self[['num_samples', 'integration_time']]])

        # Add the delta time to base times and convert to relative from start time
        times = base_times + start_delta + (durations / 2)

        return times, durations

    @classmethod
    def from_packets(cls, packets, NIX00405_offset=0):
        """
        Common generator method the create and prepare the control table.

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
        control['time_stamp'] = [SCETime(c, f) for c, f in zip(coarse, fine)]
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

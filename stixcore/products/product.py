from pathlib import Path
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

from stixcore.datetime.datetime import DateTime

__all__ = ['BaseProduct', 'ProductFactory', 'Product', 'ControlSci', 'Control']

from stixcore.products.common import _get_compression_scheme


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
                        obs_beg = DateTime.from_string(header['OBT_BEG'])
                        offset = obs_beg.as_float()
                    elif timesys == 'UTC':
                        offset = Time(header['OBT_BEG'])
                    data['time'] = data['time'] + offset

                energies = None
                if level == 'L1':
                    energies = QTable.read(file_path, hdu='ENERGIES')

                Product = self._check_registered_widget(level=level, service_type=service_type,
                                                        service_subtype=service_subtype,
                                                        ssid=ssid, control=control,
                                                        data=data, energies=energies)

                return Product(level=level, service_type=service_type,
                               service_subtype=service_subtype,
                               ssid=ssid, control=control,
                               data=data, energies=energies)

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


class Control(QTable):

    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate integration time for each sample in each packet
        base_times = np.hstack(
            [np.full(ns, DateTime(coarse=ct, fine=ft).as_float().value)
             for ns, ct, ft in self[['num_samples', 'scet_coarse', 'scet_fine']]]) * u.s
        start_delta = np.hstack(
            [(np.arange(ns) * it) for ns, it in self[['num_samples', 'integration_time']]])

        durations = np.hstack([np.ones(num_sample) * int_time for num_sample, int_time in
                              self[['num_samples', 'integration_time']]])

        # Add the delta time to base times and convert to relative from start time
        times = base_times + start_delta + (durations / 2)

        return times, durations

    @classmethod
    def from_packets(cls, packets):
        # Header
        control = cls()
        # self.energy_bin_mask = None
        # self.samples = None
        control['scet_coarse'] = np.array(packets.get_value('NIX00445'), np.uint32)
        control['scet_coarse'].meta = {'NIXS': 'NIX00445'}
        # Not all QL data have fine time in TM default to 0 if no present
        scet_fine = packets.get_value('NIX00446')
        if scet_fine:
            control['scet_fine'] = np.array(scet_fine, np.uint32)
        else:
            control['scet_fine'] = np.zeros_like(control['scet_coarse'], np.uint32)
        control['scet_fine'].meta = {'NIXS': 'NIX00446'}

        integration_time = packets.get_value('NIX00405')
        if integration_time:
            # TODO remove 0.1 after solved https://github.com/i4Ds/STIXCore/issues/59
            control['integration_time'] = (np.array(integration_time, np.float) + 0.1) * u.s
        else:
            control['integration_time'] = np.zeros_like(control['scet_coarse'], np.float) * u.s
        control['integration_time'].meta = {'NIXS': 'NIX00405'}

        # control = unique(control)
        control['index'] = np.arange(len(control))

        return control


class ControlSci(QTable):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    def _get_time(self):
        # Replicate packet time for each sample
        base_times = Time(list(chain(
            *[[DateTime(coarse=self["scet_coarse"][i], fine=self["scet_fine"][i])]
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

        # Control
        control['tc_packet_id_ref'] = np.array(packets.get_value('NIX00001'), np.int32)
        control['tc_packet_seq_control'] = np.array(packets.get_value('NIX00002'), np.int32)
        control['request_id'] = np.array(packets.get_value('NIX00037'), np.uint32)
        control['compression_scheme_counts_skm'] = _get_compression_scheme(packets, 'NIXD0007',
                                                                           'NIXD0008', 'NIXD0009')
        control['compression_scheme_triggers_skm'] = _get_compression_scheme(packets, 'NIXD0010',
                                                                             'NIXD0011', 'NIXD0012')
        control['time_stamp'] = np.array(packets.get_value('NIX00402'))

        # control['num_structures'] = np.array(packets.get('NIX00403'), np.int32)

        return control


class Data(QTable):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    @classmethod
    def from_packets(cls, packets):
        raise NotImplementedError

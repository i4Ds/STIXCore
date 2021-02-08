from pathlib import Path

import numpy as np
from sunpy.util.datatype_factory_base import (
    BasicRegistrationFactory,
    MultipleMatchError,
    NoMatchError,
)

import astropy.units as u
from astropy.io import fits
from astropy.table.table import QTable

from stixcore.datetime.datetime import DateTime


class BaseProduct:
    """
    Base Product that all other product inherit from contains the registry for the factory pattern
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

    def __init__(self, *args, **kwargs):
        print('adfa')


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
                        raise NotImplementedError()
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
        control['scet_coarse'] = np.array(packets.get('NIX00445'), np.uint32)
        control['scet_coarse'].meta = {'NIXS': 'NIX00445'}
        # Not all QL data have fine time in TM default to 0 if no present
        scet_fine = packets.get('NIX00446')
        if scet_fine:
            control['scet_fine'] = np.array(scet_fine, np.uint32)
        else:
            control['scet_fine'] = np.zeros_like(control['scet_coarse'], np.uint32)
        control['scet_fine'].meta = {'NIXS': 'NIX00446'}

        integration_time = packets.get('NIX00405')
        if integration_time:
            control['integration_time'] = (np.array(integration_time, np.float) + 1) * 0.1 * u.s
        else:
            control['integration_time'] = np.zeros_like(control['scet_coarse'], np.float) * u.s
        control['integration_time'].meta = {'NIXS': 'NIX00405'}

        # control = unique(control)
        control['index'] = np.arange(len(control))

        return control


class Data(QTable):
    def __repr__(self):
        return f'<{self.__class__.__name__} \n {super().__repr__()}>'

    @classmethod
    def from_packets(cls, packets):
        raise NotImplementedError

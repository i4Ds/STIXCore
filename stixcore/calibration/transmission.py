from pathlib import Path
from functools import partial
from collections import OrderedDict

import numpy as np
from roentgen.absorption.material import Compound, MassAttenuationCoefficient, Material

import astropy.units as u
from astropy.table.table import Table

from stixcore.config.reader import read_energy_channels

__all__ = ['Transmission']

MIL_SI = 0.0254 * u.mm

# TODO move to configuration files
COMPONENTS = OrderedDict([
    ('front_window', [('solarblack', 0.005 * u.mm), ('be', 2*u.mm)]),
    ('rear_window', [('be', 1*u.mm)]),
    ('grid_covers', [('kapton', 4 * 2 * MIL_SI)]),
    ('dem', [('kapton', 2 * 3 * MIL_SI)]),
    ('attenuator', [('al', 0.6 * u.mm)]),
    ('mli', [('al', 1000 * u.angstrom), ('kapton', 3 * MIL_SI), ('al', 40 * 1000 * u.angstrom),
             ('mylar', 20 * 0.25 * MIL_SI), ('pet', 21 * 0.005 * u.mm), ('kapton', 3 * MIL_SI),
             ('al', 1000 * u.angstrom)]),
    ('calibration_foil', [('al', 4 * 1000 * u.angstrom), ('kapton', 4 * 2 * MIL_SI)]),
    ('dead_layer', [('te_o2', 392 * u.nm)]),
])

MATERIALS = OrderedDict([
    ('al', ({'Al': 1.0}, 2.7 * u.g/u.cm**3)),
    ('be', ({'Be': 1.0}, 1.85 * u.g/u.cm**3)),
    ('kapton', ({'H': 0.026362, 'C': 0.691133, 'N': 0.073270, 'O': 0.209235},
                1.43 * u.g / u.cm ** 3)),
    ('mylar', ({'H': 0.041959, 'C': 0.625017, 'O': 0.333025}, 1.38 * u.g / u.cm ** 3)),
    ('pet', ({'H': 0.041960, 'C': 0.625016, 'O': 0.333024}, 1.370 * u.g / u.cm**3)),
    ('solarblack_oxygen', ({'H': 0.002, 'O': 0.415, 'Ca': 0.396, 'P': 0.187}, 3.2 * u.g/u.cm**3)),
    ('solarblack_carbon', ({'C': 0.301, 'Ca': 0.503, 'P': 0.195}, 3.2 * u.g/u.cm**3)),
    ('te_o2', ({'Te': 0.7995088158691722, 'O': 0.20049124678825841}, 5.670 * u.g/u.cm**3))]
)

# TODO get file from config
ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")


class Transmission:
    """
    Calculate the energy dependant transmission of X-ray through the instrument
    """
    def __init__(self, solarblack='solarblack_carbon'):
        """
        Create a new instance of the transmission with the given solar black composition.

        Parameters
        ----------

        solarblack : `str` optional
            The SolarBlack composition to use.
        """
        if solarblack not in ['solarblack_oxygen', 'solarblack_carbon']:
            raise ValueError("solarblack must be either 'solarblack_oxygen' or "
                             "'solarblack_carbon'.")

        self.solarblack = solarblack
        self.materials = MATERIALS
        self.components = COMPONENTS
        self.components = dict()
        self.energies = [ENERGY_CHANNELS[i].e_lower for i in range(1, 32)] * u.keV

        for name, layers in COMPONENTS.items():
            parts = []
            for material, thickness in layers:
                if material == 'solarblack':
                    material = self.solarblack
                mass_frac, den = MATERIALS[material]
                if material == 'al':
                    thickness = thickness * 0.8
                parts.append(self.create_material(name=material, fractional_masses=mass_frac,
                                                  thickness=thickness, density=den))
            self.components[name] = Compound(parts)

    def get_transmission(self, energies=None, attenuator=False):
        """
        Get the transmission for each detector at the center of the given energy bins.

        If energies are not supplied will evaluate at standard science energy channels

        Parameters
        ----------
        energies : `astropy.units.Quantity`, optional
            The energies to evaluate the transmission
        attenuator : `bool`, optional
            True for attenuator in X-ray path, False for attenuator not in X-ray path

        Returns
        -------
        `astropy.table.Table`
            Table containing the transmission values for each energy and detector
        """
        base_comps = [self.components[name] for name in ['front_window', 'rear_window', 'dem',
                                                         'mli', 'calibration_foil',
                                                         'dead_layer']]

        if energies is None:
            energies = self.energies

        if attenuator:
            base_comps.append(self.components['attenuator'])

        base = Compound(base_comps)
        base_trans = base.transmission(energies[:-1] + 0.5 * np.diff(energies))

        fine = Compound(base_comps + [self.components['grid_covers']])
        fine_trans = fine.transmission(energies[:-1] + 0.5 * np.diff(energies))

        # TODO need to move to configuration db
        fine_grids = np.array([11, 13, 18, 12, 19, 17]) - 1
        transmission = Table()
        # transmission['sci_channel'] = range(1, 31)
        for i in range(33):
            name = f'det-{i}'
            if np.isin(i, fine_grids):
                transmission[name] = fine_trans
            else:
                transmission[name] = base_trans
        return transmission

    def get_transmission_by_component(self):
        """
        Get the contributions to the total transmission by broken down by component.

        Returns
        -------
        `dict`
            Entries are Compounds for each component
        """
        return self.components

    def get_transmission_by_material(self):
        """
        Get the contribution to the transmission by total thickness for each material.

        Layers of the same materials are combined to return one instance with the total thickness.

        Returns
        -------
        `dict`
            Entries are meterials with the total thickness for that material.
        """
        material_thickness = dict()
        for name, layers in COMPONENTS.items():
            for material_name, thickness in layers:
                if material_name == 'solarblack':
                    material_name = self.solarblack
                if material_name in material_thickness.keys():
                    material_thickness[material_name] += thickness.to('mm')
                else:
                    material_thickness[material_name] = thickness.to('mm')
        res = {}
        for name, thickness in material_thickness.items():
            frac_mass, density = self.materials[name]
            mat = self.create_material(name=name, fractional_masses=frac_mass,
                                       density=density, thickness=thickness)
            res[name] = mat

        return res

    @classmethod
    def create_material(cls, name=None, fractional_masses=None, thickness=None, density=None):
        """
        Create a new material given the composition and fractional masses.

        Parameters
        ----------
        name : `str`
            Name of the meterial
        fractional_masses : `dict`
            The element and fractional masses of the material e.g. `{'H': 0.031, 'O': 0.969}`
        thickness : `astropy.units.Quantity`
            Thickness of the material
        density : `astropy.units.Quantity`
            Density of the material

        Returns
        -------
        `roentgen.absorption.material.Material`
            The material
        """
        material = Material('h', thickness, density)
        material.name = name
        # probably don't need this
        material.density = density

        # TODO remove in favour of upstream fix when completed
        #  see https://github.com/ehsteve/roentgen/issues/26
        def func(composition, e):
            return sum([MassAttenuationCoefficient(element).func(e) * frac_mass
                        for element, frac_mass in composition.items()])

        material_func = partial(func, fractional_masses)

        material.mass_attenuation_coefficient.func = material_func
        return material

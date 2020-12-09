from functools import partial
from collections import OrderedDict

import numpy as np
from roentgen.absorption.material import Compound, MassAttenuationCoefficient, Material

import astropy.units as u
from astropy.table.table import Table

# from stix_parser.products.quicklook import ENERGY_CHANNELS

__all__ = ['Transmission']

MIL_SI = 0.0254 * u.mm

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

# TODO move to somewhere better and create from txt file look at IDL implementation
ENERGY_CHANNELS = {
    0: {'channel_edge': 0, 'energy_edge': 0, 'e_lower': 0.0, 'e_upper': 4.0, 'bin_width': 4.0,
        'dE_E': 2.000, 'ql_channel': None},
    1: {'channel_edge': 1, 'energy_edge': 4, 'e_lower': 4.0, 'e_upper': 5.0, 'bin_width': 1.0,
        'dE_E': 0.222, 'ql_channel': 0},
    2: {'channel_edge': 2, 'energy_edge': 5, 'e_lower': 5.0, 'e_upper': 6.0, 'bin_width': 1.0,
        'dE_E': 0.182, 'ql_channel': 0},
    3: {'channel_edge': 3, 'energy_edge': 6, 'e_lower': 6.0, 'e_upper': 7.0, 'bin_width': 1.0,
        'dE_E': 0.154, 'ql_channel': 0},
    4: {'channel_edge': 4, 'energy_edge': 7, 'e_lower': 7.0, 'e_upper': 8.0, 'bin_width': 1.0,
        'dE_E': 0.133, 'ql_channel': 0},
    5: {'channel_edge': 5, 'energy_edge': 8, 'e_lower': 8.0, 'e_upper': 9.0, 'bin_width': 1.0,
        'dE_E': 0.118, 'ql_channel': 0},
    6: {'channel_edge': 6, 'energy_edge': 9, 'e_lower': 9.0, 'e_upper': 10.0, 'bin_width': 1.0,
        'dE_E': 0.105, 'ql_channel': 0},
    7: {'channel_edge': 7, 'energy_edge': 10, 'e_lower': 10.0, 'e_upper': 11.0, 'bin_width': 1.0,
        'dE_E': 0.095, 'ql_channel': 1},
    8: {'channel_edge': 8, 'energy_edge': 11, 'e_lower': 11.0, 'e_upper': 12.0, 'bin_width': 1.0,
        'dE_E': 0.087, 'ql_channel': 1},
    9: {'channel_edge': 9, 'energy_edge': 12, 'e_lower': 12.0, 'e_upper': 13.0, 'bin_width': 1.0,
        'dE_E': 0.080, 'ql_channel': 1},
    10: {'channel_edge': 10, 'energy_edge': 13, 'e_lower': 13.0, 'e_upper': 14.0, 'bin_width': 1.0,
         'dE_E': 0.074, 'ql_channel': 1},
    11: {'channel_edge': 11, 'energy_edge': 14, 'e_lower': 14.0, 'e_upper': 15.0, 'bin_width': 1.0,
         'dE_E': 0.069, 'ql_channel': 1},
    12: {'channel_edge': 12, 'energy_edge': 15, 'e_lower': 15.0, 'e_upper': 16.0, 'bin_width': 1.0,
         'dE_E': 0.065, 'ql_channel': 2},
    13: {'channel_edge': 13, 'energy_edge': 16, 'e_lower': 16.0, 'e_upper': 18.0, 'bin_width': 1.0,
         'dE_E': 0.061, 'ql_channel': 2},
    14: {'channel_edge': 14, 'energy_edge': 18, 'e_lower': 18.0, 'e_upper': 20.0, 'bin_width': 2.0,
         'dE_E': 0.105, 'ql_channel': 2},
    15: {'channel_edge': 15, 'energy_edge': 20, 'e_lower': 20.0, 'e_upper': 22.0, 'bin_width': 2.0,
         'dE_E': 0.095, 'ql_channel': 2},
    16: {'channel_edge': 16, 'energy_edge': 22, 'e_lower': 22.0, 'e_upper': 25.0, 'bin_width': 3.0,
         'dE_E': 0.128, 'ql_channel': 2},
    17: {'channel_edge': 17, 'energy_edge': 25, 'e_lower': 25.0, 'e_upper': 28.0, 'bin_width': 3.0,
         'dE_E': 0.113, 'ql_channel': 3},
    18: {'channel_edge': 18, 'energy_edge': 28, 'e_lower': 28.0, 'e_upper': 32.0, 'bin_width': 4.0,
         'dE_E': 0.133, 'ql_channel': 3},
    19: {'channel_edge': 19, 'energy_edge': 32, 'e_lower': 32.0, 'e_upper': 36.0, 'bin_width': 4.0,
         'dE_E': 0.118, 'ql_channel': 3},
    20: {'channel_edge': 20, 'energy_edge': 36, 'e_lower': 36.0, 'e_upper': 40.0, 'bin_width': 4.0,
         'dE_E': 0.105, 'ql_channel': 3},
    21: {'channel_edge': 21, 'energy_edge': 40, 'e_lower': 40.0, 'e_upper': 45.0, 'bin_width': 5.0,
         'dE_E': 0.118, 'ql_channel': 3},
    22: {'channel_edge': 22, 'energy_edge': 45, 'e_lower': 45.0, 'e_upper': 50.0, 'bin_width': 5.0,
         'dE_E': 0.105, 'ql_channel': 3},
    23: {'channel_edge': 23, 'energy_edge': 50, 'e_lower': 50.0, 'e_upper': 56.0, 'bin_width': 6.0,
         'dE_E': 0.113, 'ql_channel': 4},
    24: {'channel_edge': 24, 'energy_edge': 56, 'e_lower': 56.0, 'e_upper': 63.0, 'bin_width': 7.0,
         'dE_E': 0.118, 'ql_channel': 4},
    25: {'channel_edge': 25, 'energy_edge': 63, 'e_lower': 63.0, 'e_upper': 70.0, 'bin_width': 7.0,
         'dE_E': 0.105, 'ql_channel': 4},
    26: {'channel_edge': 26, 'energy_edge': 70, 'e_lower': 70.0, 'e_upper': 76.0, 'bin_width': 6.0,
         'dE_E': 0.082, 'ql_channel': 4},
    27: {'channel_edge': 27, 'energy_edge': 76, 'e_lower': 76.0, 'e_upper': 84.0, 'bin_width': 8.0,
         'dE_E': 0.100, 'ql_channel': 4},
    28: {'channel_edge': 28, 'energy_edge': 84, 'e_lower': 84.0, 'e_upper': 100.0,
         'bin_width': 16.0, 'dE_El': 0.174, 'ql_channel': 4},
    29: {'channel_edge': 29, 'energy_edge': 100, 'e_lower': 100.0, 'e_upper': 120.0,
         'bin_width': 20.0, 'dE_El': 0.182, 'ql_channel': 4},
    30: {'channel_edge': 30, 'energy_edge': 120, 'e_lower': 120.0, 'e_upper': 150.0,
         'bin_width': 30.0, 'dE_El': 0.222, 'ql_channel': 4},
    31: {'channel_edge': 31, 'energy_edge': 150, 'e_lower': 150.0, 'e_upper': np.inf,
         'bin_width': np.inf, 'dE_E': np.inf, 'ql_channel': None}
}


class Transmission:
    """
    Transmission of X-rays through the various components (excluding grids)
    """
    def __init__(self, solarblack='solarblack_carbon'):
        """

        Parameters
        ----------
        solarblack : `str` optional
            The SolarBlack composition to use
        """
        if solarblack not in ['solarblack_oxygen', 'solarblack_carbon']:
            raise ValueError('solarblack must be either solarblack_oxygen or solarblack_carbon.')

        self.solarblack = solarblack
        self.materials = MATERIALS
        self.components_ = COMPONENTS
        self.components = dict()
        self.energies = [ENERGY_CHANNELS[i]['e_lower'] for i in range(1, 32)] * u.keV

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
        Get the transmission for each detector and science energy channel.

        The fine grids have additional layers of kapton on both sides

        Parameters
        ----------
        energies : `astropy.uints.Quantity`, optional
            The energies to evalute the transmission
        attenuator : `bool`, optional
            True for attenutator in X-ray path, False for attenuator not in X-ray path

        Returns
        -------
        `astropy.table.Table`
            Table containing the tranmssion values for each energy and detector
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
        Get the contributions to the total transmission by component.

        Returns
        -------
        `dict`
            Entries are Compounds for each component
        """
        return self.components

    def get_transmission_by_material(self):
        """
        Get the contribution to the transmission by material.

        Layers of the same materials are combined to return one instance with the total thickness.

        Returns
        -------
        `dict`
            Entries are meterials wtih the total thickness for that material.
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
        Create a new material given it composition and fractional masses

        Parameters
        ----------
        name : `str`
            Name of the meterial
        fractional_masses : `dict`
            The elements and fraciona masses makinng up the material e.g `{'H': 'O': }`
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
        # probbably don't need this
        material.density = density

        def func(fractional_masses, e):
            return sum([MassAttenuationCoefficient(element).func(e) * frac_mass
                        for element, frac_mass in fractional_masses.items()])

        material_func = partial(func, fractional_masses)

        material.mass_attenuation_coefficient.func = material_func
        return material

    def _bin_average(self, obj, n_points=1001):
        out = []
        for i in range(self.energies.size-1):
            ee = np.linspace(self.energies[i], self.energies[i+1], n_points)
            vals = obj.transmission(ee)
            avg = np.average(vals)
            out.append(avg)
        return np.hstack(out)

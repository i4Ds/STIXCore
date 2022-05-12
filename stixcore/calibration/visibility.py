from types import SimpleNamespace

import numpy as np

import astropy.units as u
from astropy.table import Table


def calibrate_visibility(components, components_error, flare_location=(0, 0) * u.arcsec):
    """
    Return a phase calibrated visibility.

    Applies a number of correction and

    Parameters
    ----------
    components

    components_error

    Returns
    -------

    """
    real_component, imag_component = components

    real_component_err, imag_component_err = components_error

    # Compute raw phases
    phase = np.arctan2(imag_component, real_component)

    # Compute amplitudes
    modulation_efficiency = np.pi ** 3 / (8 * np.sqrt(2))  # Modulation efficiency
    observed_amplitude = np.sqrt(real_component ** 2 + imag_component ** 2)
    calibrated_amplitude = observed_amplitude * modulation_efficiency

    # Compute error on amplitudes
    systematic_error = 5 * u.percent  # from G. Hurford 5% systematics
    amplitude_stat_error = (np.sqrt((real_component / observed_amplitude
                            * real_component_err) ** 2 + (imag_component /
                            observed_amplitude * imag_component_err) ** 2)) * modulation_efficiency
    amplitude_error = np.sqrt(amplitude_stat_error ** 2 +
                              (systematic_error * calibrated_amplitude)
                              .to(observed_amplitude.unit) ** 2)

    # Apply pixel correction
    phase += 46.1 * u.deg  # Center of large pixel in
    # TODO add correction for small pixel

    # Grid correction factors
    phase_cal = Table.read('/usr/local/ssw/so/stix/dbase/demo/vis_demo/GridCorrection.csv',
                           header_start=2, data_start=3)
    grid_corr = phase_cal['Phase correction factor'] * u.deg

    # Apply grid correction
    phase += grid_corr

    # Phase correction
    phase_cal = Table.read('/usr/local/ssw/so/stix/dbase/demo/vis_demo/PhaseCorrFactors.csv',
                           header_start=3, data_start=4)
    phase_corr = phase_cal['Phase correction factor'] * u.deg

    # Apply grid correction
    phase += phase_corr

    # Compute error on phases
    phase_error = amplitude_error / calibrated_amplitude

    vis = SimpleNamespace(amplitude=observed_amplitude,
                          amplitude_error=amplitude_error,
                          phase=phase, phase_error=phase_error)

    return vis


def sas_map_cenert():
    # receter map at 0,0 taking account of mean or actual sas sol
    pass

# Phase project factor
# L1 550
# L2 47
# phase -= xy_flare  8.8 (deg)

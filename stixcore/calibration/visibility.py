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
    real_component = 0
    imag_component = 0

    real_component_err = 0
    imag_component_err = 0

    # Compute raw phases
    phase = np.atan(imag_component, real_component)

    # Compute amplitudes
    modulation_efficiency = np.pi**3/(8* np.sqrt(2.))  #Modulation efficiency
    observed_amplitude = np.sqrt(real_component**2 + imag_component**2)
    calibrated_amplitude = observed_amplitude * modulation_efficiency

    # Compute error on amplitudes
    systematic_error = 0.05  # from G Hurford 5 % systematics
    amplitude_stat_error = np.sqrt((real_component / observed_amplitude * real_component_err)**2 +
        (imag_component / observed_amplitude * imag_component_err)**2) * modulation_efficiency
    amplitude_error = np.sqrt(amplitude_stat_error**2 + systematic_error**2 * observed_amplitude**2)

    # Apply pixel correction
    phase += 46.1*u.deg # Center of large pixel in
    # TODO add correction for small pixel

    # Grid correction factors
    phase_cal = Table.read('/usr/local/ssw/so/stix/dbase/demo/vis_demo/GridCorrection.csv')
    gcorr = phase_cal.field2

    # Apply grid correction
    phase += gcorr

    # Phase correction
    phase_cal = Table.read('/usr/local/ssw/so/stix/dbase/demo/vis_demo/PhaseCorrFactors.csv')
    phase_corr = phase_cal.field2

    # Apply grid correction
    phase += phase_corr

    #Compute error on phases
    amplitude_error / calibrated_amplitude


def sas_map_cenert():
    # receter map at 0,0 taking account of mean or actual sas sol
    pass

# Phase project factor
# L1 550
# L2 47
# phase -= xy_flare  8.8 (deg)

"""
    Author: O. Limousin, CEA
    Date: Oct 23, 2024
    This script to:

    #%%%%%%%%%%%%%% FOR fit of ECC_ULTRA_FINE %%%%%%%%%%%%%%
    ../00_CALIBRATION_MONITORING_ULTRA_FINE/02_MONITOR_ECC_SPECTRA ==> XX_erg.fits

    ../00_CALIBRATION_MONITORING_ULTRA_FINE/01_MONITOR_ECC_PARA/ ==> ECC_para_XXXX.fits

        + read the log (.xlsx file)
        + open the ECC calibrated spectra XX_erg.fits
        + fit 31 and 81 keV lines (Fit_Ba_robust) (fit right hand side, and baseline)
        + register the results in a Pandas Dataframe
            + date, Run Number, Fit Goodness flags,DET, PIX,
                P31, err_P31, dE31, err_dE31, H31
                P81, err_P81, dE81, err_dE81, H81
        + compute gain/offset corrections
        + fill the dataframe with new values
            + include errors
            + include a Flag (Flag31 and Flag81) which is True if fit was OK
                if was not OK, the ECC value in NOT corrected
        + store the results in pkl
        + generate update ECC_para files to recalibrate uncalibrated files

"""

import numpy as np
import pandas as pd
from lmfit import Model

from astropy.io import fits

from stixcore.util.logging import get_logger

logger = get_logger(__name__)


def open_fits_tables(fits_path):
    # Get the data from .fits
    data = []
    with fits.open(fits_path, memmap=False) as hdul:
        header = [hdul[0].header]
        for i in range(1, len(hdul)):
            header.append(hdul[i].header)
            data.append(hdul[i].data)
        hdul.flush()

    # Get the fields of each data table and sort the data in lists
    data_names = [data_i.columns.names for data_i in data]
    data_format = [data_i.columns.formats for data_i in data]
    data_unit = [data_i.columns.units for data_i in data]
    data_list = [[list(data[i][data_names[i][j]]) for j in range(len(data_names[i]))] for i in
                 range(len(data))]
    return header, data, data_list, data_names, data_format, data_unit


def Read_fits_STIX_One_Pixel(data, PIX=0, DETECTOR_ID=1, Nbin=2024, NRebin=1, NSigma=1):
    data = data[0]
    Pix = PIX
    Pix = Pix + (DETECTOR_ID-1)*12
    obs = [0] * Nbin
    erg_c = data.ERG_center  # data.field(0)
    obs = data.field(3+Pix)
    nbin = Nbin
    Nbin = [Nbin/NRebin]
    # TODO: check if we really need to rebin with congrid here
    # erg_c = congrid(erg_c[:nbin], Nbin)
    # obs = congrid(obs[:nbin], Nbin)
    erg_c = erg_c[:nbin]
    obs = obs[:nbin]

    yerr = NSigma*np.sqrt(obs)
    return erg_c, obs, yerr


def line(x, slope, intercept):
    """a line"""
    return (slope*x + intercept)


def poly(x, degree, slope, intercept):
    """a line"""
    return (degree*x*x + slope*x + intercept)


def gaussian(x, amp, cen, wid):
    #    """1-d gaussian: gaussian(x, amp, cen, wid)"""
    return (amp*np.exp(-(x-cen)**2 / (2*wid**2)))


def Fit_Ba_Lines_Robust(erg_c, obs, PLOT_VERBOSE=1, LogScale=1):
    """
    OL, oct 22, 2024
    Robust fit procedure to adjust peak position post ECC
    The idea is the exact same as previous function but to return
        + 0's in case the fit fails
        + flag to say if the fit was successful or not
    """
    # Select Energy range for 81 keV Ba-133 line
    pipo = ((erg_c > 80.5) & (erg_c < 90.)).nonzero()
    y = (obs[pipo])
    x = (erg_c[pipo])
    x = np.array(x, dtype='float64')
    y = np.array(y, dtype='float64')
    mod = Model(gaussian) + Model(line)
    pars = mod.make_params(amp=10, cen=81, wid=0.5, slope=0, intercept=0)
    result = mod.fit(y, pars, x=x)

    if ((result.params['wid'].stderr is not None) &
            (result.params['cen'].stderr is not None) &
            (np.abs(result.best_values['cen'] - 81.) < 1.)):
        dE81 = result.best_values['wid']*2.35
        err_dE81 = result.params['wid'].stderr*2.35
        P81 = result.best_values['cen']
        err_P81 = result.params['cen'].stderr
        H81 = result.best_values['amp']
        Goodness_Flag_81 = True
    else:
        dE81, err_dE81, P81, err_P81, H81 = 0, 0, 0, 0, 0
        Goodness_Flag_81 = False

    # Select Energy range for 81 keV Ba-133 line
    # THE FOLLWING SECTION IS QUIET ROBUST BUT MIGHT OVERESTIMATE THE ENERGY RESOLUTION AT 32 keV
    # THIS ALLOWS TO FORCE THE BASE LINE TO ADJSUT in 40-45 keV range while a simple Gaussian
    # is used to fit the right hand side of the 32 keV Line
    pipo = (((erg_c > 30.2) & (erg_c < 33.0)) | ((erg_c > 40) & (erg_c < 45))).nonzero()
    y = obs[pipo]
    x = erg_c[pipo]
    x = np.array(x, dtype='float64')
    y = np.array(y, dtype='float64')

    mod = Model(gaussian, prefix='g1_') + Model(poly)
    pars = mod.make_params(g1_amp=10, g1_cen=30.6, g1_wid=0.4, degree=0., slope=0, intercept=0.)

    result = mod.fit(y, pars, x=x)

    if ((result.params['g1_wid'].stderr is not None) &
            (result.params['g1_cen'].stderr is not None) &
            ((np.abs(result.best_values['g1_cen'] - 31.) < 1.) & (Goodness_Flag_81))):
        dE31 = result.best_values['g1_wid']*2.35
        err_dE31 = result.params['g1_wid'].stderr*2.35
        P31 = result.best_values['g1_cen']
        err_P31 = result.params['g1_cen'].stderr
        H31 = result.best_values['g1_amp']
        Goodness_Flag_31 = True
    else:
        dE31, err_dE31, P31, err_P31, H31 = 0, 0, 0, 0, 0
        Goodness_Flag_31 = False

    return P31, P81, dE31, dE81, err_P31, err_P81, err_dE31, err_dE81, \
        H31, H81, Goodness_Flag_31, Goodness_Flag_81


def ecc_post_fit(erg_file, para_file, livetime):

    DETs = np.arange(32)+1
    LARGEs = [0, 1, 2, 3, 4, 5, 6, 7]
    SMALLs = [8, 9, 10, 11]
    PIXELs = LARGEs+SMALLs

    # Prep Pandas DataFrame
    df = pd.DataFrame()  # dict will be define later during concactenation with appropriated keys
    accumulator = []

    # Proceed to fit individually each pixel spectrum in the list of files
    data = open_fits_tables(erg_file)[1]  # only need data
    for DET in DETs:
        for PIX in PIXELs:  # or in  LARGEs, SMALLs
            erg_c, obs, yerr = Read_fits_STIX_One_Pixel(data, PIX=PIX, DETECTOR_ID=DET,
                                                        Nbin=2024, NRebin=1, NSigma=1)

            P31, P81, dE31, dE81, err_P31, err_P81, err_dE31, err_dE81, H31, H81, Goodness_Flag31, \
                Goodness_Flag81 = Fit_Ba_Lines_Robust(erg_c, obs/livetime,
                                                      PLOT_VERBOSE=0, LogScale=0)

            dict = {'DET':      [DET],
                    'PIX':      [PIX],
                    'P31':      [P31],
                    'err_P31':  [err_P31],
                    'dE31':     [dE31],
                    'err_dE31': [err_dE31],
                    'Flag31':   [Goodness_Flag31],
                    'P81':      [P81],
                    'err_P81':  [err_P81],
                    'dE81':     [dE81],
                    'err_dE81': [err_dE81],
                    'Flag81':   [Goodness_Flag81]}
            logger.debug(dict)

            # NB: this is faster to append a list of dict and create DataFrame at the end
            # than concatening the DataFrame row by row, this slows down dramatically progressively
            accumulator.append(pd.DataFrame(dict))

    df = pd.concat(accumulator)
    df = df.reset_index(drop=True)

    # 3- gain and offset correction factors of ECC pre-calibrated data
    G_prime = (df['P81']-df['P31'])/(80.9979-(30.6254*33.8 + 30.9731 * 62.4)/(62.4+33.8))
    O_prime = df['P31'] - G_prime*(30.6254*33.8 + 30.9731 * 62.4)/(62.4+33.8)

    # 4- add correction factors to the DataFrame
    df['Gain_Prime'] = G_prime
    df['Offset_Prime'] = O_prime

    # check, Run number and pixel number prior to assign Gain and Offset for further correction
    data_para = open_fits_tables(para_file)[1]  # only need data

    df['Gain_ECC'] = np.float32(data_para[0].gain)
    df['Offset_ECC'] = np.float32(data_para[0].off)
    df['goc'] = np.float32(data_para[0].goc)

    # 7 - Compute corrected Gain and Offset and fill df
    # 7.2 - Now assign the corrected Gain and Offset values
    # except when ECC works better
    df['Gain_Cor'] = 0.0
    df['Offset_Cor'] = 0.0

    # apply correction to gain and offset when fit is ok
    idx = df['Flag31'] & df['Flag81']
    df.loc[idx, 'Gain_Cor'] = df['Gain_ECC'][idx] * df['Gain_Prime'][idx]
    df.loc[idx, 'Offset_Cor'] = df['Gain_ECC'][idx] * df['Offset_Prime'][idx] +\
        df['Offset_ECC'][idx]
    # otherwise keep uncorrected ECC Values
    idx = ~idx
    df.loc[idx, 'Gain_Cor'] = df['Gain_ECC'][idx]
    df.loc[idx, 'Offset_Cor'] = df['Offset_ECC'][idx]

    return df

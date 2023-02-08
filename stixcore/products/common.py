from pathlib import Path

import numpy as np

from stixcore.config.reader import get_sci_channels, read_energy_channels
from stixcore.util.logging import get_logger

__all__ = ['ENERGY_CHANNELS', '_get_compression_scheme', '_get_energy_bins', '_get_detector_mask',
           '_get_pixel_mask', '_get_num_energies', '_get_unique', '_get_sub_spectrum_mask',
           '_get_energies_from_mask', 'rebin_proportional']

logger = get_logger(__name__)

# TODO get file from config
ENERGY_CHANNELS = read_energy_channels(Path(__file__).parent.parent / "config" / "data" /
                                       "common" / "detector" / "ScienceEnergyChannels_1000.csv")


def _get_compression_scheme(packets, nix):
    """
    Get the compression scheme parameters.

    Parameters
    ----------
    packets : dict
        Packets
    nix : `str`
        The parameter to look up the compression scheme values for

    Returns
    -------
    np.ndarray
        S,K,M compression scheme parameters and names
    """
    param = packets.get(nix)
    skm = param[0].skm
    values = np.array((skm[0].value, skm[1].value, skm[2].value), np.ubyte).reshape(1, -1)

    return values, {'NIXS': [skm[0].name, skm[1].name, skm[2].name],
                    'PCF_CURTX': [p.idb_info.PCF_CURTX for p in skm]}


def _get_energy_bins(packets, nixlower, nixuppper):
    """
    Get energy bin mask from packets

    Parameters
    ----------
    packets : dict
        Packets
    nixlower : str
        Parameter name of lower 32 bins
    nixuppper : str
        Parameters name of the upper bin

    Returns
    -------
    np.ndarray
        Full energy mask of len 33
    """
    energy_bin_mask = np.array(packets.get_value(nixlower), np.uint32)
    energy_bin_mask_upper = np.array(packets.get_value(nixuppper), np.bool8)
    full_energy_mask = [format(mask, 'b').zfill(32)[::-1] + format(upper, 'b') for mask, upper in
                        zip(energy_bin_mask, energy_bin_mask_upper)]
    full_energy_mask = [list(map(int, m)) for m in full_energy_mask]
    full_energy_mask = np.array(full_energy_mask).astype(np.ubyte)

    meta = {'NIXS': [nixlower, nixuppper],
            'PCF_CURTX': [packets.get(n)[0].idb_info.PCF_CURTX for n in [nixlower, nixuppper]]}

    return full_energy_mask, meta


def _get_detector_mask(packets):
    """
    Get the detector mask.
    Parameters
    ----------
    packets : dict
        Packets

    Returns
    -------
    np.ndarray
        Detector mask
    """
    detector_masks = np.array([list(format(dm, '032b'))[::-1]
                               for dm in packets.get_value('NIX00407')]).astype(np.ubyte)

    param = packets.get('NIX00407')[0]
    meta = {'NIXS': 'NIX00407', 'PCF_CURTX': param.idb_info.PCF_CURTX}

    return detector_masks, meta


def _get_pixel_mask(packets, param_name='NIXD0407'):
    """
    Get pixel mask.

    Parameters
    ----------
    packets : dict
        Packets

    Returns
    -------
    np.ndarray
        Pixel mask
    """
    pixel_masks_ints = packets.get_value(param_name)

    pixel_masks = np.array([list(format(pm, '012b'))[::-1]
                            for pm in pixel_masks_ints]).astype(np.ubyte)

    param = packets.get(param_name)[0]
    meta = {'NIXS': param_name, 'PCF_CURTX': param.idb_info.PCF_CURTX}

    return pixel_masks, meta


def _get_num_energies(packets):
    """
    Get number of energies.

    Parameters
    ----------
    packets : dict
        Packets

    Returns
    -------
    int
        Number of energies
    """
    return packets.get_value('NIX00270')


def _get_unique(packets, param_name, dtype):
    """
    Get a unique parameter raise warning if not unique.

    Parameters
    ----------
    param_name : str
        STIX parameter name eg NIX00001
    dtype : np.dtype
        Dtype to cast to eg. np.uint16/np.uint32

    Returns
    -------
    np.ndarray
        First value even if not unique
    """
    param = np.array(packets.get_value(param_name), dtype)
    if not np.all(param == param[0]):
        logger.warning('%s has changed in complete packet sequence', param_name)
    return param[0]


def _get_sub_spectrum_mask(packets):
    """
    Get subspectrum mask as bool array

    Parameters
    ----------
    packets : dict
        Merged packets

    Returns
    -------
    numpy.ndarray
        Bool array of mask
    """
    nix = 'NIX00160'
    sub_spectrum_masks = np.array([
        [bool(int(x)) for x in format(packets.get_value(nix)[i], '08b')][::-1]
        for i in range(len(packets.get_value(nix)))], np.ubyte)

    param = packets.get(nix)[0]
    meta = {'NIXS': nix, 'PCF_CURTX': param.idb_info.PCF_CURTX}
    return sub_spectrum_masks, meta


def _get_energies_from_mask(date, mask=None):
    """
    Return energy channels for
    Parameters
    ----------
    mask : list or array
        Energy bin mask

    Returns
    -------
    tuple
        Lower and high energy edges
    """
    sci_energy_channels = get_sci_channels(date.replace(tzinfo=None))
    e_lower = sci_energy_channels['Elower'].filled(np.nan).value
    e_upper = sci_energy_channels['Eupper'].filled(np.nan).value

    if mask is None:
        low = [e_lower[edge] for edge in range(32)]
        high = [e_upper[edge] for edge in range(32)]
    elif len(mask) == 33:
        edges = np.where(np.array(mask) == 1)[0]
        channel_edges = [edges[i:i + 2].tolist() for i in range(len(edges) - 1)]
        low = []
        high = []
        for edge in channel_edges:
            l, h = edge
            low.append(e_lower[l])
            high.append(e_upper[h - 1])
    elif len(mask) == 32:
        edges = np.where(np.array(mask) == 1)
        low_ind = np.min(edges)
        high_ind = np.max(edges)
        low = [e_lower[i] for i in range(low_ind, high_ind+1)]
        high = [e_upper[i] for i in range(low_ind, high_ind+1)]
    else:
        raise ValueError(f'Energy mask or edges must have a length of 32 or 33 not {len(mask)}')

    return low, high


def get_min_uint(values):
    """
    Find the smallest unsigned int that can represent max value.
    """
    max_value = np.array(values).max()
    if max_value < 256:  # 2**8
        return np.uint8
    elif max_value < 65536:  # 2**16
        return np.uint16
    elif max_value < 4294967296:  # 2**32
        return np.uint32
    elif max_value < 18446744073709551616:  # 2**64
        return np.uint64


def rebin_proportional(y1, x1, x2):
    x1 = np.asarray(x1)
    y1 = np.asarray(y1)
    x2 = np.asarray(x2)

    # the fractional bin locations of the new bins in the old bins
    i_place = np.interp(x2, x1, np.arange(len(x1)))

    cum_sum = np.r_[[0], np.cumsum(y1)]

    # calculate bins where lower and upper bin edges span
    # greater than or equal to one original bin.
    # This is the contribution from the 'intact' bins (not including the
    # fractional start and end parts.
    whole_bins = np.floor(i_place[1:]) - np.ceil(i_place[:-1]) >= 1.
    start = cum_sum[np.ceil(i_place[:-1]).astype(int)]
    finish = cum_sum[np.floor(i_place[1:]).astype(int)]

    y2 = np.where(whole_bins, finish - start, 0.)

    bin_loc = np.clip(np.floor(i_place).astype(int), 0, len(y1) - 1)

    # fractional contribution for bins where the new bin edges are in the same
    # original bin.
    same_cell = np.floor(i_place[1:]) == np.floor(i_place[:-1])
    frac = i_place[1:] - i_place[:-1]
    contrib = (frac * y1[bin_loc[:-1]])
    y2 += np.where(same_cell, contrib, 0.)

    # fractional contribution for bins where the left and right bin edges are in
    # different original bins.
    different_cell = np.floor(i_place[1:]) > np.floor(i_place[:-1])
    frac_left = np.ceil(i_place[:-1]) - i_place[:-1]
    contrib = (frac_left * y1[bin_loc[:-1]])

    frac_right = i_place[1:] - np.floor(i_place[1:])
    contrib += (frac_right * y1[bin_loc[1:]])

    y2 += np.where(different_cell, contrib, 0.)

    return y2

#########################################################
### Temporary code should be come from stixpy finally ###
#########################################################

import numpy as np
import stixpy.calibration.visibility
import stixpy.coordinates.transforms
import sunpy.map
import sunpy.sun.constants as sun_const
import sunpy.time
import xrayvision.imaging
from stixpy.calibration.visibility import (
    calibrate_visibility,
    create_meta_pixels,
    create_visibility,
)
from stixpy.coordinates.frames import STIXImaging
from stixpy.coordinates.transforms import get_hpc_info
from sunpy.coordinates import HeliographicStonyhurst, SphericalScreen, frames
from sunpy.time import TimeRange
from xrayvision.imaging import vis_to_image

from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.coordinates.representation import CartesianRepresentation
from astropy.time import Time


def get_rsun_obs(observer):
    """
    Get the observed radius of the Sun from an observer location.
    """

    rsun_obs = ((sun_const.radius / (observer.spherical.distance - sun_const.radius)).decompose() * u.radian).to(
        u.arcsec
    )
    return rsun_obs


def get_distance_off_limb(coord):
    theta_x = coord.Tx
    theta_y = coord.Ty
    rsun_obs = get_rsun_obs(coord.observer)
    distance_off_limb = np.sqrt(theta_x**2 + theta_y**2) - rsun_obs
    distance_r_sun = np.sqrt(theta_x**2 + theta_y**2) / rsun_obs

    return distance_off_limb, distance_r_sun


def generate_blank_map(date_obs, observer):
    """
    Given a date and an observer create a blank map

    """
    data = np.full((12, 12), np.nan)

    # Define a reference coordinate and create a header using sunpy.map.make_fitswcs_header
    skycoord = SkyCoord(0 * u.arcsec, 0 * u.arcsec, frame=frames.Helioprojective(observer=observer, obstime=date_obs))

    # Scale set to the following for solar limb to be in the field of view
    header = sunpy.map.make_fitswcs_header(data, skycoord, scale=[600, 600] * u.arcsec / u.pixel)

    # Use sunpy.map.Map to create the blank map
    blank_map = sunpy.map.Map(data, header)
    return blank_map


def is_visible(coord):
    """
    Returns whether the coordinate is on the visible side of the Sun.
    This function is a modified version of PR#7118
    """

    coord = coord.make_3d()
    data = coord.cartesian
    data_to_sun = coord.observer.radius * CartesianRepresentation(1, 0, 0) - data

    is_behind = data.x < 0
    # print(data.x.to(u.AU))
    is_beyond_limb = np.sqrt(1 - (data.x / data.norm()) ** 2) > coord.rsun / coord.observer.radius
    # is_above_surface = data_to_sun.norm() >= coord.rsun

    is_on_near_side = data.dot(data_to_sun) >= 0

    return is_behind | is_beyond_limb | (is_on_near_side)


def stx_estimate_flare_location(cpd_sci, time_range, energy_range):
    """
    Estimate the flare location using STIX imaging data.

    This function processes the imaging data from the STIX instrument on Solar Orbiter to estimate the location of a solar flare.
    It is based on the IDL software `stx_estimate_flare_location`.

    It creates back-projected images in both STIX imaging coordinates and Helioprojective coordinates, and finds the maximum location of the pixel.

    Optionally, it plots the results showing the maximum pixel locations in both coordinate systems.

    Parameters
    ----------
    cpd_sci : CPDProduct
        the STIX pixel data product.
    time_range : `sunpy.time.TimeRange`
        The time range over which to estimate the flare location.
    energy_range : `astropy.units.Quantity`
        The energy range (e.g., in keV) for the analysis.

    Returns
    -------
    max_stix : `astropy.coordinates.SkyCoord`
        The estimated flare location in STIX imaging coordinates.
    max_hpc : `astropy.coordinates.SkyCoord`
        The estimated flare location in Helioprojective Cartesian coordinates.

    Notes
    -----
    The function involves the following steps:
    - Reading STIX pixel data and generating meta pixels for a given time and energy range.
    - Creating visibility data from the meta pixels.
    - Obtaining solar observer coordinates and converting them to the Heliographic Stonyhurst frame.
    - Creating a back-projected image from the visibility data.
    - Transforming the coordinates of the maximum pixel in the image to Helioprojective coordinates.

    """

    meta_pixels_sci = create_meta_pixels(
        cpd_sci,
        time_range=[time_range.start, time_range.end],
        energy_range=energy_range,
        flare_location=[0, 0] * u.arcsec,
        no_shadowing=True,
    )

    # create visibilities
    vis = create_visibility(meta_pixels_sci)
    vis_tr = TimeRange(vis.meta["time_range"])

    roll, solo_xyz, pointing = get_hpc_info(vis_tr.start, vis_tr.end)
    solo = frames.HeliographicStonyhurst(*solo_xyz, obstime=vis_tr.center, representation_type="cartesian")

    center_map = SkyCoord(0 * u.arcsec, 0 * u.arcsec, frame=frames.Helioprojective(observer=solo, obstime=solo.obstime))
    center_coord = center_map.transform_to(STIXImaging(obstime=vis_tr.start, obstime_end=vis_tr.end, observer=solo))

    # get calibrated visibilities - use center of Sun as phase center
    cal_vis = calibrate_visibility(vis, flare_location=center_coord)

    # order by sub-collimator e.g. 10a, 10b, 10c, 9a, 9b, 9c ....
    isc_10_7 = [3, 20, 22, 16, 14, 32, 21, 26, 4, 24, 8, 28]
    idx = np.argwhere(np.isin(cal_vis.meta["isc"], isc_10_7)).ravel()

    # only use subcolimators 7 - 10
    vis10_7 = cal_vis[idx]

    # set up image size
    imsize = [512, 512] * u.pixel

    # to make sure the full Sun is within FOV - the 2.6 is taken to be the same as the IDL software
    pixel = get_rsun_obs(solo) * 2.6 / imsize

    # get back projection image
    bp_image = vis_to_image(vis10_7, imsize, pixel_size=pixel)

    # Make a sunpy map from the bp_image, in STIX imaging frame
    header = sunpy.map.make_fitswcs_header(
        bp_image, center_coord, telescope="STIX", observatory="Solar Orbiter", scale=pixel
    )
    fd_bp_map = sunpy.map.Map((bp_image, header))

    sidelobes_ratio = calculate_sidelobes_ratio(fd_bp_map)

    # Make a sunpy map from the bp_image, in HPC from STIX observer
    hpc_ref = center_coord.transform_to(frames.Helioprojective(observer=solo, obstime=vis_tr.center))
    header_hp = sunpy.map.make_fitswcs_header(bp_image, hpc_ref, scale=pixel, rotation_angle=90 * u.deg + roll)
    hp_map = sunpy.map.Map((bp_image, header_hp))

    # get the position of the max pixel
    max_pixel = np.argwhere(fd_bp_map.data == fd_bp_map.data.max()).ravel() * u.pixel
    # get the world coord of the max pixel - (note WCS axes and array are reversed)
    max_stix = fd_bp_map.pixel_to_world(max_pixel[1], max_pixel[0])

    # get the coordinate of the max pixel in HPC - if coordinate is off limb, assume spherical screen for transform
    with SphericalScreen(hp_map.observer_coordinate, only_off_disk=True):
        max_hpc = max_stix.transform_to(hp_map.coordinate_frame)

    vis_time_range = TimeRange(vis.meta["time_range"][0], vis.meta["time_range"][1])
    return max_stix, max_hpc, sidelobes_ratio, solo, vis_time_range


def calculate_sidelobes_ratio(bp_nat_map, threshold=200 * u.arcsec):
    """

    Calculate the sidelobes ratio for a back-projected image map.

    The sidelobes ratio is a measure of the relative strength of the sidelobes compared to the main peak of the image.
    This ratio helps determine the reliability of the flare location. A sidelobes ratio close to or above 0.9 suggests
    that the flare location may not be reliable due to significant sidelobe interference.

    Parameters
    ----------
    bp_nat_map : `sunpy.map.Map`
        The back-projected image map (in natural units) to analyze. This map is typically generated from visibility data
        and contains the image of the flare.
    threshold : `astropy.units.Quantity`, optional
        The angular separation threshold (in arcseconds) around the peak within which sidelobes are excluded from the calculation.
        Default is 200 arcseconds.

    Returns
    -------
    sidelobes_ratio : float
        The ratio of the maximum sidelobe intensity to the peak intensity in the back-projected image.
        A value close to 1 indicates significant sidelobes, potentially making the flare location unreliable.

    Notes
    -----
    - This is based upon the methodology in the STIX-GSW IDL software.
    """
    max_bp = np.max(bp_nat_map.data)
    ind_max = np.unravel_index(np.argmax(bp_nat_map.data, axis=None), bp_nat_map.data.shape)
    max_bp_coord = bp_nat_map.pixel_to_world(ind_max[1] * u.pix, ind_max[0] * u.pix)

    yy, xx = np.indices(bp_nat_map.data.shape)
    world_coords = bp_nat_map.pixel_to_world(xx * u.pix, yy * u.pix)

    distance_wrt_peak = world_coords.separation(max_bp_coord)

    bp_image_masked = np.copy(bp_nat_map.data)
    mask = distance_wrt_peak <= threshold
    bp_image_masked[mask] = 0

    sidelobes_ratio = np.max(bp_image_masked) / max_bp

    return sidelobes_ratio


def _construct_stix_calibrated_visibilities(
    cpd_sci,
    flare_location,
    time_range=None,
    energy_range=None,
    subcollimators=None,
    cpd_bkg=None,
    time_range_bkg=None,
    **kwargs,
):
    """
    Constructs calibrated STIX visibilities from STIX compressed pixel data.

    Extra kwargs are passed to `stixpy.calibration.visibility.create_meta_pixels`

    Parameters
    ----------
    cpd_sci: `stixpy.product.Product`
        The STIX pixel data. Assumed to be already background subtracted.
    flare_location: `astropy.coordinates.SkyCoord`
        The flare location. Frame must be convertible to `stixpy.coordinates.transdforms.STIXImaging`.
    time_range: `sunpy.time.TimeRange` (optional)
        The time range over which to estimate the flare location.
        Default is all times in cpd_sci.
    energy_range: `astropy.units.Quantity` in spectral units (optional)
        Length-2 quantity giving the lower and upper bounds of the energy range to use for imaging.
        Default is all finite energies in cpd_sci.
    subcollimators: `iterable` of `str`
        The labels of the subcollimators to use in estimating the flare locations, e.g.
        ``['10a', '10b', '10c',...]``
        Default is all subcollimators in cpd_sci.
    cpd_bkg: stixpy.product.Product` (optional)
        The background to subtract from the pixel data before determining the flare location.
        If None and time_range_bkg is also None, no background is subtracted.
    time_range_bkg: `sunpy.time.TimeRange`
        The time range within cpd_bkg to use for the background subtraction.
        If None, entire time range of cpd_bkg is used to determine background.
        If not None, and cpd_bkg is None, the background is determined from this time range
        applied to cpd_sci.

    Returns
    -------
    vis: `xrayvision.visibility.Visibilities`
        The calibrated STIX visibilities.
    """
    # Sanitze inputs.
    if time_range is None:
        time_range = cpd_sci.time_range
    times = Time([time_range.start, time_range.end])
    if energy_range is None:
        energy_range = u.Quantity(
            [
                cpd_sci.energies["e_low"][np.isfinite(cpd_sci.energies["e_low"])][0],
                cpd_sci.energies["e_high"][np.isfinite(cpd_sci.energies["e_high"])][-1],
            ]
        )
    no_shadowing = kwargs.pop("no_shadowing", True)
    # Generate meta_pixels from pixel_data.
    meta_pixels = stixpy.calibration.visibility.create_meta_pixels(
        cpd_sci,
        time_range=times,
        energy_range=energy_range,
        flare_location=flare_location,
        no_shadowing=no_shadowing,
        **kwargs,
    )
    # Subtract background if background pixel data provided.
    if cpd_bkg is None and time_range_bkg is not None:
        cpd_bkg = cpd_sci
    if cpd_bkg is not None:
        if time_range_bkg is None:
            time_range_bkg = cpd_bkg.time_range
        times_bkg = Time([time_range_bkg.start, time_range_bkg.end])
        meta_pixels_bkg = stixpy.calibration.visibility.create_meta_pixels(
            cpd_bkg,
            time_range=times_bkg,
            energy_range=energy_range,
            flare_location=[0, 0] * u.arcsec,
            no_shadowing=no_shadowing,
            **kwargs,
        )
        meta_pixels = _subtract_background_from_stix_meta_pixels(meta_pixels, meta_pixels_bkg)
    # Generate and calibrate visibilities.
    vis = stixpy.calibration.visibility.create_visibility(meta_pixels)
    vis = stixpy.calibration.visibility.calibrate_visibility(vis, flare_location=SkyCoord(flare_location))
    if subcollimators is not None:
        idx_subcol = np.argwhere(np.isin(vis.meta["vis_labels"], subcollimators)).ravel()
        vis = vis[idx_subcol]
    return vis


def _estimate_stix_flare_location(
    cpd_sci, time_range=None, energy_range=None, subcollimators=None, cpd_bkg=None, time_range_bkg=None
):
    """
    Estimates flare location from STIX compressed pixel data.

    FLare location is assumed to be the location of the brightest pixel in the backprojection
    map calculated from the input STIX pixel data.

    Parameters
    ----------
    cpd_sci: `stixpy.product.Product`
        The STIX pixel data. Assumed to be already background subtracted.
    time_range: `sunpy.time.TimeRange` (optional)
        The time range over which to estimate the flare location.
        Default is all times in cpd_sci.
    energy_range: `astropy.units.Quantity` in spectral units (optional)
        Length-2 quantity giving the lower and upper bounds of the energy range to use for imaging.
        Default is all finite energies in cpd_sci.
    subcollimators: `iterable` of `str` (optional)
        The labels of the subcollimators to include in the output Visibilities object, e.g.
        ``['10a', '10b', '10c',...]``
        Default is all subcollimators in cpd_sci
    cpd_bkg: stixpy.product.Product` (optional)
        The background to subtract from the pixel data before determining the flare location.
        Passed to construct_stix_calibrated_visibilities().
    time_range_bkg: `sunpy.time.TimeRange`
        The time range within cpd_bkg to use for the background subtraction.
        Passed to construct_stix_calibrated_visibilities().

    Returns
    -------
    flare_loc: `astropy.coordinates.SkyCoord`
        The estimated flare location.
    map_bp: `sunpy.map.Map`
        The backprojection map from whose brightest pixel the flare location was estimated.
    """
    if time_range is None:
        time_range = cpd_sci.time_range
    if subcollimators is None:
        subcollimators = ["10a", "10b", "10c", "9a", "9b", "9c", "8a", "8b", "8c", "7a", "7b", "7c"]
    # Construct STIX location and centre of FOV.
    roll, solo_xyz, pointing = stixpy.coordinates.transforms.get_hpc_info(time_range.start, time_range.end)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=time_range.center, representation_type="cartesian")
    fov_centre = STIXImaging(
        0 * u.arcsec, 0 * u.arcsec, obstime=time_range.start, obstime_end=time_range.end, observer=solo
    )
    # Generate calibrated visibilities using coarser subcollimators.
    vis = _construct_stix_calibrated_visibilities(
        cpd_sci, fov_centre, time_range=time_range, energy_range=energy_range, subcollimators=subcollimators
    )
    # Produce backprojected image and find brightest pixel. Use this for flare location.
    imsize = [512, 512] * u.pixel  # number of pixels of the map to reconstruct
    plate_scale = [10, 10] * u.arcsec / u.pixel  # pixel size in arcsec
    bp_image = xrayvision.imaging.vis_to_image(vis, imsize, pixel_size=plate_scale)
    max_idx = np.argwhere(bp_image == bp_image.max()).ravel()
    # Calculate WCS for backprojected image in STIXImaging and HPC frames.
    # Recalculate STIX HPC info as slightly different times will have been used
    # than input times due to onboard STIX time binning.
    vis_tr = sunpy.time.TimeRange(vis.meta["time_range"])
    roll, solo_xyz, pointing = stixpy.coordinates.transforms.get_hpc_info(vis_tr.start, vis_tr.end)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=vis_tr.center, representation_type="cartesian")
    coord = STIXImaging(0 * u.arcsec, 0 * u.arcsec, obstime=vis_tr.start, obstime_end=vis_tr.end, observer=solo)
    header_bp = sunpy.map.make_fitswcs_header(
        bp_image, coord, telescope="STIX", observatory="Solar Orbiter", scale=plate_scale
    )
    map_bp = sunpy.map.Map(bp_image, header_bp)
    wcs_bp = map_bp.wcs
    # Estimate flare location from brightest pixel in backprojection image
    flare_loc = wcs_bp.array_index_to_world(*max_idx)
    return flare_loc, map_bp, solo


def _subtract_background_from_stix_meta_pixels(meta_pixels_sci, meta_pixels_bkg):
    """
    Estimates flare location from STIX pixel data.

    Parameters
    ----------
    meta_pixels_sci: `dict`
        The STIX meta pixel representing the observations. Format must be
        same as output from `stixpy.calibration.visibility.create_meta_pixels`.
    meta_pixels_bkg: `dict`
        The STIX meta pixels representing the background. Format must be
        same as output from `stixpy.calibration.visibility.create_meta_pixels`.
    """
    meta_pixels_bkg_sub = {
        **meta_pixels_sci,
        "abcd_rate_kev_cm": meta_pixels_sci["abcd_rate_kev_cm"] - meta_pixels_bkg["abcd_rate_kev_cm"],
        "abcd_rate_error_kev_cm": np.sqrt(
            meta_pixels_sci["abcd_rate_error_kev_cm"] ** 2 + meta_pixels_bkg["abcd_rate_error_kev_cm"] ** 2
        ),
    }
    return meta_pixels_bkg_sub

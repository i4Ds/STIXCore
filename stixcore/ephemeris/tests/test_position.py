from pathlib import Path
from datetime import datetime

import numpy as np
import pytest

import astropy.units as u

from stixcore.config.config import CONFIG
from stixcore.ephemeris.manager import Spice, SpiceKernelManager
from stixcore.time.datetime import SCETime


@pytest.fixture
def spice():
    _spm = SpiceKernelManager(Path(CONFIG.get("Paths", "spice_kernels")))
    Spice.instance = Spice(_spm.get_latest_mk())
    return Spice.instance


def test_get_position(spice):
    res = spice.get_position(date=datetime(2020, 10, 7, 12), frame='SOLO_HEEQ')
    # from idl sunspice
    # CSPICE_FURNSH, 'test_position_20201001_V01.mk'
    # GET_SUNSPICE_COORD( '2020-10-7T12:00:00', 'SOLO', system='HEEQ')
    ref = [-89274134.69290607, 116495809.40033908, -16959307.70363023] * u.km
    assert np.allclose(ref, res)


def test_aux(spice):
    d = spice.datetime_to_scet(datetime(2020, 10, 7, 12))
    orient, dist, car, heeq = spice.get_auxiliary_positional_data(date=d)
    orient_ref = [-1.10229843, 0.00039536, -0.00033562] * u.deg
    assert np.allclose(orient, orient_ref)

    dist_ref = [147745601.79847595] * u.km
    assert np.allclose(dist, dist_ref)

    car_ref = [127.46399814, -6.5913527] * u.deg
    assert np.allclose(car, car_ref)

    heeq_ref = [-89274134.6929042, 116495809.4003411, -16959307.70363055] * u.km
    assert np.allclose(heeq, heeq_ref)


def test_get_orientation(spice):
    # from idl sunspice
    # CSPICE_FURNSH, 'test_position_20201001_V01.mk'
    # GET_SUNSPICE_ROLL( '2020-10-7T12:00:00', 'SOLO', system='HEEQ', yaw, pitch ) SOLO_HEEQ
    res_roll, res_pitch, res_yaw = spice.get_orientation(date=datetime(2020, 10, 7, 12),
                                                         frame='SOLO_HEEQ')
    ref_roll, ref_pitch, ref_yaw = [1.1023372100542925, 6.5917480592073163,
                                    -52.536339712903256] * u.deg
    assert np.allclose(ref_roll, res_roll)
    # IDL implementation switches sign of pitch?
    assert np.allclose(-ref_pitch, res_pitch)
    assert np.allclose(ref_yaw, res_yaw)


# def test_convert_inst(spicemanager):
#     sun_solo_heeq = [-1.16771962e+08,  8.29911725e+07,  3.61322152e+07] * u.km
#     solo_sun_heeq = -1 * sun_solo_heeq
#
#     heeq_solo = HeliocentricEarthEcliptic(*sun_solo_heeq,
#         obstime=datetime(2020, 10, 7, 12), representation_type = 'cartesian')
#
#     coord = SkyCoord(0*u.deg, 0*u.deg, observer=heeq_solo, obstime=datetime(2020, 10, 7, 12),
#                      frame='helioprojective')
#     with spicemanager as spice:
#         x, y = spice.convert_to_inst(coord)
#         assert False

def test_get_fits_headers(spice):
    start_scet = SCETime(683769519, 58289)
    avg_scet = start_scet + 12*u.h

    r1 = spice.get_fits_headers(start_time=start_scet, average_time=avg_scet)
    r2 = spice.get_fits_headers(start_time=start_scet.to_time(),
                                average_time=avg_scet.to_time())

    arr = np.array([(r1[i][1], r2[i][1]) for i in range(1, len(r1) - 2)])
    assert np.allclose(arr[1:, 0], arr[1:, 1])


def test_get_fits_headers_in_off_times(spice):
    start_scet = SCETime(983769519, 58289)
    avg_scet = start_scet + 12*u.h

    h = spice.get_fits_headers(start_time=start_scet, average_time=avg_scet)

    assert h[1][0] == 'SPICE_ERROR'
    assert h[2][1] == ''

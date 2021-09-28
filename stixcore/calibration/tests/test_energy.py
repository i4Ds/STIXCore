from datetime import datetime

from stixcore.calibration.energy import energy_width_correction


def test_energy():
    energy_width_correction(datetime(2020, 6, 7))

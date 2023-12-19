import numpy as np
import pytest
from numpy.testing import assert_array_equal

import astropy.units as u

from stixcore.products.common import unscale_triggers
from stixcore.time import SCETimeDelta


@pytest.mark.parametrize('factor', [20, 30, 40])
@pytest.mark.parametrize('n_int', [10, 20, 30])
@pytest.mark.parametrize('ssid', [21, 22, 23, 24])
def test_unscale(factor, n_int, ssid):
    dmask = np.ones((1, 32))
    duration = SCETimeDelta(n_int * u.ds)

    n_groups = 1
    if ssid == 24:
        n_groups = 16

    norm = n_groups * n_int * factor

    triggers_in = np.repeat(np.arange(255*n_int*factor).reshape(-1, 1), 16, axis=1)
    if ssid == 24:
        triggers_in = triggers_in.sum(axis=1, keepdims=True)

    triggers_scaled = np.floor(triggers_in / norm)
    trigger_unscaled_var = 0.5 * norm
    triggers_unscaled = triggers_scaled * norm + trigger_unscaled_var
    triggers_out, trigger_out_var = unscale_triggers(triggers_scaled, integration=duration,
                                                     detector_masks=dmask, ssid=ssid, factor=factor)
    assert_array_equal(triggers_out, triggers_unscaled)
    assert_array_equal(trigger_out_var, trigger_unscaled_var**2)

import pytest

from stixcore.io.RidLutManager import RidLutManager


def test_singleton():
    assert RidLutManager.instance


def test_get_reason():
    r = RidLutManager.instance.get_reason(1)
    assert r == 'subject, purpose1, r1'


def test_get_reason_multi():
    r = RidLutManager.instance.get_reason(223)
    assert r == 'subject, purpose, r223 , c2 subject, purpose_again, r223 , c2'


def test_get_scaling_factor():
    sf = RidLutManager.instance.get_scaling_factor(1)
    assert sf == 1234


def test_get_scaling_factor_not_found():
    with pytest.raises(ValueError) as e:
        RidLutManager.instance.get_scaling_factor(123344)
    assert str(e.value).startswith("can't get scaling factor")


def test_get_scaling_factor_to_many():
    with pytest.raises(ValueError) as e:
        RidLutManager.instance.get_scaling_factor(223)
    assert str(e.value).startswith("can't get scaling factor")

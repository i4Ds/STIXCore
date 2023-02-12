import pytest
from numpy.testing import assert_allclose

from stixcore.calibration.transmission import Transmission


@pytest.mark.skip('Changes to transmission data')
def test_transmission_get_transmission():
    # test_materials = OrderedDict([('test1', [('al', 1 * u.mm), ('be', 2*u.mm)]),
    #                           ('test2', [('be', 1*u.mm)])])
    #
    # test_componenets = OrderedDict([('al', ({'Al': 1.0}, 2.7 * u.g/u.cm**3)),
    #                           ('be', ({'Be': 1.0}, 1.85 * u.g/u.cm**3))])

    trans = Transmission()
    res = trans.get_transmission()
    assert len(res.columns) == 34
    assert len(res) == 31
    assert res['energies'][0] == 4.0
    assert res['energies'][-1] == 150.0
    assert_allclose(res['det-0'][0], 1.2304064853433878e-05)
    assert_allclose(res['det-0'][-1], 0.9223594048282234)
    assert_allclose(res['det-10'][0], 2.866929141727759e-06)
    assert_allclose(res['det-10'][-1], 0.9186555303212736)

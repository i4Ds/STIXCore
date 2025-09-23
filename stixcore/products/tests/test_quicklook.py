import re
from io import StringIO
from asyncio.log import logger
from unittest.mock import patch

import pytest

from astropy.table import Table
from astropy.utils.diff import report_diff_values

from stixcore.data.test import test_data
from stixcore.products.level0 import quicklookL0 as qll0
from stixcore.products.level1 import quicklookL1 as qll1

testpackets = [
    (
        test_data.tmtc.TM_21_6_30,
        qll0.LightCurve,
        qll1.LightCurve,
        "lightcurve",
        "0659402030f00008",
        "0659402958f00008",
        232,
    ),
    (
        test_data.tmtc.TM_21_6_31,
        qll0.Background,
        qll1.Background,
        "background",
        "0659399870f00254",
        "0659402958f00254",
        386,
    ),
    (test_data.tmtc.TM_21_6_32, qll0.Spectra, qll1.Spectra, "spectra", "0659399434f00008", "0659402538f00008", 4),
    (test_data.tmtc.TM_21_6_33, qll0.Variance, qll1.Variance, "variance", "0659399970f00008", "0659402958f00008", 747),
    (
        test_data.tmtc.TM_21_6_34,
        qll0.FlareFlag,
        qll1.FlareFlag,
        "flareflag",
        "0659400170f00008",
        "0659402958f00008",
        697,
    ),
    (
        test_data.tmtc.TM_21_6_41_complete,
        qll0.EnergyCalibration,
        qll1.EnergyCalibration,
        "energy",
        "0659318520f00000",
        "0659402519f00000",
        1,
    ),
]


@patch("stixcore.products.levelb.binary.LevelB")
@pytest.mark.parametrize("packets", testpackets, ids=[f[0].stem for f in testpackets])
def test_quicklook(levelb, packets):
    hex_file, cl_l0, cl_l1, name, beg, end, size = packets
    with hex_file.open("r") as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = Table({"raw_file": ["raw.xml"], "packet": [0]})

    ql_l0 = cl_l0.from_levelb(levelb, parent="afits.fits")
    assert ql_l0.parent == ["afits.fits"]
    assert ql_l0.raw == ["raw.xml"]
    assert ql_l0.level == "L0"
    assert ql_l0.name == name
    assert ql_l0.scet_timerange.start.to_string(sep="f") == beg
    assert ql_l0.scet_timerange.end.to_string(sep="f") == end
    assert len(ql_l0.data) == size

    ql_l1 = cl_l1.from_level0(ql_l0, parent="al0.fits")
    assert ql_l1.level == "L1"


@patch("stixcore.products.levelb.binary.LevelB")
def test_lightcurve(levelb):
    hex_file = test_data.tmtc.TM_21_6_30
    with hex_file.open("r") as file:
        hex = file.read()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", hex)]
    levelb.control = Table({"raw_file": ["raw.xml"], "packet": [0]})

    ql_lc_product_l0 = qll0.LightCurve.from_levelb(levelb, parent="raw.xml")

    assert ql_lc_product_l0.level == "L0"
    assert ql_lc_product_l0.name == "lightcurve"
    assert ql_lc_product_l0.type == "ql"
    assert len(ql_lc_product_l0.control) == 1
    assert len(ql_lc_product_l0.data) == ql_lc_product_l0.control["num_samples"]

    ql_lc_product_l1 = qll1.LightCurve.from_level0(ql_lc_product_l0, parent="al0.fits")
    assert ql_lc_product_l1.level == "L1"
    assert ql_lc_product_l1.name == "lightcurve"
    assert len(ql_lc_product_l1.control) == 1
    assert len(ql_lc_product_l1.data) == ql_lc_product_l1.control["num_samples"]


@patch("stixcore.products.levelb.binary.LevelB")
@pytest.mark.parametrize("packets", testpackets, ids=[f[0].stem for f in testpackets])
def test_quicklook_add(levelb, packets):
    hex_file, cl_l0, cl_l1, name, beg, end, size = packets
    with hex_file.open("r") as file:
        hex = file.readlines()

    levelb.data.__getitem__.return_value = [re.sub(r"\s+", "", h) for h in hex]
    levelb.control = Table({"raw_file": ["raw.xml"], "packet": [0]})

    ql_l0_o = cl_l0.from_levelb(levelb, parent="afits.fits")
    ql_l0_c = cl_l0.from_levelb(levelb, parent="afits.fits")

    if isinstance(ql_l0_o, (qll0.LightCurve, qll0.Background)):
        pytest.skip("Issue with QL LC")

    t_shift = (ql_l0_c.scet_timerange.duration() / 2.0).astype("int")
    ql_l0_c.data["time"] += t_shift
    # I think would need something like this
    # offset = t_shift/(4*u.s).value
    ##ql_l0_c.data[:offset] = ql_l0_c.data[offset:]
    ql_l0_c.control["scet_coarse"] = ql_l0_c.control["scet_coarse"] + t_shift.value

    a1 = ql_l0_o + ql_l0_c
    a2 = ql_l0_c + ql_l0_o

    # and here potentially only compare the data columns/columns excluding index
    report = StringIO()
    equal_data = report_diff_values(a1.data, a2.data, fileobj=report)
    if not equal_data:
        report.seek(0)
        logger.warning("Diff in DATA table\n\n" + report.read())

    report = StringIO()
    equal_control = report_diff_values(a1.control, a2.control, fileobj=report)
    if not equal_control:
        report.seek(0)
        logger.warning("Diff in CONTROL table\n\n" + report.read())

    assert equal_data & equal_control

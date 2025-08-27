import bitstring
import pytest

from stixcore.tmtc.parser import Parameter, parse_binary, parse_repeated, split_into_length


def test_parse_binary():
    data = {"int:1": -1, "int:8": -(2**7), "int:16": -(2**15), "uint:1": 1, "uint:8": 2**8 - 1, "uint:16": 2**16 - 1}
    fmt_str = ", ".join(data.keys())
    test_binary = bitstring.pack(fmt_str, *data.values())
    structure = {i: key for i, key in enumerate(data.keys())}
    res = parse_binary(test_binary, structure)
    assert list(res["fields"].values()) == list(data.values())


def test_parse_unpack_NIX00065():
    hb = 5
    lb = 10
    comb = (hb << 8) + lb

    NIXD0159 = Parameter(
        name="NIXD0159",
        value=0,
        idb_info="",
    )

    res = NIXD0159.unpack_NIX00065()

    assert res.name == "NIXD0159"
    assert res.value == 0
    assert res.children[0].name == "NIX00065"
    assert res.children[0].value == 1

    NIXD0159.value = 1
    with pytest.raises(TypeError) as e:
        NIXD0159.unpack_NIX00065()
    assert e is not None

    NIX00065 = Parameter(name="NIX00065", value=lb, idb_info="")
    NIXD0159.children = [NIX00065]
    res = NIXD0159.unpack_NIX00065()
    assert res.children[0].value == lb

    children = [Parameter(name="NIX00065", value=hb, idb_info=""), Parameter(name="NIX00065", value=lb, idb_info="")]
    NIXD0159.children = children
    NIXD0159.value = 2

    res = NIXD0159.unpack_NIX00065()
    assert res.children[0].value == comb

    NIXD0159.value = 3
    with pytest.raises(ValueError) as e:
        NIXD0159.unpack_NIX00065()
    assert e is not None


def test_split_into_length():
    a = range(0, 10)
    split = [2, 3, 5]
    sa = split_into_length(a, split)
    assert len(sa) == len(split)
    for i, s in enumerate(split):
        assert len(sa[i]) == s

    with pytest.raises(ValueError) as e:
        split_into_length(a, [5, 3])
    assert e is not None

    with pytest.raises(ValueError) as e:
        split_into_length(a, [5, 10])
    assert e is not None


def test_parser_repeated():
    fmt = ", ".join(["uint:4"] * 4)
    values = list(range(4))
    test_binary = bitstring.pack(fmt, *values)
    res = parse_repeated(test_binary, {"param": "uint:4"}, 4)
    assert res["fields"]["param"] == [[0], [1], [2], [3]]

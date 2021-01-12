import bitstring
import numpy as np

from stixcore.tmtc.parser import PacketData, parse_binary, parse_repeated


def test_parse_binary():
    data = {'int:1': -1, 'int:8': -2**7, 'int:16': -2**15, 'uint:1': 1, 'uint:8': 2**8-1,
            'uint:16': 2**16-1}
    fmt_str = ', '.join(data.keys())
    test_binary = bitstring.pack(fmt_str, *data.values())
    structure = {i: key for i, key in enumerate(data.keys())}
    res = parse_binary(test_binary, structure)
    assert list(res['fields'].values()) == list(data.values())


def test_parse_unpack_NIX00065():
    hb = 5
    lb = 10
    res = [1, 1, 2, 1, 256 + (hb << 8) + lb]

    pd = PacketData.parameter_dict_2_PacketData(
        {'NIXD0159': [1, 2, {'NIX00065': 2}, 4, {'NIX00065': [hb, lb]}]}
    )
    v, name = PacketData.unpack_NIX00065(pd.get('NIXD0159'))
    assert v == res
    assert name == 'NIX00065'

    flatt = pd.flatten()
    assert (flatt.get(name) == np.array(res)).all()


def test_parser_repeated():
    fmt = ', '.join(['uint:4']*4)
    values = list(range(4))
    test_binary = bitstring.pack(fmt, *values)
    res = parse_repeated(test_binary, {'param': 'uint:4'}, 4)
    assert res['fields']['param'] == [[0], [1], [2], [3]]

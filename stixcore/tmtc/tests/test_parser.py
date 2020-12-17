import bitstring

from stixcore.tmtc.parser import parse_binary, parse_repeated


def test_parse_binary():
    data = {'int:1': -1, 'int:8': -2**7, 'int:16': -2**15, 'uint:1': 1, 'uint:8': 2**8-1,
            'uint:16': 2**16-1}
    fmt_str = ', '.join(data.keys())
    test_binary = bitstring.pack(fmt_str, *data.values())
    structure = {i: key for i, key in enumerate(data.keys())}
    res = parse_binary(test_binary, structure)
    assert list(res['fields'].values()) == list(data.values())


def test_parser_repeated():
    fmt = ', '.join(['uint:4']*4)
    values = list(range(4))
    test_binary = bitstring.pack(fmt, *values)
    res = parse_repeated(test_binary, {'param': 'uint:4'}, 4)
    assert res['fields']['param'] == [[0], [1], [2], [3]]

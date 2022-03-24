from pathlib import Path
from unittest.mock import patch

from stixcore.processing.TMtoLL01 import main, process_request


def test_process_request(monkeypatch, tmp_path):
    monkeypatch.setenv("instr_input_requests", "/Users/shane/Projects/STIX/LLDP/top_input/STIX")
    monkeypatch.setenv("instr_output", "/Users/shane/Projects/STIX/LLDP/instr_output")
    request = Path("/Users/shane/Projects/STIX/LLDP/top_input/STIX"
                   "/requests/request_20210412_183337_183_detected")
    out = tmp_path
    process_request(request, out)


@patch('stixcore.processing.TMtoLL01.get_paths')
def test_main(mock_paths):
    outpath = Path('/Users/shane/Projects/STIX/LLDP/instr_output')
    paths = {
        'requests': Path('/Users/shane/Projects/STIX/LLDP/top_input/STIX/requests'),
        'output':  outpath,
        'products': outpath.joinpath('products'),
        'failed': outpath.joinpath('failed'),
        'temporary': outpath.joinpath('temporary'),
        'logs': outpath.joinpath('logs'),
    }
    for p in paths.values():
        p.mkdir(exist_ok=True, parents=True)
    mock_paths.return_value = paths
    main()

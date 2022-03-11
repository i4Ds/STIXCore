from pathlib import Path

from stixcore.processing.TMtoLL01 import process_request


def test_process_request(monkeypatch, tmp_path):
    monkeypatch.setenv("instr_input_requests", "/Users/shane/Projects/STIX/LLDP/top_input/STIX")
    monkeypatch.setenv("instr_output", "/Users/shane/Projects/STIX/LLDP/instr_output")
    request = Path("/Users/shane/Projects/STIX/LLDP/top_input/STIX"
                   "/requests/request_20210412_183337_183_detected")
    out = tmp_path
    process_request(request, out)

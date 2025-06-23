# helper script to check for unprocessed TM xml files
# the same logic is implemented into the search_unprocessed_tm_files()
# method here: stixcore/processing/pipeline.py

import re
from pprint import pprint
from pathlib import Path

from stixcore.config.config import CONFIG

unprocessed_tm_files = list()
TM_REGEX = re.compile(r'.*PktTmRaw.*.xml$')
logging_dir = Path(CONFIG.get('Pipeline', 'log_dir'))
tm_dir = Path(CONFIG.get('Paths', 'tm_archive'))
tm_dir = Path("/data/stix/SOLSOC/from_edds/tm/incomming")
list_of_log_files = logging_dir.glob('*.xml.out')
latest_log_file = logging_dir / "TM_BatchRequest.PktTmRaw.SOL.0.2023.305.15.33.13.852.bBWz@2023.352.21.00.01.039.1.xml.out"  # noqa
tm_file = Path(tm_dir / str(latest_log_file.name)[0:-4])
ftime = tm_file.stat().st_mtime

for tmf in tm_dir.glob("*.xml"):
    log_out_file = logging_dir / (tmf.name + ".out")
    log_file = logging_dir / (tmf.name + ".log")
    print(f"test: {tmf.name}")
    if TM_REGEX.match(tmf.name) and tmf.stat().st_mtime > ftime and (not log_out_file.exists()
                                                                     or not log_file.exists()):
        unprocessed_tm_files.append(tmf)
        print(f"NOT FOUND: {log_out_file.name}")
    else:
        print(f"found: {log_out_file.name}")

pprint(unprocessed_tm_files)

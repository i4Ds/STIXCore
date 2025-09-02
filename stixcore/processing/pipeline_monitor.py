import sys
import json
import smtplib
import argparse
import datetime
from pprint import pprint, pformat
from pathlib import Path

from dateutil import parser as dateparser

from stixcore.config.config import CONFIG
from stixcore.processing.pipeline_status import get_status
from stixcore.util.logging import get_logger

__all__ = ["pipeline_monitor"]

logger = get_logger(__name__)


def pipeline_monitor(args):
    """Status logger and notification script for the pipeline.

    SetUp via cron.
    Query the number of open files still to process. Logs that number into a status file
    and checks if the the number is constantly equal or increasing.

    Sends an notification via mail if a possible pipeline stuck is detected.
    """
    parser = argparse.ArgumentParser(description="stix pipeline monitor")
    parser.add_argument(
        "-p",
        "--port",
        help="connection port for the status info server",
        default=CONFIG.getint("Pipeline", "status_server_port", fallback=12345),
        type=int,
    )

    parser.add_argument(
        "-s", "--save_file", help="file to persist last status", default="monitor_status.json", type=str
    )

    args = parser.parse_args(args)

    ret = get_status(b"next", args.port)
    open_files = int(ret.replace("open files: ", ""))
    save_file = Path(args.save_file)

    status = {}
    status["last"] = []

    if save_file.exists():
        with open(save_file, "+r") as f:
            try:
                status = json.load(f)
            except Exception:
                pass

    status["last"].append({"date": datetime.datetime.now().isoformat(timespec="milliseconds"), "open": open_files})

    status["last"] = status["last"][-9:]

    if len(status["last"]) == 9 and open_files > 0:
        stuck = True
        last_open = status["last"][0]
        for la in status["last"][1:]:
            if la["open"] <= 0 or la["open"] < last_open["open"]:
                stuck = False
                break
            last_open = la
        if stuck:
            fd = dateparser.parse(status["last"][0]["date"])
            ld = dateparser.parse(status["last"][-1]["date"])
            if (ld - fd).days >= 1:
                if CONFIG.getboolean("Publish", "report_mail_send", fallback=False):
                    try:
                        sender = CONFIG.get("Pipeline", "error_mail_sender", fallback="")
                        receivers = CONFIG.get("Publish", "report_mail_receivers").split(",")
                        host = CONFIG.get("Pipeline", "error_mail_smpt_host", fallback="localhost")
                        port = CONFIG.getint("Pipeline", "error_mail_smpt_port", fallback=25)
                        smtp_server = smtplib.SMTP(host=host, port=port)
                        message = f"""Subject: StixCore Pipeline Monitor

Pipeline stuck?

{pformat(status)}

Login to server and check
"""

                        smtp_server.sendmail(sender, receivers, message)
                    except Exception as e:
                        logger.error(f"Error: unable to send monitor email: {e}")

    with open(save_file, "w") as f:
        json.dump(status, f, indent=4)

    pprint(status)


def main():
    pipeline_monitor(sys.argv[1:])


if __name__ == "__main__":
    main()

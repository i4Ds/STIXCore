import sys
import socket
import argparse

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger

__all__ = ["pipeline_status", "get_status"]

logger = get_logger(__name__)


def get_status(msg, port=12346):
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    server_address = ("localhost", port)
    logger.info(f"connecting to {server_address[0]}:{server_address[1]}")
    sock.connect(server_address)

    try:
        # Send data
        sock.sendall(msg)
        sock.sendall(b"\n")
        server = sock.makefile("rb")
        response = ""
        while True:
            line = server.readline()
            if not line:
                break
            response += line.decode()
        return f"{response.rstrip()}"

    finally:
        sock.close()


def pipeline_status(args):
    parser = argparse.ArgumentParser(description="stix pipeline status")
    parser.add_argument(
        "-p",
        "--port",
        help="connection port for the status info server",
        default=CONFIG.getint("Pipeline", "status_server_port", fallback=12345),
        type=int,
    )

    parser.add_argument("-l", "--last", help="get the last TM file", const="last", type=str, dest="cmd", nargs="?")

    parser.add_argument(
        "-e",
        "--error",
        help="get the last TM file where an error occurred",
        const="error",
        type=str,
        dest="cmd",
        nargs="?",
    )

    parser.add_argument(
        "-c", "--current", help="get the current TM file", const="current", type=str, dest="cmd", nargs="?"
    )

    parser.add_argument(
        "-o", "--open", help="get the number of open TM files", const="open", type=str, dest="cmd", nargs="?"
    )

    parser.add_argument(
        "-n", "--next", help="get a list of open TM files", const="next", type=str, dest="cmd", nargs="?"
    )

    parser.add_argument(
        "-C", "--config", help="get the config of the pipeline service", const="config", type=str, dest="cmd", nargs="?"
    )

    args = parser.parse_args(args)

    cmd = args.cmd.encode() if args.cmd else b"last"

    print(get_status(cmd, args.port))


def main():
    pipeline_status(sys.argv[1:])


if __name__ == "__main__":
    main()

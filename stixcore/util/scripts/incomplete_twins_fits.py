import sys
import argparse
from pathlib import Path

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger
from stixcore.util.util import get_complete_file_name_and_path

logger = get_logger(__name__)


def incomplete_twins(args):
    parser = argparse.ArgumentParser(
        description="STIX publish to ESA processing step", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # paths
    parser.add_argument(
        "-f",
        "--fits_dir",
        help="fits directory where fits files should be found in",
        default=CONFIG.get("Publish", "target_dir"),
        type=str,
    )

    parser.add_argument(
        "-m",
        "--move_twin",
        help="should the matched twin FITS moved",
        default=False,
        action="store_true",
        dest="move_twin",
    )

    parser.add_argument(
        "--twin_target", help="target dir where the twin FITS is moved", default=None, dest="target_dir"
    )

    args = parser.parse_args(args)

    fits_dir = Path(args.fits_dir)
    if not fits_dir.exists():
        raise argparse.ArgumentError("--fits_dir directory not found")

    if args.move_twin and args.target_dir is None:
        raise argparse.ArgumentError("No target dir (--twin_target) given but --fits_dir enabled")

    target_dir = Path(args.target_dir)
    if not target_dir.exists():
        logger.info(f"path not found to target dir: {target_dir} creating dir")
        target_dir.mkdir(parents=True, exist_ok=True)

    twins = []

    for ufits in fits_dir.rglob("*_V*U.fits"):
        cfile = get_complete_file_name_and_path(ufits)
        if cfile.exists():
            logger.info(cfile)
            twins.append(cfile)

            if ufits.stat().st_size < cfile.stat().st_size:
                logger.warn(f"filesize of {ufits} is less then {cfile} UNEXPECTED")

            if args.move_twin:
                td = target_dir / cfile.relative_to(fits_dir).parent
                td.mkdir(parents=True, exist_ok=True)
                cfile.rename(td / cfile.name)

    return twins


def main():
    incomplete_twins(sys.argv[1:])


if __name__ == "__main__":
    main()

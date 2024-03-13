import sys
import logging
import argparse
from pathlib import Path

from astropy.io import fits

from stixcore.config.config import CONFIG
from stixcore.time.datetime import SCETime
from stixcore.util.logging import get_logger

__all__ = ['find_fits']

logger = get_logger(__name__)


def find_fits(args):
    """CLI STIX find fits file

    Parameters
    ----------
    args : list
        see -h for details

    Returns
    -------
    list
        list of fits files meeting the criteria
    """

    parser = argparse.ArgumentParser(description='STIX find fits files',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-v", "--include_versions",
                        help="what versions should be found",
                        default=CONFIG.get('Publish', 'include_versions', fallback="*"), type=str)

    parser.add_argument("-l", "--include_levels",
                        help="what levels should be found", type=str,
                        default=CONFIG.get('Publish', 'include_levels', fallback="LB, L0, L1, L2"))

    parser.add_argument("-p", "--include_products",
                        help="what products should be found", type=str,
                        default=CONFIG.get('Publish', 'include_products',
                                           fallback="21,ql,hk,sci,aux,cal"))

    parser.add_argument("-s", "--start_obt",
                        help="start onboard time",
                        default=0, type=int)

    parser.add_argument("-e", "--end_obt",
                        help="end onboard time",
                        default=2 ^ 32, type=int)

    parser.add_argument("-f", "--fits_dir",
                        help="input FITS directory for files to publish ",
                        default=CONFIG.get('Publish', 'fits_dir'), type=str)

    parser.add_argument("--log_level",
                        help="the level of logging",
                        default=None, type=str,
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
                        dest='log_level')

    args = parser.parse_args(args)

    if args.log_level:
        logger.setLevel(logging.getLevelName(args.log_level))

    fits_dir = Path(args.fits_dir)

    include_levels = dict([(level, 1) for level in
                           args.include_levels.lower().replace(" ", "").split(",")])

    if args.include_versions == "*":
        include_all_versions = True
    else:
        include_all_versions = False
        include_versions = dict([(int(version), 1) for version in
                                 args.include_versions.lower().replace(" ", "")
                                 .replace("v", "").split(",")])

    if args.include_products == "*":
        include_all_products = True
    else:
        include_all_products = False
        include_products = dict([(prod, 1) for prod in
                                 args.include_products.lower().replace(" ", "").split(",")])

    candidates = fits_dir.rglob("solo_*.fits")
    found = list()

    n_candidates = 0

    args.start_obt = SCETime(args.start_obt)
    args.end_obt = SCETime(args.end_obt)

    logger.info(f'include_all_products: {include_all_products}')
    logger.info(f'include_products: {include_products}')
    logger.info(f'include_all_versions: {include_all_versions}')
    logger.info(f'include_versions: {include_all_products}')
    logger.info(f'include_levels: {include_levels}')
    logger.info(f'fits_dir: {fits_dir}')
    logger.info(f'start obt: {args.start_obt}')
    logger.info(f'end obt: {args.end_obt}')

    logger.info("start searching fits files")

    for c in candidates:
        n_candidates += 1

        if n_candidates % 1000 == 0:
            logger.info(f'tested: {n_candidates} found  {len(found)}')

        parts = c.name[:-5].split('_')
        level = parts[1].lower()

        # version filter
        if not include_all_versions:
            version = int(parts[4].lower().replace("v", ""))
            if version not in include_versions:
                continue

        # level filter
        if level not in include_levels:
            continue

        # product filter
        if not include_all_products:
            product = str(parts[2].lower().split("-")[1])
            if product not in include_products:
                continue

        # prod = Product(c)
        obt_beg = SCETime.from_string(fits.getval(c, 'OBT_BEG'))
        obt_end = SCETime.from_string(fits.getval(c, 'OBT_END'))

        # obt filter overlapping
        if not ((obt_beg >= args.start_obt and obt_end <= args.end_obt) or
                (obt_beg >= args.start_obt and args.end_obt < obt_end) or
                (obt_beg < args.start_obt and obt_end > args.start_obt)):
            continue

        found.append(c)

    logger.info(f'#candidates: {n_candidates}')
    logger.info(f'#found: {len(found)}')

    for f in found:
        print(str(f))

    return found


def main():
    find_fits(sys.argv[1:])


if __name__ == '__main__':
    main()

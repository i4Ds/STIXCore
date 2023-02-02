import os
import sys
import shutil
import smtplib
import sqlite3
import argparse
import tempfile
import urllib.request
from enum import Enum
from pprint import pformat
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict

import numpy as np
from paramiko import SSHClient
from scp import SCPClient

import astropy.units as u
from astropy.io import ascii, fits
from astropy.io.fits.diff import HDUDiff
from astropy.table import Table
from astropy.table.operations import unique, vstack

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger

__all__ = ['PublishConflicts', 'PublishHistoryStorage', 'publish_fits_to_esa',
           'PublishHistoryStorage', 'PublishResult']

logger = get_logger(__name__)


class PublishConflicts(Enum):
    """Cases of ESA publishing actions based on the history DB of already published files."""

    ADDED = 0
    """This files raises no conflicts so publishing without any errors or duplication handling"""

    SAME_ESA_NAME = 1
    """There are other files already published to ESA with the same resulting ESA file name.
    Truncated request id modifier"""

    SAME_EXISTS = 2
    """The very same files was published before nothing todo"""


class PublishResult(Enum):
    """Result type for a FITS file published to ESA """

    PUBLISHED = 10
    """All good files was published"""

    IGNORED = 11
    """This files was ignored and not published to ESA"""

    MODIFIED = 12
    """Other files with the same ESA file name have already be published.
    Therefore the filename was modified to a supplementary data product. (-sub1/2)"""

    BLACKLISTED = 13
    """This files was ignored via a blacklist and not published to ESA"""

    ERROR = 14
    """An error was raised during publication"""


class PublishHistoryStorage:
    """Persistent handler for meta data on already published files."""

    def __init__(self, filename):
        """Create a new persistent handler. Will open or create the given sqlite DB file.

        Parameters
        ----------
        filename : path like object
            path to the sqlite database file
        """
        self.conn = None
        self.cur = None
        self.filename = filename
        self._connect_database()

    def _connect_database(self):
        """Connects to the sqlite file or creates an empty one if not present."""
        try:
            self.conn = sqlite3.connect(self.filename)
            self.cur = self.conn.cursor()
            logger.info('PublishHistory DB loaded from {}'.format(self.filename))

            sql = '''CREATE TABLE if not exists published (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        path TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        p_date FLOAT NOT NULL,
                        m_date FLOAT NOT NULL,
                        esaname TEXT NOT NULL,
                        modified_esaname TEXT,
                        result INTEGER
                    )
            '''
            self.cur.execute(sql)
            self.cur.execute('CREATE INDEX if not exists esaname ON published (esaname)')
            self.cur.execute('CREATE INDEX if not exists p_date ON published (p_date)')

            self.conn.commit()
        except sqlite3.Error:
            logger.error('Failed load DB from {}'.format(self.filename))
            self.close()
            raise

    def close(self):
        """Close the IDB connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.cur = None
        else:
            logger.warning("DB connection already closed")

    def is_connected(self):
        """Is the handler connected to the db file.

        returns
        -------
        True | False
        """
        if self.cur:
            return True
        return False

    def count(self):
        """Counts the number of entries in the DB

        Returns
        -------
        int
            number of tracked files
        """
        return self.cur.execute("select count(1) from published").fetchone()[0]

    def add(self, path):
        """Adds a new file to the history.

        Parameters
        ----------
        path : path like object
            path to the FITS file to be published

        Returns
        -------
        (`PublishConflicts`, `list`)
            PublishConflicts with already published files and a list of all "conflicting files"
            including the newly added files as last entry.
        """
        name = path.name
        dir = str(path.parent)
        date = datetime.now().timestamp()
        parts = name[:-5].split('_')
        version = int(parts[4].lower().replace("v", ""))
        esa_name = '_'.join(parts[0:5])
        m_date = path.stat().st_ctime
        try:
            self.cur.execute("""insert into published
                                    (name, path, version, p_date, m_date, esaname)
                                    values(?, ?, ?, ?, ?, ?)""",
                             (name, dir, version, date, m_date, esa_name))
            others = self.find_by_esa_name(esa_name)
            if (len(others) > 1):
                return (PublishConflicts.SAME_ESA_NAME, others)
            else:
                return (PublishConflicts.ADDED, others)
        except sqlite3.IntegrityError:
            return (PublishConflicts.SAME_EXISTS, self.find_by_name(name))

    def _execute(self, sql, arguments=None, result_type='list'):
        """Execute sql and return results in a list or a dictionary."""
        if not self.cur:
            raise Exception('DB is not initialized!')
        else:
            if arguments:
                self.cur.execute(sql, arguments)
            else:
                self.cur.execute(sql)
            if result_type == 'list':
                rows = self.cur.fetchall()
            else:
                rows = [
                    dict(
                        zip([column[0]
                            for column in self.cur.description], row))
                    for row in self.cur.fetchall()
                ]
            return rows

    def find_by_name(self, name):
        """Finds a history entry based on the file name (unique constraint).

        Parameters
        ----------
        name : `str`
            the file name

        Returns
        -------
        `hash`
            the history entry
        """
        return self._execute("select * from published where name = ?", (name, ), result_type='hash')

    def find_by_esa_name(self, esa_name):
        """Finds all history entries based on the the ESA name (not unique).

        Parameters
        ----------
        esa_name : `str`
            the truncated ESA file name of the fits file

        Returns
        -------
        `list`
            list of all found files with the same ESA file name
        """
        return self._execute("select * from published where esaname = ? order by p_date asc",
                             (esa_name, ), result_type='hash')

    def set_modified_esa_name(self, name, id):
        """Updates the modified ESA file name of an entry.

        Parameters
        ----------
        name : `str`
            the new modified esa name
        id : `int`
            the id of the entry to update

        Returns
        -------
        `hash`
            the update result
        """
        return self._execute("update published set modified_esaname = ? where id = ? ",
                             (name, id), result_type='hash')

    def set_result(self, result, id):
        """Sets the result of a publishing action for a given file.

        Parameters
        ----------
        result : `PublishResult`
            the result
        id : `int`
            the id of the entry to update

        Returns
        -------
        `hash`
            the update result
        """
        return self._execute("update published set result = ? where id = ? ",
                             (result.value, id), result_type='hash')

    def get_all(self):
        """Queries all entries

        Returns
        -------
        `list`
            all found entries.
        """
        return self._execute("select * from published")


def send_mail_report(files):
    if CONFIG.getboolean('Publish', 'report_mail_send', fallback=False):
        try:
            sender = CONFIG.get('Pipeline', 'error_mail_sender', fallback='')
            receivers = CONFIG.get('Publish', 'report_mail_receivers').split(",")
            host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
            port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
            smtp_server = smtplib.SMTP(host=host, port=port)
            su = (f"E {len(files[PublishResult.ERROR])} "
                  f"I {len(files[PublishResult.IGNORED]) + len(files[PublishResult.BLACKLISTED])} "
                  f"S {len(files[PublishResult.MODIFIED])} "
                  f"P {len(files[PublishResult.PUBLISHED])}")
            error = "" if len(files[PublishResult.ERROR]) <= 0 else "ERROR-"
            message = f"""Subject: StixCore TMTC Publishing {error}Report {su}

ERRORS (E)
**********
{pformat(files[PublishResult.ERROR])}

PUBLISHED ESA DUPLICATES (D)
****************************

RE-REQUESTED (I)
****************
{pformat(files[PublishResult.IGNORED])}

SUPPLEMENTS (S)
***************
{pformat(files[PublishResult.MODIFIED])}

BLACKLISTED (I)
***************
{pformat(files[PublishResult.BLACKLISTED])}

PUBLISHED (P)
*************
{pformat(files[PublishResult.PUBLISHED])}


StixCore

==========================

do not answer to this mail.
"""
            smtp_server.sendmail(sender, receivers, message)
        except Exception as e:
            logger.error(f"Error: unable to send report email: {e}")


def publish_fits_to_esa(args):
    """CLI STIX publish to ESA processing step

    Parameters
    ----------
    args : list
        see -h for details

    Returns
    -------
    list
        list of published files
    """

    parser = argparse.ArgumentParser(description='STIX publish to ESA processing step',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # pathes
    parser.add_argument("-t", "--target_dir",
                        help="target directory where fits files should be copied to",
                        default=CONFIG.get('Publish', 'target_dir'), type=str)

    # pathes
    parser.add_argument("-T", "--target_host",
                        help="target host server where fits files should be copied to",
                        default=CONFIG.get('Publish', 'target_host', fallback="localhost"),
                        type=str)

    parser.add_argument("-s", "--same_esa_name_dir",
                        help=("target directory where fits files should be copied to if there are "
                              "any naming conflicts with already published files"),
                        default=CONFIG.get('Publish', 'same_esa_name_dir'), type=str)

    parser.add_argument("-r", "--rid_lut_file",
                        help=("Path to the rid LUT file"),
                        default=CONFIG.get('Publish', 'rid_lut_file'), type=str)

    parser.add_argument("--update_rid_lut",
                        help="update rid lut file before publishing",
                        default=False,
                        action='store_true', dest='update_rid_lut')

    parser.add_argument("--sort_files",
                        help="should the matched FITS be sorted by name before publishing.",
                        default=False,
                        action='store_true', dest='sort_files')

    parser.add_argument("--blacklist_files",
                        help="input txt file with file names to that should not be delivered",
                        default=None, type=str, dest='blacklist_files')

    parser.add_argument("--supplement_report",
                        help="path to file where supplements and reqeust reasons are summarized",
                        default=None, type=str, dest='supplement_report')

    parser.add_argument("-d", "--db_file",
                        help="Path to the history publishing database", type=str,
                        default=CONFIG.get('Publish', 'db_file',
                                           fallback=str(Path.home() / "published.sqlite")))

    parser.add_argument("-w", "--waiting_period",
                        help="how long to wait after last file modification before publishing",
                        default=CONFIG.get('Publish', 'waiting_period', fallback="14d"), type=str)

    parser.add_argument("-v", "--include_versions",
                        help="what versions should be published",
                        default=CONFIG.get('Publish', 'include_versions', fallback="*"), type=str)

    parser.add_argument("-l", "--include_levels",
                        help="what levels should be published", type=str,
                        default=CONFIG.get('Publish', 'include_levels', fallback="L0, L1, L2"))

    parser.add_argument("-p", "--include_products",
                        help="what products should be published", type=str,
                        default=CONFIG.get('Publish', 'include_products', fallback="ql,hk,sci,aux"))

    parser.add_argument("-f", "--fits_dir",
                        help="input FITS directory for files to publish ",
                        default=CONFIG.get('Paths', 'fits_archive'), type=str)

    args = parser.parse_args(args)

    def addBSDComment(p, rid_lut):
        try:
            parts = p.name.split('_')
            if len(parts) > 5:
                rid = parts[5].replace(".fits", "")
                rid = int(rid.split('-')[0])
                reqeust = rid_lut.loc[rid]
                reason = " ".join(np.atleast_1d(reqeust['description']))
                c_entry = f"BSD request id: '{rid}' reason: '{reason}'"
                fits.setval(p, "COMMENT", value=c_entry)
                return c_entry
        except Exception as e:
            logger.info(e, exc_info=True)
            logger.info("no BSD request comment added")
        return ''

    def addHistory(p, name):
        time_formated = datetime.now().isoformat(timespec='milliseconds')
        h_entry = f"published to ESA SOAR as '{name}' on {time_formated}"
        fits.setval(p, "HISTORY", value=h_entry)

    def copyFile(scp, p, target_dir, add_history=True):
        if add_history:
            addHistory(p, p.name)
        try:
            if scp:
                scp.put(p, remote_path=target_dir)
            else:
                shutil.copy(p, target_dir)
            return (True, None)
        except Exception as e:
            return (False, e)

    def read_rid_lut(file, update=False):
        converters = {'_id': np.uint, 'unique_id': np.uint, 'start_utc': datetime,
                      'duration': np.uint, 'type': str, 'subject': str,
                      'purpose': str, 'comment': str}

        if update or not file.exists():
            rid_lut = Table(names=converters.keys(), dtype=converters.values())
            last_date = date(2018, 1, 1)
            today = date.today()
            if file.exists():
                rid_lut = ascii.read(args.rid_lut_file, delimiter=",", converters=converters)
                last_date = datetime.strptime(rid_lut['start_utc'].max(), '%Y-%m-%dT%H:%M:%S')\
                    .date()

            if not file.parent.exists():
                logger.info(f'path not found to rid lut file dir: {file.parent} creating dir')
                file.parent.mkdir(parents=True, exist_ok=True)
            rid_lut_file_update_url = CONFIG.get('Publish', 'rid_lut_file_update_url')
            while last_date < today:
                last_date_1m = last_date + timedelta(days=30)
                ldf = last_date.strftime('%Y%m%d')
                ld1mf = last_date_1m.strftime('%Y%m%d')
                update_url = f"{rid_lut_file_update_url}{ldf}/{ld1mf}"
                logger.info(f'download publish lut file: {update_url}')
                last_date = last_date_1m
                updatefile = tempfile.NamedTemporaryFile().name
                urllib.request.urlretrieve(update_url, updatefile)
                update_lut = ascii.read(updatefile, delimiter=",", converters=converters)

                if len(update_lut) < 1:
                    continue
                logger.info(f'found {len(update_lut)} entries')
                rid_lut = vstack([rid_lut, update_lut])

            rid_lut = unique(rid_lut, silent=True)
            ascii.write(rid_lut, file, overwrite=True, delimiter=",")
            logger.info(f'write total {len(rid_lut)} entries to local storage')
        else:
            logger.warn("read lut")
            rid_lut = ascii.read(args.rid_lut_file, delimiter=",", converters=converters)

        rid_lut['description'] = [", ".join(r.values()) for r in
                                  rid_lut['subject', 'purpose', 'comment'].filled()]
        rid_lut.add_index('unique_id')

        return rid_lut

    rid_lut = read_rid_lut(Path(args.rid_lut_file), update=args.update_rid_lut)

    scp = None
    if args.target_host != 'localhost':
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(args.target_host)

        # SCPCLient takes a paramiko transport as an argument
        scp = SCPClient(ssh.get_transport())

    db_file = Path(args.db_file)
    fits_dir = Path(args.fits_dir)
    if not fits_dir.exists():
        logger.error(f'path not found to input files: {fits_dir}')
        return

    wait_period = u.Quantity(args.waiting_period)
    wait_period_s = wait_period.to(u.s).value

    target_dir = Path(args.target_dir)
    if not target_dir.exists():
        logger.info(f'path not found to target dir: {target_dir} creating dir')
        target_dir.mkdir(parents=True, exist_ok=True)

    same_esa_name_dir = Path(args.same_esa_name_dir)
    if not same_esa_name_dir.exists():
        logger.info(f'path not found to target dir: {same_esa_name_dir} creating dir')
        same_esa_name_dir.mkdir(parents=True, exist_ok=True)

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
    to_publish = list()
    now = datetime.now().timestamp()
    hist = PublishHistoryStorage(db_file)
    n_candidates = 0

    blacklist_files = list()

    if args.blacklist_files:
        try:
            with open(args.blacklist_files, "r") as f:
                for ifile in f:
                    try:
                        ifile = ifile.strip()
                        if len(ifile) > 1:
                            blacklist_files.append(ifile)
                    except Exception as e:
                        print(e)
                        print("skipping this blacklist file line")
            blacklist_files = set(blacklist_files)
        except IOError as e:
            print(e)
            print("Fall back to no blacklist files")

    supplement_report = None
    if args.supplement_report:
        args.supplement_report = Path(args.supplement_report)
        supplement_report = Table(names=["filename", "basefile", 'comment'],
                                  dtype=[str, str, str])
        if not args.supplement_report.exists():
            ascii.write(supplement_report, args.supplement_report, delimiter=",")

    tempdir = tempfile.TemporaryDirectory()
    tempdir_path = Path(tempdir.name)

    logger.info(f'db_file: {db_file}')
    logger.info(f'include_all_products: {include_all_products}')
    logger.info(f'include_products: {include_products}')
    logger.info(f'include_all_versions: {include_all_versions}')
    logger.info(f'include_versions: {include_all_products}')
    logger.info(f'include_levels: {include_levels}')
    logger.info(f'same_esa_name_dir: {same_esa_name_dir}')
    logger.info(f'target_dir: {target_dir}')
    logger.info(f'target_host: {args.target_host}')
    logger.info(f'scp: {not scp is None}')
    logger.info(f'wait_period: {wait_period}')
    logger.info(f'fits_dir: {fits_dir}')
    logger.info(f'temp_dir: {tempdir_path}')
    logger.info(f'rid_lut_file: {args.rid_lut_file}')
    logger.info(f'update_rid_lut: {args.update_rid_lut}')
    logger.info(f"update_rid_lut_url: {CONFIG.get('Publish', 'rid_lut_file_update_url')}")
    logger.info(f'sort file: {args.sort_files}')
    logger.info(f'blacklist files: {blacklist_files}')
    logger.info(f'supplement report: {args.supplement_report}')
    logger.info("start publishing")

    for c in candidates:
        n_candidates += 1
        parts = c.name[:-5].split('_')
        level = parts[1].lower()

        if not include_all_versions:
            version = int(parts[4].lower().replace("v", ""))
            if version not in include_versions:
                continue

        last_mod = c.stat().st_ctime

        # should the level by published
        if level not in include_levels:
            continue

        # is the waiting time after last modification done
        if (now - last_mod) < wait_period_s:
            continue

        if not include_all_products:
            product = str(parts[2].lower().split("-")[1])
            if product not in include_products:
                continue

        old = hist.find_by_name(c.name)

        # was the same file already published
        if len(old) > 0:
            # as the added keyword by publishing will also change the mtime
            # we do not publish again - just with new name (version)
            # old = old[0]
            # if old['m_date'] == last_mod:
            continue

        to_publish.append(c)

    logger.info(f'#candidates: {n_candidates}')
    logger.info(f'#to_publish: {len(to_publish)}')

    if args.sort_files:
        to_publish = sorted(to_publish)

    published = defaultdict(list)
    for p in to_publish:
        try:
            comment = addBSDComment(p, rid_lut)
            add_res, data = hist.add(p)

            if p.name in blacklist_files:
                hist.set_result(PublishResult.BLACKLISTED, data[0]["id"])
                published[PublishResult.BLACKLISTED].append(data)
            elif add_res == PublishConflicts.ADDED:
                ok, error = copyFile(scp, p, target_dir)
                if ok:
                    hist.set_result(PublishResult.PUBLISHED, data[0]["id"])
                    published[PublishResult.PUBLISHED].append(data)
                else:
                    hist.set_result(PublishResult.ERROR, data[0]["id"])
                    published[PublishResult.ERROR].append((p, error))
            elif add_res == PublishConflicts.SAME_EXISTS:
                # do nothing but add to report
                published[PublishResult.IGNORED].append(data)
            elif add_res == PublishConflicts.SAME_ESA_NAME:
                # esa naming conflict
                equal_data_once = False
                new_entry_id = None
                for f in data:
                    # do not test against itself
                    if p.name == f['name']:
                        new_entry_id = f['id']
                        continue
                    with fits.open(p) as a:
                        with fits.open(Path(f['path']) / f['name']) as b:
                            diff = HDUDiff(a["DATA"], b["DATA"], ignore_keywords=['*'])

                            if diff.identical:
                                equal_data_once = True
                                break

                # ignore re-requested data
                if equal_data_once:  # and new_entry_id:
                    hist.set_result(PublishResult.IGNORED, new_entry_id)
                    data[-1]['result'] = PublishResult.IGNORED
                    published[PublishResult.IGNORED].append(data)
                else:
                    # cases that covers the same time period but other configurations (pixel sets)

                    sup_nr = len([1 for d in data if d['modified_esaname']]) + 1

                    # only allow vor sup1 and sup2
                    if sup_nr > 2:
                        shutil.copy(p, same_esa_name_dir)
                        hist.set_result(PublishResult.IGNORED, new_entry_id)
                        published[PublishResult.ERROR].append((p, "max supplement error"))
                    else:
                        shutil.copy(p, tempdir_path)
                        old_name = p.name
                        parts = old_name.split('_')
                        parts[2] += f"-sup{sup_nr}"
                        new_name = '_'.join(parts)

                        os.rename(tempdir_path / old_name, tempdir_path / new_name)
                        parent_comment = f"supplement data product combine with: {data[0]['name']}"
                        fits.setval(tempdir_path / new_name, 'COMMENT', value=parent_comment)
                        if args.supplement_report:
                            supplement_report.add_row([new_name, data[0]['name'], comment])

                        ok, error = copyFile(scp, tempdir_path / new_name, target_dir)
                        if ok:
                            hist.set_result(PublishResult.MODIFIED, new_entry_id)
                            data[-1]['result'] = PublishResult.MODIFIED

                            # also set the history entry in the header of the orig file
                            # to the temporary files it was already added by copyFile
                            addHistory(p, new_name)

                            modified_esaname = '_'.join(parts[:-1]) + ".fits"
                            hist.set_modified_esa_name(modified_esaname, new_entry_id)
                            data[-1]['modified_esaname'] = modified_esaname
                            published[PublishResult.MODIFIED].append(data)
                        else:
                            hist.set_result(PublishResult.ERROR, data[0]["id"])
                            published[PublishResult.ERROR].append((p, error))

            logger.info(f"processed file: {data} > {str(add_res)}")
        except Exception as e:
            published[PublishResult.ERROR].append((p, e))
            logger.error(e, exc_info=True)

    send_mail_report(published)
    if args.supplement_report:
        # append the new report
        with open(args.supplement_report, mode='a') as f:
            f.seek(0, os.SEEK_END)
            supplement_report.write(f, format='ascii.no_header', delimiter=",")
    hist.close()
    tempdir.cleanup()
    return published


if __name__ == '__main__':
    publish_fits_to_esa(sys.argv[1:])
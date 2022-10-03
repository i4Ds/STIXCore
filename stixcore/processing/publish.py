
import sys
import shutil
import smtplib
import sqlite3
import argparse
from enum import Enum
from pprint import pformat
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import astropy.units as u

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger

__all__ = ['PublishConflicts', 'PublishHistoryStorage', 'publish_fits_to_esa']

logger = get_logger(__name__)


class PublishConflicts(Enum):
    ADDED = 0
    SAME_ESA_NAME = 1
    SAME_EXISTS = 2


class PublishHistoryStorage:
    def __init__(self, filename):
        self.conn = None
        self.cur = None
        self.filename = filename
        self._connect_database()

    def _connect_database(self):
        try:
            self.conn = sqlite3.connect(self.filename)
            self.cur = self.conn.cursor()
            logger.info('PublishHistory DB loaded from {}'.format(self.filename))

            sql = '''CREATE TABLE if not exists published (
                        name TEXT PRIMARY KEY,
                        path TEXT,
                        version INTEGER,
                        p_date FLOAT,
                        m_date FLOAT,
                        esaname TEXT
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
        """Is the reader connected to the IDB.

        returns
        -------
        True | False
        """
        if self.cur:
            return True
        return False

    def count(self):
        return self.cur.execute("select count(1) from published").fetchone()[0]

    def add(self, path):
        name = path.name
        dir = str(path.parent)
        date = datetime.now().timestamp()
        parts = name[:-5].split('_')
        version = int(parts[4].lower().replace("v", ""))
        esa_name = '_'.join(parts[0:5])
        m_date = path.stat().st_mtime
        try:
            self.cur.execute("insert into published values (?, ?, ?, ?, ?, ?)",
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
        return self._execute("select * from published where name = ?", (name, ), result_type='hash')

    def find_by_esa_name(self, esa_name):
        return self._execute("select * from published where esaname = ?", (esa_name, ),
                             result_type='hash')

    def get_all(self):
        return self._execute("select * from published")


def send_mail_report(files):
    if CONFIG.getboolean('Publish', 'report_mail_send', fallback=False):
        try:
            sender = CONFIG.get('Pipeline', 'error_mail_sender', fallback='')
            receivers = CONFIG.get('Publish', 'report_mail_receivers').split(",")
            host = CONFIG.get('Pipeline', 'error_mail_smpt_host', fallback='localhost')
            port = CONFIG.getint('Pipeline', 'error_mail_smpt_port', fallback=25)
            smtp_server = smtplib.SMTP(host=host, port=port)
            summary = (f"E {len(files['failed'])} "
                       f"C {len(files[PublishConflicts.SAME_EXISTS])} "
                       f"D {len(files[PublishConflicts.SAME_ESA_NAME])} "
                       f"A {len(files[PublishConflicts.ADDED])}")
            error = "" if 'failed' not in files else "ERROR-"
            message = f"""Subject: StixCore TMTC Publishing {error}Report {summary}

ERRORS
******
{files['failed']}

PUBLISHED DUPLICATES
********************
{pformat(files[PublishConflicts.SAME_EXISTS])}

PUBLISHED ESA DUPLICATES
************************
{pformat(files[PublishConflicts.SAME_ESA_NAME])}

PUBLISHED
*********
{pformat(files[PublishConflicts.ADDED])}


StixCore

==========================

do not answer to this mail.
"""
            smtp_server.sendmail(sender, receivers, message)
        except Exception as e:
            logger.error(f"Error: unable to send report email: {e}")


def publish_fits_to_esa(args):

    parser = argparse.ArgumentParser(description='stix pipeline processing')

    # pathes
    parser.add_argument("-t", "--target_dir",
                        help="target directory where fits files should be copied to",
                        default=CONFIG.get('Publish', 'target_dir'), type=str)

    parser.add_argument("-d", "--db_file",
                        help="Path to the history publishing database", type=str,
                        default=CONFIG.get('Publish', 'db_file',
                                           fallback=str(Path.home() / "published.sqlite")))

    parser.add_argument("-w", "--waiting_period",
                        help="how long to wait after last file modification before publishing",
                        default=CONFIG.get('Publish', 'waiting_period', fallback="14d"), type=str)

    parser.add_argument("-i", "--include_levels",
                        help="what levels should be published", type=str,
                        default=CONFIG.get('Publish', 'include_levels', fallback="L0, L1, L2"))

    parser.add_argument("-f", "--fits_dir",
                        help="input FITS directory for files to publish ",
                        default=CONFIG.get('Paths', 'fits_archive'), type=str)

    args = parser.parse_args(args)

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

    include_levels = dict([(level, 1) for level in
                           args.include_levels.lower().replace(" ", "").split(",")])

    candidateds = fits_dir.rglob("solo_*.fits")
    to_publish = list()
    now = datetime.now().timestamp()
    hist = PublishHistoryStorage(db_file)

    for c in candidateds:
        parts = c.name[:-5].split('_')
        level = parts[1].lower()
        # TODO filter for certain products similar like level
        # product = str(parts[2].lower().replace("stix-", ""))
        # TODO filter for certain versions similar like level
        # version = int(parts[4].lower().replace("v", ""))
        last_mod = c.stat().st_mtime

        # should the level by published
        if level not in include_levels:
            continue

        # is the waiting time after last modification done
        if (now - last_mod) < wait_period_s:
            continue

        old = hist.find_by_name(c.name)

        # was the same file already published
        if len(old) == 1:
            old = old[0]
            if old['m_date'] == last_mod:
                continue

        to_publish.append(c)

    published = defaultdict(list)
    for p in to_publish:
        try:
            add_res, data = hist.add(p)
            shutil.copy(p, target_dir)
            published[add_res].append(data)
            logger.info(f"published file: {data} > {str(add_res)}")
        except Exception as e:
            published['failed'].append((p, e))
            logger.error(e, exc_info=True)

    send_mail_report(published)
    hist.close()


if __name__ == '__main__':
    publish_fits_to_esa(sys.argv[1:])

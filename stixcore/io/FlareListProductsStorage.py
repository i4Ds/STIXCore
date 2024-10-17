import sqlite3
from datetime import datetime

from stixcore.util.logging import get_logger

logger = get_logger(__name__)


class ProcessingHistoryStorage:
    """Persistent handler for meta data on already processed Products"""

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
            logger.info('ProcessingHistoryStorage DB loaded from {}'.format(self.filename))

            self.cur.execute('''CREATE TABLE if not exists processed_flare_products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        flareid TEXT NOT NULL,
                        flarelist TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        ssid INTEGER NOT NULL,
                        fitspath TEXT NOT NULL,
                        p_date FLOAT NOT NULL
                    )
            ''')
            self.cur.execute('''CREATE INDEX if not exists processed_flare_products_idx ON
                                processed_flare_products (flareid, flarelist, version, ssid)''')

            self.cur.execute('''CREATE TABLE if not exists processed_fits_products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        level TEXT NOT NULL,
                        type TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        fits_in_path TEXT NOT NULL,
                        fits_out_path TEXT NOT NULL,
                        p_date TEXT NOT NULL
                    )
            ''')

            self.cur.execute('''CREATE INDEX if not exists processed_fits_products_idx ON
                                processed_fits_products
                                (name, level, type, version, fits_in_path)''')

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

    def count_processed_flare_products(self):
        """Counts the number of already processed flares in the DB

        Returns
        -------
        int
            number of processed flares
        """
        return self.cur.execute("select count(1) from processed_flare_products").fetchone()[0]

    def count_processed_fits_products(self):
        """Counts the number of entries in the DB

        Returns
        -------
        int
            number of tracked files
        """
        return self.cur.execute("select count(1) from processed_fits_products").fetchone()[0]

    def add_processed_fits_products(self, name, level, type, version,
                                    fits_in_path, fits_out_path, p_date):
        """Adds a new file to the history of processed fits files.

        Parameters
        ----------
        product : stix data product
        TODO

        """
        df = p_date.isoformat()
        in_path = str(fits_in_path)
        out_path = str(fits_out_path)

        self.cur.execute("""insert into processed_fits_products
                            (name, level, type, version, fits_in_path, fits_out_path, p_date)
                            values(?, ?, ?, ?, ?, ?, ?)""",
                         (name, level, type, version, in_path, out_path, df))
        logger.info(f"""insert into processed_fits_products
                            (name: '{name}', level: '{level}', type: '{type}',
                             version: '{version}', fits_in_path: '{in_path}',
                             fits_out_path: '{out_path}', p_date: '{df}')""")

    def has_processed_fits_products(self, name, level, type, version,
                                    fits_in_path, fits_in_create_time):
        """
        TODO

        """
        return self.cur.execute("""select count(1) from processed_fits_products where
                            fits_in_path = ? and
                            name = ? and
                            level = ? and
                            type = ? and
                            version = ? and
                            p_date > ?
                         """, (str(fits_in_path), name, level, type, version,
                               fits_in_create_time.isoformat())).fetchone()[0] > 0

    def add_processed_flare_products(self, product, path, flarelistid, flarelistname):
        """Adds a new file to the history of processed flares.

        Parameters
        ----------
        product : stix data product
        TODO

        """
        fitspath = str(path)
        p_date = datetime.now().timestamp()
        ssid = product.ssid
        version = product.version

        self.cur.execute("""insert into processed_flare_products
                            (flareid, flarelist, version, ssid, fitspath, p_date)
                            values(?, ?, ?, ?, ?, ?)""",
                         (flarelistid, flarelistname, version, ssid, fitspath, p_date))

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

    def get_all(self):
        """Queries all entries

        Returns
        -------
        `list`
            all found entries.
        """
        return self._execute("select * from published")

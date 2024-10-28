import sqlite3
from pathlib import Path
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

            # TODO reactivate later
            # self.cur.execute('''CREATE TABLE if not exists processed_flare_products (
            #             id INTEGER PRIMARY KEY AUTOINCREMENT,
            #             flareid TEXT NOT NULL,
            #             flarelist TEXT NOT NULL,
            #             version INTEGER NOT NULL,
            #             name TEXT NOT NULL,
            #             level TEXT NOT NULL,
            #             type TEXT NOT NULL,
            #             fitspath TEXT NOT NULL,
            #             p_date FLOAT NOT NULL
            #         )
            # ''')
            # self.cur.execute('''CREATE INDEX if not exists processed_flare_products_idx ON
            #                     processed_flare_products (flareid, flarelist, version, name,
            # level, type)''')

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

    # TODO reactivate later
    # def count_processed_flare_products(self):
    #     """Counts the number of already processed flares in the DB

    #     Returns
    #     -------
    #     int
    #         number of processed flares
    #     """
    #     return self.cur.execute("select count(1) from processed_flare_products").fetchone()[0]

    def count_processed_fits_products(self) -> int:
        """Counts the number of entries in the DB

        Returns
        -------
        int
            number of tracked files
        """
        return self.cur.execute("select count(1) from processed_fits_products").fetchone()[0]

    def add_processed_fits_products(self, name: str, level: str, type: str, version: int,
                                    fits_in_path: Path, fits_out_path: Path, p_date: datetime):
        """Adds a new file to the history of processed fits files.

        Parameters
        ----------
        name : str
            name of the generated product
        level : str
            level of the generated product
        type : str
            type of the generated product
        version : int
            version of the generated product
        fits_in_path : Path
            the path to the input file
        fits_out_path : Path
            the path of the generated product
        p_date : datetime
            processing time
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

    def has_processed_fits_products(self, name: str, level: str, type: str, version: int,
                                    fits_in_path: Path, fits_in_create_time: datetime) -> bool:
        """Checks if an entry for the given input file and target product exists and if
        the processing was before the given datetime.

        Parameters
        ----------
        name : str
            name of the generated product
        level : str
            level of the generated product
        type : str
            type of the generated product
        version : int
            version of the generated product
        fits_in_path : Path
            the path to the input file
        fits_in_create_time : datetime
            the time the input file was created/updated

        Returns
        -------
        bool
            Was a processing already registered and was it later as the fileupdate time
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

    # TODO reactivate later
    # def add_processed_flare_products(self, product, path, flarelistid, flarelistname):
    #     fitspath = str(path)
    #     p_date = datetime.now().timestamp()
    #     ssid = product.ssid
    #     version = product.version

    #     self.cur.execute("""insert into processed_flare_products
    #                         (flareid, flarelist, version, ssid, fitspath, p_date)
    #                         values(?, ?, ?, ?, ?, ?)""",
    #                      (flarelistid, flarelistname, version, ssid, fitspath, p_date))

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

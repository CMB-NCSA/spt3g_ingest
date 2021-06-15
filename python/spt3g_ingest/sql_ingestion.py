
from astropy.io import fits
import sqlite3
from spt3g_ingest.data_types import Fd
import logging

logger = logging.getLogger(__name__)

# SQL string definitions
# Create SQL statement to create table automatically
_table_statement = ''
for k in Fd.keys():
    _table_statement += '{} {},\n'.format(k, Fd[k])
# remove last comma
_table_statement = _table_statement.rstrip(',\n')


def read_header(fitsfile):
    if fitsfile.endswith('.fz'):
        hdu = 1
    else:
        hdu = 0
    F = fits.open(fitsfile)
    header = F[hdu].header
    return header


def connect_db(dbname, tablename='FILE_INFO_V0'):
    """Establisih connection to DB"""

    logger.info(f"Establishing DB connection to: {dbname}")

    # Connect to DB
    # SQLlite DB lives in a file
    con = sqlite3.connect(dbname)

    # Create the table
    create_table = """
    CREATE TABLE IF NOT EXISTS {tablename} (
    {statement}
    )
    """.format(**{'tablename': tablename, 'statement': _table_statement})
    logger.debug(create_table)

    cur = con.cursor()
    cur.execute(create_table)
    con.commit()
    return con


def ingest_files(files, tablename, con=None, dbname=None):
    """ Task to ingest a bunch of files into a sqlite3 table"""

    # Make new connection if not available
    if not con:
        con = sqlite3.connect(dbname)

    # Get cursor
    cur = con.cursor()

    insert_row = """
    INSERT INTO {tablename} values ({values})
    """

    for idx in range(len(files)):

        fitsfile = files[idx]
        logger.info(f"Ingesting {fitsfile} to: {tablename}")

        header = read_header(fitsfile)

        # Create the ingested values in the same order,
        # starting for those 3 keys by hand
        values = []
        values.append(str(idx))  # INGESTION_ID
        values.append(str(fitsfile))  # FILENAME
        values.append(str(fitsfile+'.spt'))  # PARENT FILE

        for k in Fd.keys():
            k = k.replace('_', '-')  # SQL doesn't like column names with -
            try:
                values.append(str(header[k]))
            except KeyError:
                pass
                # print('{} Not Found'.format(k))

        # Convert the values into a long string
        vvv = ''
        for v in values:
            vvv += '\"' + v + '\", '
        vvv = vvv.rstrip(', ')

        query = insert_row.format(**{'tablename': tablename, 'values': vvv})
        logger.debug(f"Executing:{query}")
        cur.execute(query)
        logger.info(f"Ingestion Done for: {fitsfile}")
        con.commit()
    # con.close()

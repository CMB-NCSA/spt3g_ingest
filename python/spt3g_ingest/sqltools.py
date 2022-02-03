
import os
import astropy
from astropy.time import Time
import sqlite3
from spt3g_ingest.data_types import Fd
import logging
import re

logger = logging.getLogger(__name__)

# SQL string definitions
# Create SQL statement to create table automatically
_table_statement = ''
for k in Fd.keys():
    _table_statement += '{} {},\n'.format(k, Fd[k])
# remove last comma
_table_statement += 'UNIQUE(ID) '
_table_statement = _table_statement.rstrip(',\n')

# Template to insert a row
_insert_row = """
INSERT{or_replace}INTO {tablename} values ({values})
"""


def read_header(fitsfile):
    """
    Read in the FITS file header using astropy.fits
    """
    is_compressed = False
    with astropy.io.fits.open(fitsfile) as fits:
        for k in range(len(fits)):
            h = fits[k]._header
            if not h.get('ZIMAGE'):
                continue
            if h['ZIMAGE'] is True:
                is_compressed = True
                continue

        if is_compressed:
            hdu = 1
        else:
            hdu = 0
        header = fits[hdu].header

    return header


def fix_fits_keywords(header):
    """
    Update header keyword to change '-' by '_' as columns with '-' are not
    allowed on SQL
    """
    new_header = {}
    for key in header.keys():
        new_key = key.replace('-', '_')
        new_header[new_key] = header[key]

    # Temporary fix - needs to be removed
    # Making it backwards complatible with older files.
    # Check the FILETYPE is present, if not get from filename
    if 'FILETYPE' not in header.keys():
        logger.warning("Adding FILETYPE from FITSNAME pattern to header to compatibility")
        # Try to get it from the filename
        if re.search('_passthrough.fits', header['FITSNAME']):
            new_header['FILETYPE'] = 'psth'
        elif re.search('_fltd.fits', header['FITSNAME']):
            new_header['FILETYPE'] = 'filtered'
        # For headers without FILETYPE (i.e.: yearly) we set it to raw
        else:
            raise Exception("ERROR: Cannot provide suitable FILETYPE from header or pattern")
        logger.warning(f"Added FILETYPE {new_header['FILETYPE']} from pattern")

    return new_header


def extract_values_header(header):

    # Create the ingested values in the same order,
    # starting for those 3 keys by hand
    values = []
    for k in Fd.keys():
        try:
            values.append(str(header[k]))
        except KeyError:
            pass
            logger.debug('{} Not Found'.format(k))
    return values


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


def check_dbtable(dbname, tablename):
    """ Check tablename exists in database"""
    logger.info(f"Checking {tablename} exits in: {dbname}")
    # Connect to DB
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
    con.close()
    return


def ingest_fitsfile(fitsfile, tablename, con=None, dbname=None, replace=False):
    """ Ingest file into an sqlite3 table"""

    # Make new connection if not available
    if not con:
        close_con = True
        con = sqlite3.connect(dbname)
    else:
        close_con = False

    # Get cursor
    cur = con.cursor()

    if replace:
        or_replace = ' OR REPLACE '
    else:
        or_replace = ' '

    logger.info(f"Ingesting: {fitsfile} to: {tablename}")

    # Read in the header
    header = read_header(fitsfile)
    # Fix the keywords in the header
    header = fix_fits_keywords(header)

    # Extra metadata for ingestion
    ID = os.path.basename(fitsfile).split('.fits')[0]
    FILENAME = os.path.basename(fitsfile)
    FILEPATH = os.path.dirname(fitsfile)
    INGESTION_DATE = Time.now().isot

    # Create the ingested values in the same order,
    # starting for those 3 keys by hand
    values = []
    values.append(ID)  # ID
    values.append(FILENAME)  # FILENAME
    values.append(FILEPATH)  # FILEPATH
    values.append(INGESTION_DATE)  # INGESTION_DATE
    values = values + extract_values_header(header)

    # Convert the values into a long string
    vvv = ''
    for v in values:
        vvv += '\"' + v + '\", '
    vvv = vvv.rstrip(', ')

    query = _insert_row.format(**{'or_replace': or_replace,
                                  'tablename': tablename, 'values': vvv})

    logger.debug(f"Executing:{query}")
    try:
        cur.execute(query)
        con.commit()
        logger.info(f"Ingestion Done for: {fitsfile}")
    except sqlite3.IntegrityError:
        logger.warning(f"NOT UNIQUE: ingestion failed for {fitsfile}")

    if close_con:
        con.close()

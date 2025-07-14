
import os
import astropy
from astropy.time import Time
import sqlite3
import spt3g_ingest.data_types as data_types
import logging
import re
import time
import hashlib
import pandas as pd

logger = logging.getLogger(__name__)

# Template to insert a row
_insert_row = """
INSERT{or_replace}INTO {tablename} values ({values})
"""


def make_table_statement(Fd):
    # SQL string definitions
    # Create SQL statement to create table automatically
    table_statement = ''
    for k in Fd.keys():
        table_statement += '{} {},\n'.format(k, Fd[k])
    # remove last comma
    table_statement += 'UNIQUE(ID) '
    table_statement = table_statement.rstrip(',\n')
    return table_statement


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


def extract_values_header(header, Fd=data_types.Fd):

    # Create the ingested values in the same order,
    # starting for those 3 keys by hand
    values = []
    for k in Fd.keys():
        try:
            values.append(str(header[k]))
        except KeyError:
            # These 3 values are now missing from the headers
            if k in ['DATEREF', 'MJDREFI', 'MJDREFF']:
                values.append(str(None))
            else:
                pass
                logger.debug('{} Not Found'.format(k))
    return values


def create_con(dbname, read_only=False):
    """Establish connection to DB"""
    logger.info(f"Establishing DB connection to: {dbname}")
    if read_only:
        con = sqlite3.connect(f'file:{dbname}?mode=ro', uri=True)
    else:
        con = sqlite3.connect(dbname)
    return con


def connect_db(dbname, tablename='FILE_INFO_V0', Fd=data_types.Fd):
    """Establisih connection to DB"""
    logger.info(f"Establishing DB connection to: {dbname}")
    con = sqlite3.connect(dbname)
    # Create the table if does not exist
    check_dbtable(dbname, tablename, Fd=Fd)
    return con


def check_dbtable(dbname, tablename, con=None, Fd=data_types.Fd):
    """ Check if tablename exists in database"""
    logger.info(f"Checking if table: {tablename} exits in DB: {dbname}")
    # Connect to DB, make new connection if not available
    if not con:
        close_con = True
        con = sqlite3.connect(dbname)
    else:
        close_con = False

    con = sqlite3.connect(dbname)
    # Create the table
    table_statement = make_table_statement(Fd)
    create_table = """
    CREATE TABLE IF NOT EXISTS {tablename} (
    {statement}
    )
    """.format(**{'tablename': tablename, 'statement': table_statement})
    logger.debug(create_table)

    cur = con.cursor()
    cur.execute(create_table)
    con.commit()
    if close_con:
        con.close()
    return


def ingest_fitsfile(fitsfile, tablename, dbname=None, replace=False):
    """ Ingest file into an sqlite3 table"""

    if replace:
        or_replace = ' OR REPLACE '
    else:
        or_replace = ' '

    logger.info(f"Ingesting: {fitsfile} to: {tablename}")

    # Read in the header -- we get it in astropy format
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
    commit_with_retry(fitsfile, query, dbname, max_retries=10)


def ingest_g3file(g3file, header, tablename, dbname=None, replace=False, dryrun=False):
    """ Ingest file into an sqlite3 table"""

    # Repack header to astropy format
    for key in header.keys():
        header[key] = header[key][0]

    if replace:
        or_replace = ' OR REPLACE '
    else:
        or_replace = ' '

    g3file_short = header['FILENAME']
    logger.info(f"Ingesting: {g3file_short} to: {tablename}")

    # Extra metadata for ingestion
    INGESTION_DATE = Time.now().isot

    header['INGESTION_DATE'] = INGESTION_DATE
    header['FILETYPE'] = 'rawmap'

    # Get file size and md5sum
    header['SIZEINBYTES'], header['MD5SUM'] = compute_md5_and_size(g3file)

    # Replace '-' with "_"
    header = fix_fits_keywords(header)

    # Create the ingested values in the same order,
    # starting for those 3 keys by hand
    values = []
    values = values + extract_values_header(header, Fd=data_types.g3Fd)

    # Convert the values into a long string
    vvv = ''
    for v in values:
        vvv += '\"' + v + '\", '
    vvv = vvv.rstrip(', ')

    query = _insert_row.format(**{'or_replace': or_replace,
                                  'tablename': tablename, 'values': vvv})
    logger.debug(f"Executing:{query}")
    if dryrun:
        logger.info(f"DRYRUN: {query}")
    else:
        commit_with_retry(g3file, query, dbname, max_retries=10)


def compute_md5_and_size(g3file):
    """Compute full path, size, and MD5 for one file."""

    if not os.path.isfile(g3file):
        raise FileNotFoundError(f"File not found: {g3file}")

    size = os.path.getsize(g3file)
    md5 = hashlib.md5()
    with open(g3file, 'rb') as f:
        while chunk := f.read(8192):
            md5.update(chunk)
    return size, md5.hexdigest()


def commit_with_retry(g3file, query, dbname, max_retries=3, retry_delay=1):
    for attempt in range(max_retries):
        try:
            con = sqlite3.connect(dbname)
            cursor = con.cursor()
            cursor.execute(query)
            con.commit()
            logger.info(f"Ingestion Done for: {g3file}")
            return
        except sqlite3.IntegrityError:
            logger.warning(f"NOT UNIQUE: ingestion failed for {g3file}")
            return
        except sqlite3.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"WARNING: ingestion {attempt}/{max_retries} failed "
                               f"for:{g3file} -- will retry in {retry_delay}[sec]")
                time.sleep(retry_delay)
            else:
                raise e
        finally:
            if con:
                con.close()


def query_with_retry(query, dbname, max_retries=3, retry_delay=1):
    for attempt in range(max_retries):
        try:
            con = sqlite3.connect(dbname)
            df = pd.read_sql_query(query, con)
            return df
        except sqlite3.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"WARNING: query: {query} failed "
                               f"-- will retry in {retry_delay}[sec]")
                time.sleep(retry_delay)
            else:
                raise e
        finally:
            if con:
                con.close()


def get_query_field_seasons(tablename, files=[]):
    """
    Format query template to get field and seasons
    """

    query_files_template = """
    select FILENAME, BAND, FIELD, SEASON from {tablename}
      where
       {in_files}
    """
    # Get just the basename of the files
    basefiles = [os.path.basename(f) for f in files]
    # Formatting in_files
    filelist = ','.join("\'{}\'".format(s) for s in basefiles)
    in_files = f"FILENAME in ({filelist})"
    kw = {'tablename': tablename,
          'in_files': in_files}
    return query_files_template.format(**kw)

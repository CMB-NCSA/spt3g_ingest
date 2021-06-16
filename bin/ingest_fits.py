#!/usr/bin/env python

import argparse
import logging
import time
from spt3g_ingest import ingstools
from spt3g_ingest import sqltools


def cmdline():

    parser = argparse.ArgumentParser(description="FITS ingestion tool")
    parser.add_argument("files", nargs='+',
                        help="Filename(s) to ingest")
    # Ingest options
    parser.add_argument("--tablename", action='store', default="file_info_v0",
                        help="Table name with file infomation")
    parser.add_argument("--dbname", action='store', default="/data/spt3g/dblib/spt3g.db",
                        help="Name of the sqlite3 database file")

    # Logging options (loglevel/log_format/log_format_date)
    default_log_format = '[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s][%(funcName)s] %(message)s'
    default_log_format_date = '%Y-%m-%d %H:%M:%S'
    parser.add_argument("--loglevel", action="store", default='INFO', type=str.upper,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging Level [DEBUG/INFO/WARNING/ERROR/CRITICAL]")
    parser.add_argument("--log_format", action="store", type=str, default=default_log_format,
                        help="Format for logging")
    parser.add_argument("--log_format_date", action="store", type=str, default=default_log_format_date,
                        help="Format for date section of logging")
    args = parser.parse_args()
    return args


if __name__ == "__main__":

    # Get the command-line arguments
    args = cmdline()
    # Create logger
    ingstools.create_logger(level=args.loglevel,
                            log_format=args.log_format,
                            log_format_date=args.log_format_date)

    logger = logging.getLogger(__name__)

    # Prepare DB in case we want to ingeste
    con = sqltools.connect_db(args.dbname, args.tablename)

    # Loop over all of the files
    t0 = time.time()
    for fitsfile in args.files:
        sqltools.ingest_fitsfile(fitsfile, args.tablename, con=con)
        logger.info(f"Total time: {ingstools.elapsed_time(t0)}")

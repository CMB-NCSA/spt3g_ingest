#!/usr/bin/env python

import argparse
import os
import sys
import logging
import time
from spt3g_ingest import ingstools
from spt3g_ingest import sqltools


def cmdline():

    parser = argparse.ArgumentParser(description="spt3g ingestion tool")
    parser.add_argument("files", nargs='+',
                        help="Filename(s) to ingest")
    parser.add_argument("--outdir", type=str, action='store', default=None,
                        required=True, help="Location for output files")
    parser.add_argument("--clobber", action='store_true', default=False,
                        help="Clobber output files")
    parser.add_argument("--compress", action='store_true', default=False,
                        help="Compress (gzip) output files")
    parser.add_argument("--fpack", action='store_true', default=False,
                        help="Fpack output fits file")
    parser.add_argument("--fpack_options", action="store", default='-g2',
                        help="Fpack options")

    # Ingest options
    parser.add_argument("--ingest", action='store_true', default=False,
                        help="Ingest files")
    parser.add_argument("--replace", action='store_true', default=False,
                        help="Replace ingest entry")
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

    # Make sure we do --compress or --fpack
    if args.compress and args.fpack:
        sys.exit("ERROR: cannot use both --compress and -fpack")

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
    if args.ingest:
        con = sqltools.connect_db(args.dbname, args.tablename)

    # Loop over all of the files
    t0 = time.time()
    k = 1
    nfiles = len(args.files)
    for g3file in args.files:

        logger.info(f"Doing: {k}/{nfiles} files")

        basename = ingstools.get_g3basename(g3file)
        # compress in an option of convert_to_fits (spt3g software)
        if args.compress:
            fitsfile = os.path.join(args.outdir, basename+".fits.gz")
        else:
            fitsfile = os.path.join(args.outdir, basename+".fits")

        if args.fpack:
            if os.path.isfile(fitsfile+'.fz') and not args.clobber:
                logger.warning(f"Skipping: {g3file} -- file exists: {fitsfile}.fz")
            else:
                ingstools.convert_to_fits(g3file, fitsfile,
                                          overwrite=args.clobber,
                                          compress=args.compress)
                ingstools.run_fpack(fitsfile, fpack_options=args.fpack_options)

            fitsfile = fitsfile + ".fz"
        else:
            ingstools.convert_to_fits(g3file, fitsfile,
                                      overwrite=args.clobber,
                                      compress=args.compress)

        if args.ingest:
            sqltools.ingest_fitsfile(fitsfile, args.tablename, con=con, replace=args.replace)

        logger.info(f"Total time: {ingstools.elapsed_time(t0)}")
        k += 1

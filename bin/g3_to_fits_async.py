#!/usr/bin/env python3

import argparse
import os
import sys
import logging
import time
from spt3g_ingest import ingstools
from spt3g_ingest import sqltools
import multiprocessing as mp
from spt3g import core, maps, transients


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

    # Filter options
    parser.add_argument("--filter_transient", action='store_true', default=False,
                        help="Filter daily maps for transients")
    parser.add_argument("--mask", action="store", default=None,
                        help="Input mask for filter transients")
    parser.add_argument("--coadd", nargs="*", default=None,
                        help="Name of the coadd filenames (g3)")
    parser.add_argument("--band", nargs="*", default=['90GHz', '150GHz', '220GHz'],
                        help="The bands to select from: 90GHz, 150GHz and 220GHz")
    parser.add_argument("--polarized", action="store_true", default=False,
                        help="Filter polarized maps.")

    # Ingest options
    parser.add_argument("--ingest", action='store_true', default=False,
                        help="Ingest files")
    parser.add_argument("--replace", action='store_true', default=False,
                        help="Replace ingest entry")
    parser.add_argument("--tablename", action='store', default="file_info_v1",
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

    # Use multiprocessing
    parser.add_argument("--np", action="store", default=1, type=int,
                        help="Run using multi-process, 0=automatic, 1=single-process [default]")

    args = parser.parse_args()

    # Make sure we do --compress or --fpack
    if args.compress and args.fpack:
        sys.exit("ERROR: cannot use both --compress and -fpack")

    # Some checks in case we want to filter for transients
    if args.filter_transient:
        # Check for mask
        if not args.mask:
            raise RuntimeError("Missing mask file")
        # Check that coadds has same number as bands
        if len(args.coadd) != len(args.band):
            raise RuntimeError("Missing coadd (g3) files")

    return args


def run_g3file_raw(g3file, hdr, args):
    """
    Function to call the raw dump of G3 files
    """

    basename = ingstools.get_g3basename(g3file)
    folder_date = os.path.join(args.outdir, ingstools.get_folder_date(hdr))
    fitsfile = os.path.join(folder_date, basename + ".fits")

    # compress in an option of convert_to_fits (spt3g software)
    if args.compress:
        fitsfile = fitsfile + ".gz"

    if args.fpack:
        if os.path.isfile(fitsfile+'.fz') and not args.clobber:
            logger.warning(f"Skipping: {g3file} -- file exists: {fitsfile}.fz")
        else:
            ingstools.convert_to_fits_raw(g3file, fitsfile,
                                          hdr=hdr,
                                          overwrite=args.clobber,
                                          compress=args.compress)
            ingstools.run_fpack(fitsfile, fpack_options=args.fpack_options)

        fitsfile = fitsfile + ".fz"
    else:
        ingstools.convert_to_fits_raw(g3file, fitsfile,
                                      hdr=hdr,
                                      overwrite=args.clobber,
                                      compress=args.compress)

    if args.ingest:
        con = sqltools.connect_db(args.dbname, args.tablename)
        sqltools.ingest_fitsfile(fitsfile, args.tablename, con=con, replace=args.replace)
        con.close()

    return


def run_g3file_filtd(g3file, hdr, args):
    """
    Function to call the filtering of G3 files
    """

    basename = ingstools.get_g3basename(g3file) + "_filtd"
    folder_date = os.path.join(args.outdir, ingstools.get_folder_date(hdr))
    fitsfile = os.path.join(folder_date, basename + ".fits")
    band = hdr['BAND'][0]

    # Populate additional metadata for DB
    hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')

    # compress in an option of convert_to_fits (spt3g software)
    if args.compress:
        fitsfile = fitsfile + ".gz"

    # Load the pipe with tasks
    pipe = filter_g3file(g3file, hdr, args)

    if args.fpack:
        if os.path.isfile(fitsfile+'.fz') and not args.clobber:
            logger.warning(f"Skipping: {g3file} -- file exists: {fitsfile}.fz")
        else:

            # This workflow needs to be re-written
            # Write as FITS file
            pipe.Add(maps.fitsio.SaveMapFrame, map_id=band, output_file=fitsfile,
                     compress=args.compress, overwrite=args.clobber, hdr=hdr)
            logger.info(f"Will create fitsfile: {fitsfile}")
            logger.info(f"Running Filtering pipe for {g3file}")
            t0 = time.time()
            pipe.Run(profile=True)
            del pipe
            logger.info(f"Total time: {ingstools.elapsed_time(t0)} for Filtering pipe {g3file}")
            # We fpack the file
            ingstools.run_fpack(fitsfile, fpack_options=args.fpack_options)

        fitsfile = fitsfile + ".fz"
    else:
        # This workflow needs to be re-written
        # Write as FITS file
        logger.info(f"Adding SaveMapFrame for: {fitsfile}")
        pipe.Add(maps.fitsio.SaveMapFrame, map_id=band, output_file=fitsfile,
                 compress=args.compress, overwrite=args.clobber, hdr=hdr)
        logger.info(f"Will create fitsfile: {fitsfile}")
        logger.info("Running Filtering pipe")
        t0 = time.time()
        pipe.Run(profile=True)
        del pipe
        logger.info(f"Total time: {ingstools.elapsed_time(t0)} for Filtering pipe {g3file}")

    if args.ingest:
        con = sqltools.connect_db(args.dbname, args.tablename)
        sqltools.ingest_fitsfile(fitsfile, args.tablename, con=con, replace=args.replace)
        con.close()

    return


def filter_g3file(g3file, hdr, args):
    "Run the actual filtering"

    # Create pipe
    pipe = core.G3Pipeline()
    pipe.Add(core.G3Reader, filename=g3file)
    band = hdr['BAND'][0]

    logger.info(f"Filtering {g3file} band: {band}")

    if args.coadd is not None:
        # Match the band of the coadd
        for map_id, data in args.g3coadds.items():
            if map_id == 'Coadd'+band:
                logger.info(f"Adding InjectMaps for {map_id}")
                pipe.Add(
                    maps.InjectMaps,
                    map_id=map_id,
                    maps_in=data,
                    ignore_missing_weights=True,
                )

    elif args.mask is not None:
        # mask has been injected in coadd.
        # this handles the no-coadd case
        logger.info(f"Adding mask InjectMaps for {args.mask_id}")
        pipe.Add(
            maps.InjectMaps,
            map_id=args.mask_id,
            maps_in={"T": args.g3mask},
            ignore_missing_weights=True,
        )
        # del args.g3mask

    if not args.polarized:
        pipe.Add(maps.map_modules.MakeMapsUnpolarized)

    # Add the TransientMapFiltering to the pipe
    logger.info(f"Adding TransientMapFiltering for {band}")
    pipe.Add(
        transients.TransientMapFiltering,
        bands=args.band,
        subtract_coadd=args.subtract_coadd,
        mask_id=args.mask_id,
    )

    return pipe


def run_g3file(g3file, k, args):

    """
    Function that contains all the run steps to be able to call
    using multiprocessing
    """

    # Let's time this
    t0 = time.time()
    nfiles = len(args.files)

    # Get logger
    logger = logging.getLogger(__name__)
    logger.info(f"Doing: {k}/{nfiles} files")

    # Get metadata from g3file
    hdr = ingstools.get_metadata(g3file, logger=logger)

    # Run the tasks
    run_g3file_raw(g3file, hdr, args)
    if args.filter_transient:
        run_g3file_filtd(g3file, hdr, args)

    logger.info(f"Completed: {k}/{nfiles} files")
    logger.info(f"Total time: {ingstools.elapsed_time(t0)} for: {g3file}")


def load_mask(args):
    """Load the mask for filter_transient routines"""

    t0 = time.time()
    logger.info(f"Reading mask file: {args.mask}")
    mask = None
    for frame in core.G3File(args.mask):
        if frame.type != core.G3FrameType.Map:
            continue
        if "Mask" in frame:
            mask = frame["Mask"]
        else:
            mask = frame["T"]
        break
    if mask is None:
        raise RuntimeError("Missing mask frame")
    mask_id = "Mask"
    logger.info(f"Total time reading {args.mask}: {ingstools.elapsed_time(t0)}")
    return mask, mask_id


def load_coadds(args):
    """
    Load coadd g3 files for transients filtering.
    If specified, apply mask to coadd to save memory
    """

    t0 = time.time()
    coadds = {}
    # Loop over all coadd files
    for g3coaddfile in args.coadd:
        logger.info(f"Reading coadd file(s): {g3coaddfile}")

        # Loop over frames
        for frame in core.G3File(g3coaddfile):
            logger.debug(f"Reading frame: {frame}")
            if frame.type != core.G3FrameType.Map:
                continue
            if frame["Id"] not in args.band:
                continue
            if not args.polarized:
                maps.map_modules.MakeMapsUnpolarized(frame)
            maps.map_modules.RemoveWeights(frame, zero_nans=True)
            del frame["Wunpol"]
            tmap = frame.pop("T")
            # apply mask
            if args.mask is not None:
                tmap *= args.g3mask
            #    del args.g3mask  -- no need to delete
            tmap.compact(zero_nans=True)
            coadds["Coadd{}".format(frame["Id"])] = {"T": tmap}

    subtract_coadd = True
    logger.info(f"Total time coadd read: {ingstools.elapsed_time(t0)}")
    return coadds, subtract_coadd


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


if __name__ == "__main__":

    # Keep time
    t0 = time.time()

    # Get the command-line arguments
    args = cmdline()
    # Create logger
    ingstools.create_logger(level=args.loglevel,
                            log_format=args.log_format,
                            log_format_date=args.log_format_date)

    logger = logging.getLogger(__name__)

    # Read in the mask and store in args
    if args.mask is not None:
        args.g3mask, args.mask_id = load_mask(args)
    else:
        args.g3maks = None   # Not sure we need this
        args.mask_id = None

    # Read in the coadds and store in args
    if args.coadd is not None:
        args.g3coadds, args.subtract_coadd = load_coadds(args)
        logger.info(f"Coadd keys:{args.g3coadds.keys()}")

    # Get the number of processors to use
    NP = ingstools.get_NP(args.np)

    # The number of files to process
    nfiles = len(args.files)

    if NP > 1:
        p = mp.Pool(processes=NP, maxtasksperchild=1)
        logger.info(f"Will use {NP} processors to convert and ingest")

    # Loop over all of the files
    k = 1
    t0 = time.time()
    for g3file in args.files:
        if NP > 1:
            kw = {}
            args.k = k
            fargs = (g3file, k, args)
            logger.info(f"apply_async for {g3file}")
            p.apply_async(run_g3file, fargs, kw)
        else:
            run_g3file(g3file, k, args)
        k += 1

    if NP > 1:
        p.close()
        p.join()

    logger.info(f"Grand Total time: {ingstools.elapsed_time(t0)}")

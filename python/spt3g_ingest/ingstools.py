# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from astropy.time import Time

LOGGER = logging.getLogger(__name__)

# Mapping of metadata to FITS keywords
_keywords_map = {'ObservationStart': ('DATE-BEG', 'Observation start date'),
                 'ObservationStop': ('DATE-END', 'Observation end date'),
                 'ObservationID': ('OBS-ID', 'Observation ID'),
                 'Id': ('BAND', 'Band name'),
                 'SourceName': ('OBJECT', 'Name of object'),
                 }


def extract_metadata_frame(frame, metadata=None, logger=None):
    """
    Extract selected metadata from a g3 frame
    """

    # Loop over all items and select only the ones in the Mapping
    if not metadata:
        metadata = {}
    for k in iter(frame):
        if k in _keywords_map.keys():
            keyword = _keywords_map[k][0]
            # Need to re-cast GETime objects
            if type(frame[k]) == core.G3Time:
                gtime = Time(frame[k].isoformat(), format='isot', scale='utc').isot
                metadata[keyword] = (gtime, _keywords_map[k][1])
            else:
                metadata[keyword] = (frame[k], _keywords_map[k][1])
    return metadata


def convert_to_fits(g3file, fitsfile=None, outpath='',
                    overwrite=True, compress=False, logger=None):

    if not logger:
        logger = LOGGER

    # Define fitsfile name only if undefined
    if fitsfile is None:
        basename = get_g3basename(g3file)
        fitsfile = os.path.join(outpath, f"{basename}.fits")

    # Skip if fitsfile exists and overwrite not True
    if os.path.isfile(fitsfile) and not overwrite:
        logger.warning(f"File exists, skipping: {fitsfile}")
        return

    g3 = core.G3File(g3file)
    logger.info(f"Loading: {g3file}")

    # Loop over to extract metadata, we can only loop once over the g3 object,
    # and this loop relies on Map being the last frame of the g3 object
    hdr = {}
    t0 = time.time()
    for frame in g3:

        # Extract metadata
        if frame.type == core.G3FrameType.Observation or frame.type == core.G3FrameType.Map:
            logger.info(f"Extracting metadata from frame: {frame.type}")
            hdr = extract_metadata_frame(frame, hdr)

        # Populate additional metadata for DB
        hdr['PARENT'] = (os.path.basename(g3file), 'Name of parent file')
        hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')

        # Convert to FITS
        if frame.type == core.G3FrameType.Map:
            logger.info(f"Transforming to FITS: {frame.type} -- Id: {frame['Id']}")
            maps.RemoveWeights(frame, zero_nans=True)
            maps.fitsio.save_skymap_fits(fitsfile, frame['T'], overwrite=overwrite,
                                         compress=compress, hdr=hdr)
            logger.info(f"Created: {fitsfile}")
            logger.info(f"Creation time: {elapsed_time(t0)}")

    return


def get_g3basename(g3file):
    """ Get the basename for a g3 file that could be compressed or not"""
    if os.path.splitext(g3file)[1] == '.gz':
        basename = os.path.splitext(os.path.splitext(g3file)[0])[0]
    else:
        basename = os.path.splitext(g3file)[0]
    return os.path.basename(basename)


def configure_logger(logger, logfile=None, level=logging.NOTSET, log_format=None, log_format_date=None):
    """
    Configure an existing logger
    """
    # Define formats
    if log_format:
        FORMAT = log_format
    else:
        FORMAT = '[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s][%(funcName)s] %(message)s'
    if log_format_date:
        FORMAT_DATE = log_format_date
    else:
        FORMAT_DATE = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(FORMAT, FORMAT_DATE)

    # Need to set the root logging level as setting the level for each of the
    # handlers won't be recognized unless the root level is set at the desired
    # appropriate logging level. For example, if we set the root logger to
    # INFO, and all handlers to DEBUG, we won't receive DEBUG messages on
    # handlers.
    logger.setLevel(level)

    handlers = []
    # Set the logfile handle if required
    if logfile:
        fh = RotatingFileHandler(logfile, maxBytes=2000000, backupCount=10)
        fh.setFormatter(formatter)
        fh.setLevel(level)
        handlers.append(fh)
        logger.addHandler(fh)

    # Set the screen handle
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.setLevel(level)
    handlers.append(sh)
    logger.addHandler(sh)
    return


def create_logger(logfile=None, level=logging.NOTSET, log_format=None, log_format_date=None):
    """
    Simple logger that uses configure_logger()
    """
    logger = logging.getLogger(__name__)
    configure_logger(logger, logfile=logfile, level=level,
                     log_format=log_format, log_format_date=log_format_date)
    logging.basicConfig(handlers=logger.handlers, level=level)
    logger.propagate = False
    return logger


def elapsed_time(t1, verb=False):
    """
    Returns the time between t1 and the current time now
    I can can also print the formatted elapsed time.
    ----------
    t1: float
        The initial time (in seconds)
    verb: bool, optional
        Optionally print the formatted elapsed time
    returns
    -------
    stime: float
        The elapsed time in seconds since t1
    """
    t2 = time.time()
    stime = "%dm %2.2fs" % (int((t2-t1)/60.), (t2-t1) - 60*int((t2-t1)/60.))
    if verb:
        print("Elapsed time: {}".format(stime))
    return stime

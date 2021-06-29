# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from astropy.time import Time
import subprocess
import multiprocessing

LOGGER = logging.getLogger(__name__)

# Mapping of metadata to FITS keywords
_keywords_map = {'ObservationStart': ('DATE-BEG', 'Observation start date'),
                 'ObservationStop': ('DATE-END', 'Observation end date'),
                 'ObservationID': ('OBS-ID', 'Observation ID'),
                 'Id': ('BAND', 'Band name'),
                 'SourceName': ('OBJECT', 'Name of object'),
                 }


def pre_populate_metadata(metadata=None):
    """ Pre-populate metadata dict with None if not defined"""

    if not metadata:
        metadata = {}

    # Populate all values with None if not present
    for k in _keywords_map.keys():
        keyword = _keywords_map[k][0]
        if keyword not in metadata:
            metadata[keyword] = (None, _keywords_map[k][1])
    return metadata


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
            # Need to re-cast G3Time objects
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

    # Pre-populate extra metadata that we will need
    hdr = pre_populate_metadata()
    # Populate additional metadata for DB
    hdr['PARENT'] = (os.path.basename(g3file), 'Name of parent file')
    hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')

    # Loop over to extract metadata, we can only loop once over the g3 object,
    # and this loop relies on Map being the last frame of the g3 object
    t0 = time.time()
    for frame in g3:

        # Extract metadata
        if frame.type == core.G3FrameType.Observation or frame.type == core.G3FrameType.Map:
            logger.info(f"Extracting metadata from frame: {frame.type}")
            hdr = extract_metadata_frame(frame, hdr)

        # Convert to FITS
        if frame.type == core.G3FrameType.Map:
            logger.info(f"Transforming to FITS: {frame.type} -- Id: {frame['Id']}")
            maps.RemoveWeights(frame, zero_nans=True)
            # Make sure OBS-ID is populated for yearly maps
            if hdr['OBS-ID'][0] is None and hdr['FITSNAME'][0].split("_")[0] == 'yearly':
                f = hdr['FITSNAME'][0].split("_")
                # from basename get for example: 'yearly_winter_2020'
                OBSID = ("_".join([f[0], f[2], f[3]]), hdr['OBS-ID'][1])
                hdr['OBS-ID'] = OBSID
                logger.info(f"Inserting OBS-ID to header: {hdr['OBS-ID']}")
            maps.fitsio.save_skymap_fits(fitsfile, frame['T'], overwrite=overwrite,
                                         compress=compress, hdr=hdr)
            logger.info(f"Created: {fitsfile}")
            logger.info(f"FITS creation time: {elapsed_time(t0)}")

    return


def run_fpack(fitsfile, fpack_options='', logger=None):
    "fpack a fitsfile"

    if not logger:
        logger = LOGGER
    # Remove fz file if exists
    if os.path.isfile(fitsfile+'.fz'):
        os.remove(fitsfile+'.fz')
    cmd = f"fpack {fpack_options} {fitsfile}"
    logger.info(f"running: {cmd}")
    t0 = time.time()
    return_code = subprocess.call(cmd, shell=True)
    logger.info(f"fpack time: {elapsed_time(t0)}")
    logger.info(f"Created: {fitsfile}.fz")
    os.remove(fitsfile)
    return return_code


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


def get_NP(MP):

    """ Get the number of processors in the machine
    if MP == 0, use all available processor
    """
    # For it to be a integer
    MP = int(MP)
    if MP == 0:
        NP = multiprocessing.cpu_count()
    elif isinstance(MP, int):
        NP = MP
    else:
        raise ValueError('MP is wrong type: %s, integer type' % MP)
    return NP

# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler

LOGGER = logging.getLogger(__name__)


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

    f1 = core.G3File(g3file)
    logger.info(f"Loading: {g3file}")
    for frame in f1:
        if frame.type == core.G3FrameType.Map:
            t0 = time.time()
            logger.info(f"type: {frame.type} -- Id: {frame['Id']}")
            maps.fitsio.save_skymap_fits(fitsfile, frame['T'],
                                         overwrite=overwrite, compress=compress)
            logger.info(f"Created: {fitsfile}")
            logger.info(f"Creation time: {elapsed_time(t0)}")


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

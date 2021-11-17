# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps, transients
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from astropy.time import Time
import subprocess
import multiprocessing as mp
import types
import copy
import shutil
import magic
import errno
import spt3g_ingest
from spt3g_ingest import sqltools

LOGGER = logging.getLogger(__name__)

# Mapping of metadata to FITS keywords
_keywords_map = {'ObservationStart': ('DATE-BEG', 'Observation start date'),
                 'ObservationStop': ('DATE-END', 'Observation end date'),
                 'ObservationID': ('OBS-ID', 'Observation ID'),
                 'Id': ('BAND', 'Band name'),
                 'SourceName': ('OBJECT', 'Name of object'),
                 }


class g3worker():

    """ A class to filter g3 maps, dump them a FITS files and ingest
    their metadata"""

    def __init__(self, **keys):

        # Load the configurarion
        self.config = types.SimpleNamespace(**keys)

        # Start Logging
        self.setup_logging()

        # Prepare vars
        self.prepare()

        # Check input files vs file list
        self.check_input_files()

        # Load mask for transients
        if self.config.mask:
            self.load_mask()

        # Load coadds for transients
        if self.config.coadd is not None:
            self.load_coadds()
            self.logger.info(f"Coadd keys:{self.g3coadds.keys()}")

    def check_input_files(self):
        " Check if the inputs are a list or a file with a list"

        t = magic.Magic(mime=True)
        if self.nfiles == 1 and t.from_file(self.config.files[0]) == 'text/plain':
            self.logger.info(f"{self.config.files[0]} is a list of files")
            # Now read them in
            with open(self.config.files[0], 'r') as f:
                lines = f.read().splitlines()
            self.logger.info(f"Read: {len(lines)} input files")
            self.config.files = lines
            self.nfiles = len(lines)
        else:
            self.logger.info("Nothing to see here")

    def run_files(self):
        " Run all g3files"
        if self.NP > 1:
            self.run_mp()
        else:
            self.run_serial()

    def run_mp(self):
        " Run g3files using multiprocessing.Process"
        k = 1
        jobs = []
        # Loop one to defined the jobs
        for g3file in self.config.files:
            self.logger.info(f"Starting mp.Process for {g3file}")
            fargs = (g3file, k)
            p = mp.Process(target=self.run_g3file, args=fargs)
            jobs.append(p)
            k += 1

        # Loop over the process in chunks of size NP
        for job_chunk in chunker(jobs, self.NP):
            for job in job_chunk:
                self.logger.info(f"Starting job: {job.name}")
                job.start()
            for job in job_chunk:
                self.logger.info(f"Joining job: {job.name}")
                job.join()

    def run_serial(self):
        " Run all g3files serialy "
        k = 1
        for g3file in self.config.files:
            self.run_g3file(g3file, k)
            k += 1

    def run_async(self):
        # *** DO NOT USE THIS ONE, it has memory issues with spt3g pipe()
        " Run g3files using multiprocessing.apply_async"

        with mp.get_context('spawn').Pool() as p:
            p = mp.Pool(processes=self.NP, maxtasksperchild=1)
            self.logger.info(f"Will use {self.NP} processors to convert and ingest")

            k = 1
            for g3file in self.config.files:
                fargs = (g3file, k)
                kw = {}
                self.logger.info(f"Starting apply_async.Process for {g3file}")
                p.apply_async(self.run_g3file, fargs, kw)
                k += 1
            p.close()
            p.join()

    def run_g3file(self, g3file, k):
        " Run the task(s) for a g3file"
        t0 = time.time()
        self.logger.info(f"Doing: {k}/{self.nfiles} files")
        self.g3_to_fits_raw(g3file)
        if self.config.filter_transient:
            self.g3_to_fits_filtd(g3file)
        self.logger.info(f"Completed: {k}/{self.nfiles} files")
        self.logger.info(f"Total time: {elapsed_time(t0)} for: {g3file}")

    def prepare(self):
        """Intit dictionaries to store relevat information"""
        # Define the dictionary that will hold the headers
        self.hdr = {}
        self.basename = {}
        self.folder_date = {}
        self.precooked = {}

        # The number of files to process
        self.nfiles = len(self.config.files)

        # Get the number of processors to use
        self.NP = get_NP(self.config.np)

        # Check DB table exists
        if self.config.ingest:
            sqltools.check_dbtable(self.config.dbname, self.config.tablename)

    def precook_g3file(self, g3file):
        """Perform tasks need for each g3file to be worked on"""

        if g3file in self.precooked.keys():
            self.logger.info(f"Skipping {g3file} -- already precooked")
            return
        self.logger.info(f"Preparing file: {g3file}")
        # Get the basename
        self.basename[g3file] = get_g3basename(g3file)
        # Get the metadata of the g3file
        self.hdr[g3file] = get_metadata(g3file)
        self.folder_date[g3file] = get_folder_date(self.hdr[g3file])
        self.precooked[g3file] = True

    def get_fitsname(self, g3file, suffix=''):
        "Set the name for the output fitsfile"
        fitsfile = os.path.join(self.config.outdir,
                                self.folder_date[g3file],
                                f"{self.basename[g3file]}{suffix}.fits")
        return fitsfile

    def setup_logging(self):
        """ Simple logger that uses configure_logger() """

        # Create the logger
        create_logger(level=self.config.loglevel,
                      log_format=self.config.log_format,
                      log_format_date=self.config.log_format_date)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging Started at level:{self.config.loglevel}")
        self.logger.info(f"Running spt3g_ingest version: {spt3g_ingest.__version__}")

    def load_mask(self):
        """Load the mask for filter_transient routines"""
        t0 = time.time()
        self.logger.info(f"Reading mask file: {self.config.mask}")
        self.g3mask = None
        for frame in core.G3File(self.config.mask):
            if frame.type != core.G3FrameType.Map:
                continue
            if "Mask" in frame:
                self.g3mask = frame["Mask"]
            else:
                self.g3mask = frame["T"]
            break
        if self.g3mask is None:
            raise RuntimeError("Missing mask frame")
        self.mask_id = "Mask"
        self.logger.info(f"Total time reading {self.config.mask}: {elapsed_time(t0)}")
        return

    def load_coadds(self):
        """
        Load coadd g3 files for transients filtering.
        If specified, apply mask to coadd to save memory
        """

        t0 = time.time()
        # dictionary to store g3coadd frames
        self.g3coadds = {}
        # Get the number of coadds
        self.Ncoadds = len(self.config.coadd)

        # Prepare for MP, use mp.Manage to hold outputs
        if self.NP >= self.Ncoadds and self.NP > 1:
            p = {}
            manager = mp.Manager()
            return_dict = manager.dict()
        else:
            return_dict = None

        # Loop over all coadd files
        for filename in self.config.coadd:
            if self.NP >= self.Ncoadds and self.NP > 1:
                ar = (filename, return_dict)
                p[filename] = mp.Process(target=self.load_single_coadd, args=ar)
                self.logger.info(f"Starting job: {p[filename].name}")
                p[filename].start()
            else:
                res = self.load_single_coadd(filename, return_dict)
                self.g3coadds.update(res)

        # Make sure all process are closed before proceeding
        if self.NP >= self.Ncoadds and self.NP > 1:
            for filename in p.keys():
                self.logger.info(f"joining job: {p[filename].name}")
                p[filename].join()
            # Update with returned dictionary
            self.g3coadds = return_dict

        self.subtract_coadd = True
        self.logger.info(f"Total time coadd read: {elapsed_time(t0)}")
        return

    def load_single_coadd(self, g3coaddfile, g3coadds):
        """Load a single coadd file"""

        self.logger.info(f"Reading coadd file(s): {g3coaddfile}")

        if g3coadds is None:
            g3coadds = {}

        # Loop over frames
        for frame in core.G3File(g3coaddfile):
            self.logger.debug(f"Reading frame: {frame}")
            if frame.type != core.G3FrameType.Map:
                continue
            if frame["Id"] not in self.config.band:
                continue
            if not self.config.polarized:
                maps.map_modules.MakeMapsUnpolarized(frame)
            maps.map_modules.RemoveWeights(frame, zero_nans=True)
            del frame["Wunpol"]
            tmap = frame.pop("T")
            # apply mask
            if self.config.mask is not None:
                tmap *= self.g3mask
            tmap.compact(zero_nans=True)
            g3coadds["Coadd{}".format(frame["Id"])] = {"T": tmap}

        return g3coadds

    def g3_to_fits_raw(self, g3file, fitsfile=None):
        """ Dump g3file as fits"""

        t0 = time.time()
        # Pre-cook the g3file
        self.precook_g3file(g3file)

        # Define fitsfile name only if undefined
        if fitsfile is None:
            fitsfile = self.get_fitsname(g3file, '_raw')

        # Skip if fitsfile exists and overwrite/clobber not True
        # Note that if skip is False we proceed, and therefore will overwrite
        # the fitsfile. That why we set overwrite=True in save_skymap_fits()
        if self.skip_fitsfile(fitsfile):
            self.logger.warning(f"File exists, skipping: {fitsfile}")
            return

        # Make a copy of the header to modify
        hdr = copy.deepcopy(self.hdr[g3file])
        # Populate additional metadata for DB
        hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')
        hdr['FILETYPE'] = 'raw'

        # Second loop to write FITS
        g3 = core.G3File(g3file)
        self.logger.info(f"Loading: {g3file} for g3_to_fits_raw()")
        for frame in g3:
            # Convert to FITS
            if frame.type == core.G3FrameType.Map:
                self.logger.info(f"Transforming to FITS: {frame.type} -- Id: {frame['Id']}")
                maps.RemoveWeights(frame, zero_nans=True)
                # Make sure that the folder exists:
                create_dir(os.path.dirname(fitsfile))
                # Check for weight Plane before:
                try:
                    weight = frame['Wunpol']
                except KeyError:
                    weight = None
                    self.logger.warning("No 'Wunpol' frame to add as weight")
                maps.fitsio.save_skymap_fits(fitsfile, frame['T'],
                                             overwrite=True,
                                             compress=self.config.compress,
                                             W=weight,
                                             hdr=hdr)
                self.logger.info(f"Created: {fitsfile}")
        self.logger.info(f"G3 to FITS creation time: {elapsed_time(t0)}")

        # If we want to fpack
        if self.config.fpack:
            self.run_fpack(fitsfile)

        self.logger.info(f"Total FITS creation time: {elapsed_time(t0)}")

        if self.config.ingest:
            if self.config.fpack:
                fitsfile = fitsfile + ".fz"
            sqltools.ingest_fitsfile(fitsfile, self.config.tablename,
                                     dbname=self.config.dbname,
                                     replace=self.config.replace)

        return

    def g3_to_fits_filtd(self, g3file, fitsfile=None):
        """Filter a g3file and write result as fits"""

        t0 = time.time()
        # Pre-cook the g3file
        self.precook_g3file(g3file)

        # Define fitsfile name only if undefined
        if fitsfile is None:
            fitsfile = self.get_fitsname(g3file, '_flt')

        # Skip if fitsfile exists and overwrite/clobber not True
        # Note that if skip is False we proceed, and therefore will overwrite
        # the fitsfile. That why we set overwrite=True in save_skymap_fits()
        if self.skip_fitsfile(fitsfile):
            self.logger.warning(f"File exists, skipping: {fitsfile}")
            return

        # Make a copy of the header to modify
        hdr = copy.deepcopy(self.hdr[g3file])
        # Populate additional metadata for DB
        hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')
        hdr['FILETYPE'] = 'filtered'

        # Construct the map_id
        band = hdr['BAND'][0]
        map_id = 'Coadd'+band

        # Create a pipe
        self.logger.info(f"Filtering {g3file} band: {band}")
        pipe = core.G3Pipeline()
        pipe.Add(core.G3Reader, filename=g3file)

        if self.config.coadd is not None:
            # Match the band of the coadd
            self.logger.info(f"Adding InjectMaps for {map_id}")
            pipe.Add(maps.InjectMaps, map_id=map_id,
                     maps_in=self.g3coadds[map_id], ignore_missing_weights=True)

        elif self.config.mask is not None:
            # mask has been injected in coadd.
            # this handles the no-coadd case
            self.logger.info(f"Adding mask InjectMaps for {self.mask_id}")
            pipe.Add(maps.InjectMaps, map_id=self.mask_id,
                     maps_in={"T": self.g3mask},
                     ignore_missing_weights=True)

        if not self.config.polarized:
            pipe.Add(maps.map_modules.MakeMapsUnpolarized)

        # Add the TransientMapFiltering to the pipe
        self.logger.info(f"Adding TransientMapFiltering for {band}")
        pipe.Add(transients.TransientMapFiltering,
                 bands=self.config.band,  # or just band
                 subtract_coadd=self.subtract_coadd,
                 mask_id=self.mask_id)
        # Write as FITS file
        self.logger.info(f"Adding SaveMapFrame for: {fitsfile}")
        # Make sure that the folder exists:
        create_dir(os.path.dirname(fitsfile))
        pipe.Add(maps.fitsio.SaveMapFrame,
                 map_id=band, output_file=fitsfile,
                 compress=self.config.compress,
                 overwrite=True, hdr=hdr)
        self.logger.info(f"Will create fitsfile: {fitsfile}")
        self.logger.info("Running Filtering pipe")
        pipe.Run(profile=False)
        del pipe
        self.logger.info(f"Total time: {elapsed_time(t0)} for Filtering pipe {g3file}")

        # If we want to fpack
        if self.config.fpack:
            self.run_fpack(fitsfile)

        self.logger.info(f"Total FITS creation time: {elapsed_time(t0)}")

        if self.config.ingest:
            if self.config.fpack:
                fitsfile = fitsfile + ".fz"
            sqltools.ingest_fitsfile(fitsfile, self.config.tablename,
                                     dbname=self.config.dbname,
                                     replace=self.config.replace)
        return

    def run_fpack(self, fitsfile):
        "Run fpack on a fitsfile"
        # Remove fz file if exists
        if os.path.isfile(fitsfile+'.fz'):
            os.remove(fitsfile+'.fz')
        cmd = f"fpack {self.config.fpack_options} {fitsfile}"
        self.logger.info(f"running: {cmd}")
        t0 = time.time()
        return_code = subprocess.call(cmd, shell=True)
        self.logger.info(f"fpack time: {elapsed_time(t0)}")
        self.logger.info(f"Created: {fitsfile}.fz")
        os.remove(fitsfile)
        return return_code

    def skip_fitsfile(self, fitsfile, size=10):
        """
        Check if fitsfile should be skipped, checking if the file exists
        and that is greater than [size] (in MB)
        """

        # Get the size fron MB to Bytes
        size = size*1024**2
        if self.config.fpack:
            fitsfile = fitsfile + ".fz"
        if os.path.isfile(fitsfile) and not self.config.clobber:
            # Make sure we don't have a zombie file of zero size
            if os.path.getsize(fitsfile) > size:
                skip = True
                self.logger.warning(f"File exists, skipping: {fitsfile}")
            else:
                self.logger.warning(f"Found zombie file: {fitsfile}, will remove it")
                os.remove(fitsfile)
                skip = False
        else:
            skip = False
        return skip


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


def get_metadata(g3file, logger=None):

    """
    Extract metadata from g3file and store in header dictionary
    """

    t0 = time.time()
    if not logger:
        logger = LOGGER

    # Pre-populate extra metadata that we will need
    hdr = pre_populate_metadata()
    hdr['PARENT'] = (os.path.basename(g3file), 'Name of parent file')
    g3 = core.G3File(g3file)
    logger.info(f"Loading: {g3file} for metadata extraction")
    for frame in g3:
        # Extract metadata
        if frame.type == core.G3FrameType.Observation or frame.type == core.G3FrameType.Map:
            logger.info(f"Extracting metadata from frame: {frame.type}")
            hdr = extract_metadata_frame(frame, hdr)

        if frame.type == core.G3FrameType.Map:
            if hdr['OBS-ID'][0] is None and hdr['PARENT'][0].split("_")[0] == 'yearly':
                f = hdr['PARENT'][0].split("_")
                # from basename get for example: 'yearly_winter_2020'
                OBSID = ("_".join([f[0]] + f[2:-1]), hdr['OBS-ID'][1])
                hdr['OBS-ID'] = OBSID
                hdr['DATE-BEG'] = OBSID
                logger.info(f"Inserting OBS-ID to header: {hdr['OBS-ID']}")
                logger.info(f"Inserting DATE-BEG to header: {hdr['DATE-BEG']}")

    logger.info(f"Metadata Extraction time: {elapsed_time(t0)}")
    return hdr


def get_folder_date(hdr):
    """
    Extract the folder name based on the observation date
    """
    try:
        folder_date = Time(hdr['DATE-BEG'][0]).strftime("%Y-%m")
    except ValueError:
        folder_date = hdr['DATE-BEG'][0]
    return folder_date


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
        NP = mp.cpu_count()
    elif isinstance(MP, int):
        NP = MP
    else:
        raise ValueError('MP is wrong type: %s, integer type' % MP)
    return NP


def create_dir(dirname):
    "Safely attempt to create a folder"
    if not os.path.isdir(dirname):
        LOGGER.info(f"Creating directory {dirname}")
        try:
            os.makedirs(dirname, exist_ok=True)
        except OSError as e:
            if e.errno != errno.EEXIST:
                LOGGER.warning(f"Problem creating {dirname} -- proceeding with trepidation")


def chunker(seq, size):
    "Chunk a sequence in chunks of a given size"
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def relocate_g3file(g3file, outdir, dryrun=False):
    "Function to relcate a 'raw' g3 file"
    # Get the metadata for folder information
    hdr = get_metadata(g3file)
    folder_date = get_folder_date(hdr)
    dirname = os.path.join(outdir, folder_date)

    if dryrun:
        LOGGER.info(f"DRYRUN: mv {g3file} {dirname}")
        return

    if not os.path.isdir(dirname):
        LOGGER.info(f"Creating directory {dirname}")
        os.mkdir(dirname)
    LOGGER.info(f"Moving {g3file} --> {dirname}")
    shutil.move(g3file, dirname)
    return

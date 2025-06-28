# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps, transients, sources
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from astropy.time import Time
import multiprocessing as mp
import types
import copy
import shutil
import magic
import errno
import re
import spt3g_ingest
from spt3g_ingest import sqltools
from tempfile import mkdtemp
import numexpr as ne
import numpy as np
import math
import astropy
from astropy.nddata import Cutout2D
import pandas as pd
import socket
import spt3g_ingest.data_types as data_types


# The filetype extensions for file types
# FILETYPE_SUFFIX = {'filtered': 'fltd', 'passthrough': 'psth'}
FILETYPE_SUFFIX = {'filtered': 'fltd', 'coaddfiltered': 'cfltd', 'passthrough': 'psth'}
FILETYPE_EXT = {'FITS': 'fits', 'G3': 'g3.gz'}

# The format for the COADS_ID frame
COADD_ID = "Coadd_{band}_{field_or_season}"

# Logger
LOGGER = logging.getLogger(__name__)
LOGGER.propagate = False
logger = LOGGER

# Mapping of metadata to FITS keywords
_keywords_map = {'ObservationStart': ('DATE-BEG', 'Observation start date'),
                 'ObservationStop': ('DATE-END', 'Observation end date'),
                 'ObservationID': ('OBSID', 'Observation ID'),
                 'Id': ('BAND', 'Observing Frequency'),
                 'SourceName': ('FIELD', 'Name of Observing Field'),
                 }

# Get the names of all of the SPT fields (so far...)
_ALL_SPT_FIELDS = sources.get_all_fields()

# The allowed bands
_allowed_bands = ['90GHz', '150GHz', '220GHz']


class g3worker():

    """ A class to filter g3 maps, dump them a FITS files and ingest
    their metadata"""

    def __init__(self, **keys):

        # Load the configurarion
        self.config = types.SimpleNamespace(**keys)

        # Get the number of processors to use
        self.NP = get_NP(self.config.np)
        if self.NP > 1:
            self.MP = True
        else:
            self.MP = False

        # Start Logging
        self.logger = LOGGER
        self.setup_logging()

        # Prepare vars
        self.prepare()

        # Check input files vs file list
        self.check_input_files()

        # Find and preload coadd from database
        if self.config.preload_coadds and self.config.filter_transient_coadd:
            self.get_coadd_seasons()
            self.get_unique_coadd_names()
            self.load_coadds()
        elif self.config.coadds and self.config.filter_transient_coadd:
            self.load_coadds()

    def get_coadd_seasons(self):
        """
        Query the archive DB to get the coadd file ID/name for each g3 observation
        """

        # Stage db file in needed
        if self.config.stage:
            dbname = stage_file(self.config.dbname, stage_prefix=self.config.stage_prefix)
        else:
            dbname = self.config.dbname

        con = sqltools.create_con(dbname, read_only=True)
        # Build the query:
        query = sqltools.get_query_field_seasons(self.config.tablename, files=self.config.files)
        self.logger.info("Running query to find seasons and fields for input files")
        # self.logger.debug(query)
        df = pd.read_sql_query(query, con)
        # Store the short name of the season
        df['SEASON_SHORT'] = df['SEASON'].str.replace('spt3g-', '', regex=False)
        # Combine BAND+SEASON and BAND+FIELD
        df['BAND_SEASON'] = df['BAND'] + ' ' + df['SEASON']
        df['BAND_FIELD'] = df['BAND'] + ' ' + df['FIELD']
        # Pass into the class for later
        self.df_query = df
        con.close()
        if self.config.stage:
            remove_staged_file(dbname)

    def get_unique_coadd_names(self):
        """
        Get the list of unique keys of coadd files needed for the input file list
        and create a pandas DataFrame with it.
        For winter+summer observations, we need one per season+band.
        For galaxy observations the coadds are field+band
        """
        df = self.df_query

        # Select unique band+field for non spt3g-galaxy season
        df_season = df.loc[df['SEASON'] != 'spt3g-galaxy', ['BAND', 'FIELD', 'SEASON']]
        df_season['KEY'] = df_season['BAND'] + ' ' + df_season['SEASON']
        keys = df_season['KEY'].unique().tolist()

        self.coadd_filenames = []
        for key in keys:
            band, season = key.split()
            name = get_coadd_filename(band, season=season)
            self.logger.debug(f"Added coadd filename: {name}")
            self.coadd_filenames.append(name)

        # Select unique band+field for spt3g-galaxy fields
        df_field = df.loc[df['SEASON'] == 'spt3g-galaxy', ['BAND', 'FIELD', 'SEASON']]
        df_field['KEY'] = df_field['BAND'] + ' ' + df_field['SEASON'] + ' ' + df_field['FIELD']
        self.coadd_fields_keys = df_field['KEY'].unique().tolist()
        keys = df_field['KEY'].unique().tolist()
        for key in keys:
            band, season, field = key.split()
            name = get_coadd_filename(band, season=season, field=field)
            self.logger.debug(f"Added coadd filename: {name}")
            self.coadd_filenames.append(name)

        # Get the full path
        self.config.coadds = [get_coadd_archive_path() + os.sep + f for f in self.coadd_filenames]

    def check_input_files(self):
        """Check if the inputs are a list or a file with a list"""

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
            # Avoid small files that make filtering crash
            if self.skip_g3file(g3file, size=50):
                continue
            self.logger.info(f"Creating mp.Process for {g3file}")
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
            # Avoid small files that make filtering crash
            if self.skip_g3file(g3file, size=50):
                continue
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

        # Stage if needed
        if self.config.stage:
            g3file = stage_file(g3file, stage_prefix=self.config.stage_prefix)

        if self.config.passthrough:
            self.g3_to_fits(g3file)
        if self.config.filter_transient:
            self.g3_transient_filter(g3file, subtract_coadd=False)
        if self.config.filter_transient_coadd:
            self.g3_transient_filter(g3file, subtract_coadd=True)

        # Remove stage file
        if self.config.stage:
            remove_staged_file(g3file)

        self.logger.info(f"Completed: {k}/{self.nfiles} files")
        self.logger.info(f"Total time: {elapsed_time(t0)} for: {g3file}")

    def prepare(self):
        """Intit dictionaries to store relevat information"""
        # Define the dictionary that will hold the headers
        self.hdr = {}
        self.basename = {}
        self.folder_date = {}
        self.folder_year = {}
        self.field_name = {}
        self.precooked = {}
        self.field_season = {}
        self.g3coadds = {}

        # The number of files to process
        self.nfiles = len(self.config.files)

        # Set the number of threads for numexpr
        self.set_nthreads()

        # Check DB tables exists
        if self.config.ingest:
            sqltools.check_dbtable(self.config.dbname, self.config.tablename)
        if self.config.run_check or self.config.run_insert:
            sqltools.check_dbtable(self.config.run_dbname, self.config.run_tablename, Fd=data_types.g3RunFd)

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
        # Update the field name if passed in the command line
        if self.config.field_name:
            self.hdr[g3file]['FIELD'] = (self.config.field_name, self.hdr[g3file]['FIELD'][1])
            self.logger.info(f"Updated metadata for FIELD with: {self.config.field_name}")

        self.folder_date[g3file] = get_folder_date(self.hdr[g3file])
        self.folder_year[g3file] = get_folder_year(self.hdr[g3file])
        self.field_name[g3file] = self.hdr[g3file]['FIELD'][0]
        self.field_season[g3file] = self.hdr[g3file]['SEASON'][0]
        self.precooked[g3file] = True

    def set_outname(self, g3file, suffix='', filetype='FITS'):
        "Set the name for the output filename"
        ext = FILETYPE_EXT[filetype]
        outname = os.path.join(self.config.outdir,
                               self.folder_year[g3file],
                               # self.field_name[g3file],
                               self.folder_date[g3file],
                               f"{self.basename[g3file]}_{suffix}.{ext}")
        return outname

    def setup_logging(self):
        """
        Sets up the logging configuration using `create_logger` and logs key info.
        Configures logging level, format, and other related settings based on the
        configuration object. Logs the start of logging and the version of the
        `mmblink` package.

        Raises:
        - ValueError: If the logger configuration is invalid or incomplete.
        """

        # Create the logger
        if self.logger is None:
            self.logger = logging.getLogger(__name__)

        if self.logger.hasHandlers():
            self.logger.debug("Logger already has handlers.")
            return

        create_logger(logger=self.logger, MP=self.MP,
                      level=self.config.loglevel,
                      log_format=self.config.log_format,
                      log_format_date=self.config.log_format_date)
        self.logger.info(f"Logging Started at level:{self.config.loglevel}")
        self.logger.info(f"Running spt3g_ingest version: {spt3g_ingest.__version__}")

    def load_coadds(self):
        """
        Load coadd g3 files for transients filtering.
        """

        t0 = time.time()
        # dictionary to store g3coadd frames
        self.g3coadds = {}
        # Get the number of coadds
        self.Ncoadds = len(self.config.coadds)

        # Prepare for MP, use mp.Manage to hold outputs
        if self.NP > 1:
            p = {}
            jobs = []
            manager = mp.Manager()
            return_dict = manager.dict()
        else:
            return_dict = None

        # Loop over all coadd files
        for filename in self.config.coadds:
            if self.NP > 1:
                self.logger.info(f"Creating mp.Process for {filename}")
                fargs = (filename, return_dict)
                p = mp.Process(target=load_coadd_frame, args=fargs)
                jobs.append(p)
                # self.logger.info(f"Starting job: {p[filename].name}")
            else:
                res = load_coadd_frame(filename, return_dict)
                self.g3coadds.update(res)

        # Make sure all process are closed before proceeding
        if self.NP > 1:
            for job_chunk in chunker(jobs, self.NP):
                for job in job_chunk:
                    self.logger.info(f"Starting job: {job.name}")
                    job.start()
                for job in job_chunk:
                    self.logger.info(f"Joining job: {job.name}")
                    job.join()
                    self.logger.info(f"Completed job: {job.name}")
                    del job
            self.g3coadds = return_dict

        # Just to make sure we got them
        self.logger.info(f"Total time to read coadd files: {elapsed_time(t0)}")
        self.logger.info("List of loaded coadds:")
        for k, v in self.g3coadds.items():
            self.logger.info(f"NAME:{k} -- FRAME:{v}")
        return

    def g3_to_fits(self, g3file, overwrite=False, fitsfile=None, trim=True):
        """ Dump g3file as fits"""

        t0 = time.time()
        if self.NP > 1:
            self.setup_logging()

        # Before we proceed any further, we check against DB or runs
        filetype = 'PSTH'
        if self.config.run_check and self.check_run_g3file(g3file, filetype):
            self.logger.info(f"Skipping: {os.path.basename(g3file)} already processed for {filetype}")
            return

        # Pre-cook the g3file
        self.precook_g3file(g3file)

        # Define fitsfile name only if undefined
        if fitsfile is None:
            suffix = FILETYPE_SUFFIX['passthrough']
            fitsfile = self.set_outname(g3file, suffix=suffix, filetype="FITS")

        # Skip if fitsfile exists and overwrite/clobber not True
        # Note that if skip is False we proceed, and therefore will overwrite
        # the fitsfile. That why we set overwrite=True in the save_skymap_fits
        # functions
        if self.skip_filename(fitsfile):
            self.logger.warning(f"File already exists, skipping: {fitsfile}")
            return

        if self.hdr[g3file]['BAND'][0] not in self.config.band:
            self.logger.warning(f"Skipping: {g3file} not in selected bands")
            return

        # Make a copy of the header to modify
        hdr = copy.deepcopy(self.hdr[g3file])
        # Populate additional metadata for DB
        hdr['FITSNAME'] = (os.path.basename(fitsfile), 'Name of fits file')
        hdr['FILETYPE'] = ('passthrough', 'The file type')
        hdr['PARENT'] = (os.path.basename(g3file), 'Name of parent file')
        # Get the metadata/hdr into an astropy Header object
        hdr = metadata_to_astropy_header(hdr)
        # Prepare to write FITS
        g3 = core.G3File(g3file)
        self.logger.info(f"Loading: {g3file} for g3_to_fits()")

        # Make sure that the folder exists:
        create_dir(os.path.dirname(fitsfile))

        # Change the path of the fitsfile is indirect_write
        if self.config.indirect_write:
            # Keep the orginal name
            fitsfile_keep = fitsfile
            tmp_dir = mkdtemp(prefix=self.config.indirect_write_prefix)
            fitsfile = os.path.join(tmp_dir, os.path.basename(fitsfile_keep))
            self.logger.info(f"Will use indirect_write to: {fitsfile}")
            # Make sure that the folder exists:
            create_dir(os.path.dirname(fitsfile))

        for frame in g3:

            # Convert to FITS
            if frame.type == core.G3FrameType.Map:

                if frame['Id'] not in _allowed_bands:
                    self.logger.warning(f"Ignoring {frame['Id']} -- not in allowed_bands")
                    continue

                self.logger.info(f"Transforming to FITS: {frame.type} -- band: {hdr['BAND']}")
                self.logger.debug("Removing weights")
                maps.RemoveWeights(frame, zero_nans=True)
                maps.MakeMapsUnpolarized(frame)
                # Get the units of the Temperature map and add to the header
                units, units_name = get_T_frame_units(frame)
                hdr['UNITS'] = (units, f"Data units {units_name}")
                self.logger.debug(f"Removing G3 units --> {units_name}")
                remove_g3_units(frame, units=core.G3Units.mK)
                # In case we have many bands per g3 file
                band = frame['Id']
                if band != hdr['BAND']:
                    self.logger.warning(f"Metadata band:{hdr['BAND']} does not match frame band: {band}")
                    continue

                if trim:
                    field = hdr['FIELD']
                    self.logger.info(f"Will write trimmed FITS file for field: {field}")
                    save_skymap_fits_trim(frame, fitsfile, field,
                                          hdr=hdr,
                                          compress=self.config.compress,
                                          overwrite=True)
                else:
                    # Get the T and weight frames
                    T = frame['T']
                    try:
                        W = frame.get('Wpol', frame.get('Wunpol', None))
                    except KeyError:
                        W = None
                        self.logger.warning("No 'Wpol/Wunpol' frame to add as weight")
                    maps.fitsio.save_skymap_fits(
                        fitsfile, T,
                        overwrite=True,
                        compress=self.config.compress,
                        hdr=hdr,
                        W=W)

        self.logger.info(f"Created: {fitsfile}")
        self.logger.info(f"Total time: {elapsed_time(t0)} for passthrough: {g3file}")

        # And now we write back fits file to the orginal location
        if self.config.indirect_write:
            self.logger.info(f"Moving {fitsfile} --> {fitsfile_keep}")
            shutil.move(fitsfile, fitsfile_keep)
            shutil.rmtree(tmp_dir)
            fitsfile = fitsfile_keep

        self.logger.info(f"Total FITS creation time: {elapsed_time(t0)}")

        if self.config.ingest:
            sqltools.ingest_fitsfile(fitsfile, self.config.tablename,
                                     dbname=self.config.dbname,
                                     replace=self.config.replace)

        # Insert into the runs DB
        if self.config.run_insert:
            self.ingest_run_g3file(g3file, 'PSTH')

        return

    def g3_transient_filter(self, g3file, trim=True, subtract_coadd=False):
        """
        Perform Transient filer on a g3file and write result as G3/FITS file
        """
        if self.NP > 1:
            self.setup_logging()
        t0 = time.time()

        # Define output names
        if subtract_coadd is True:
            suffix = FILETYPE_SUFFIX['coaddfiltered']
            filetype = 'CFLTD'
        else:
            suffix = FILETYPE_SUFFIX['filtered']
            filetype = 'FLTD'

        # Before we proceed any further, we check against DB or runs
        if self.config.run_check and self.check_run_g3file(g3file, filetype):
            self.logger.info(f"Skipping: {os.path.basename(g3file)} already processed for {filetype}")
            return

        # Pre-cook the g3file
        self.precook_g3file(g3file)

        # Create tmp folder and dict with names to keepmif indirect_write
        if self.config.indirect_write:
            outname_keep = {}  # dict to keep the actual output names
            tmp_dir = mkdtemp(prefix=self.config.indirect_write_prefix)

        outname = {}
        skipfile = {}
        for ft in self.config.output_filetypes:
            outname[ft] = self.set_outname(g3file, suffix=suffix, filetype=ft)
            skipfile[ft] = self.skip_filename(outname[ft])
            # Make sure that the folder exists:
            create_dir(os.path.dirname(outname[ft]))
            # Change the path of the output files if indirect_write
            if self.config.indirect_write:
                outname_keep[ft] = outname[ft]
                outname[ft] = os.path.join(tmp_dir, os.path.basename(outname_keep[ft]))
                self.logger.info(f"Will use indirect_write to: {outname[ft]}")
                # Make sure that the folder exists:
                create_dir(os.path.dirname(outname[ft]))

        # Make a copy of the header to modify
        hdr = copy.deepcopy(self.hdr[g3file])
        # Populate additional metadata for DB
        if 'FITS' in self.config.output_filetypes:
            hdr['FITSNAME'] = (os.path.basename(outname['FITS']), 'Name of fits file')
        hdr['FILETYPE'] = ('filtered', 'The file type')
        hdr['PARENT'] = (os.path.basename(g3file), 'Name of parent file')
        # The UNITS
        hdr['BUNIT'] = ('mJy', 'Flux is in [mJy]')
        # Get the metadata/hdr into an astropy Header object
        hdr = metadata_to_astropy_header(hdr)

        # Construct the map_id
        band = hdr['BAND']
        season = hdr['SEASON']
        field = hdr['FIELD']

        # Find the coadd filename
        if subtract_coadd is True:
            g3coaddfile = get_coadd_filename(band, field=field, season=season)
            if g3coaddfile in self.g3coadds:
                self.logger.info(f"Coadd frame ALREADY loaded for: {g3coaddfile}")
            else:
                self.logger.info(f"Coadd frame NOT loaded for: {g3coaddfile}")
                g3coaddfile_fullpath = os.path.join(get_coadd_archive_path(), g3coaddfile)
                self.g3coadds[g3coaddfile] = load_coadd_frame(g3coaddfile_fullpath)
                self.logger.info(f"Coadd LOADED for: {g3coaddfile}")

        # Create a pipe
        self.logger.info(f"Loading pipe for {g3file} band: {band}")
        pipe = core.G3Pipeline()

        # Read in the g3file that we will work on
        pipe.Add(core.G3Reader, filename=g3file)

        # If we want coadd subtraction we add it to the pipe
        if subtract_coadd is True:
            # Match the band of the coadd
            self.logger.info(f"Adding InjectMaps for: {g3coaddfile}")
            pipe.Add(maps.InjectMaps,
                     map_id=f"Coadd{band}",
                     maps_in=self.g3coadds[g3coaddfile],
                     ignore_missing_weights=True)

        if not self.config.polarized:
            pipe.Add(maps.map_modules.MakeMapsUnpolarized)

        # Add the TransientMapFiltering to the pipe
        self.logger.info(f"Adding TransientMapFiltering for {band}")
        pipe.Add(transients.TransientMapFiltering,
                 bands=self.config.band,  # or just band
                 subtract_coadd=subtract_coadd,
                 field=self.field_season[g3file],
                 compute_snr_annulus=self.config.compute_snr_annulus)

        if 'G3' in self.config.output_filetypes:
            self.logger.info(f"Preparing to write G3: {outname['G3']}")
            pipe.Add(core.G3Writer, filename=outname['G3'])

        if 'FITS' in self.config.output_filetypes:
            self.logger.info(f"Preparing to write FITS: {outname['FITS']}")
            pipe.Add(maps.RemoveWeights, zero_nans=True)
            pipe.Add(remove_g3_units, units=core.G3Units.mJy)
            # pipe.Add(maps.fitsio.SaveMapFrame, output_file=outname['FITS'],
            #         compress='GZIP_2', overwrite=True, hdr=hdr)
            if trim:
                field = hdr['FIELD']
                self.logger.info(f"Will write trimmed FITS file for field: {field}")
                pipe.Add(save_skymap_fits_trim,
                         fitsfile=outname['FITS'],
                         field=field,
                         hdr=hdr,
                         compress=self.config.compress,
                         overwrite=True)

        self.logger.info("Executing .Run()")
        pipe.Run(profile=True)
        del pipe

        # And now we write back fits file to the orginal location
        if self.config.indirect_write:
            for ft in self.config.output_filetypes:
                self.logger.info(f"Moving {outname[ft]} --> {outname_keep[ft]}")
                shutil.move(outname[ft], outname_keep[ft])
                outname[ft] = outname_keep[ft]
                self.logger.info(f"Created file: {outname[ft]}")

        # Insert into the runs DB
        if self.config.run_insert:
            self.ingest_run_g3file(g3file, filetype)

        self.logger.info(f"Total time: {elapsed_time(t0)} for Filtering: {g3file}")
        return

    def skip_g3file(self, g3file, size=50):
        """
        Function to skip rogue/junk g3 files that should no be processed
        """
        file_size = os.path.getsize(g3file)/1024**2
        if file_size < size:
            skip = True
            self.logger.warning(f"File size: {file_size:.2f} Mb < {size} Mb, skipping: {g3file}")
        else:
            skip = False
        return skip

    def skip_filename(self, filename, size=10):
        """
        Check if filename (fits file) should be skipped, checking if the file
        exists and that is greater than [size] (in MB)
        """
        # Get the size fron MB to Bytes
        size = size*1024**2
        if os.path.isfile(filename) and not self.config.clobber:
            # Make sure we don't have a zombie file of zero size
            if os.path.getsize(filename) > size:
                skip = True
                self.logger.warning(f"File exists, skipping: {filename}")
            else:
                self.logger.warning(f"Found zombie file: {filename}, will remove it")
                os.remove(filename)
                skip = False
        else:
            skip = False
        return skip

    def set_nthreads(self):
        """Set the number of theards for numexpr"""
        if self.config.ntheads == 0:
            # ncores = os.cpu_count()
            ncores = ne.detect_number_of_cores()
            self.nthread = int(ncores/self.NP)
            if self.nthread < 1:
                self.nthread = 1
        else:
            self.nthread = self.config.ntheads
        ne.set_num_threads(self.nthread)
        self.logger.info(f"Set the number of threads for numexpr as {self.nthread}")

    def check_run_g3file(self, g3file, filetype):
        """Check if file has been run before"""
        g3file = os.path.basename(g3file)
        query = f"SELECT {filetype} FROM g3runinfo WHERE ID = '{g3file}'"
        self.logger.debug(f"run_check query: {query}")
        df = sqltools.query_with_retry(query, self.config.run_dbname)
        if df.empty:
            # False empty
            return False
        elif df[filetype][0] == 1:
            return True
        else:
            # False null"
            return False

    def ingest_run_g3file(self, g3file, filetype):

        query_template = """
        INSERT INTO {tablename} (ID, {filetype}, DATE_{filetype})
        VALUES ('{g3file}', 1, '{date}')
        ON CONFLICT(ID) DO UPDATE SET
            {filetype} = excluded.{filetype},
            DATE_{filetype} = excluded.DATE_{filetype}
        """

        g3file = os.path.basename(g3file)
        date = Time.now().isot
        # filename = os.path.basename(filename)
        query = query_template.format(**{'tablename': self.config.run_tablename,
                                         'g3file': g3file,
                                         'filetype': filetype,
                                         'date': date})
        self.logger.debug(query)
        sqltools.commit_with_retry(g3file, query, self.config.run_dbname)
        self.logger.info(f"Inserted {g3file} -- {filetype}")


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


def extract_metadata_frame(frame, metadata=None):
    """
    Extract selected metadata from a frame in a G3 file.

    Parameters:
    - frame: A frame object from the G3 file.
    - metadata (dict): A dictionary to store the extracted metadata (optional).

    Returns:
    - metadata (dict): Updated metadata dictionary with extracted values.
    """
    # Loop over all items and select only the ones in the Mapping
    if not metadata:
        metadata = {}
    for k in iter(frame):
        if k in _keywords_map.keys():
            keyword = _keywords_map[k][0]
            # We need to treat BAND diferently to avoid inconsistensies
            # in how Id is defined (i.e Coadd_90GHz, 90GHz, vs combined_90GHz)
            if keyword == 'BAND':
                try:
                    value = re.findall("90GHz|150GHz|220GHz", frame[k])[0]
                except IndexError:
                    continue
            # Need to re-cast G3Time objects
            elif isinstance(frame[k], core.G3Time):
                value = Time(frame[k].isoformat(), format='isot', scale='utc').isot
            else:
                value = frame[k]
            metadata[keyword] = (value, _keywords_map[k][1])

    # For backwards compatibily we add copies for OBS-ID and OBJECT
    metadata['OBS-ID'] = metadata['OBSID']
    metadata['OBJECT'] = metadata['FIELD']
    return metadata


def get_T_frame_units(frame):
    """Get the units of the Temperature frame"""

    if 'T' not in frame:
        msg = "'T' map not present in frame"
        LOGGER.error(msg)
        raise Exception(msg)

    units_name = frame['T'].units.name
    if units_name == 'Tcmb':
        units = core.G3Units.uK
        name = "uK"
    elif units_name == 'FluxDensity':
        units = core.G3Units.mJy
        name = "mJy"
    else:
        raise Exception(f"cannot extract units from frame[T]: {frame['T']}")
    return units, f"{units_name}[{name}]"


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

    # Make sure FIELD is a valid SPT field
    field_name = hdr['FIELD'][0]
    if field_name not in _ALL_SPT_FIELDS:
        logger.warning(f"{field_name} not in SPT fields... continuing with trepidation")

    # Get the field season
    try:
        field_season = get_field_season(hdr)
    except Exception:
        logger.warning(f"Cannot get field season for file: {g3file}")
        field_season = ''
    hdr['SEASON'] = (field_season, "Field Season")
    logger.info(f"Metadata Extraction time: {elapsed_time(t0)}")
    return hdr


def get_field_season(hdr, logger=None):
    """
    Get the field name for the g3file
    """
    if not logger:
        logger = LOGGER

    field_name = hdr['FIELD'][0]
    parent = hdr['PARENT'][0]
    # Check if this is a winter/yearly field
    if field_name is None:
        if ('winter' in parent or 'yearly' in parent):
            field_season = 'spt3g-winter'
        else:
            logger.warning(f"Cannot find field_name for field_name:{field_name}")
            field_season = None
    else:
        field_season = sources.get_field_season(field_name)

    logger.info(f"Will use field_season: {field_season} for file: {parent}")
    return field_season


def get_folder_date(hdr):
    """
    Extract the folder name based on the observation date
    """
    try:
        folder_date = Time(hdr['DATE-BEG'][0]).strftime("%Y-%m")
    except ValueError:
        folder_date = hdr['DATE-BEG'][0]
    return folder_date


def get_folder_year(hdr):
    """
    Extract the folder year based on the observation date
    """
    try:
        folder_year = Time(hdr['DATE-BEG'][0]).strftime("%Y")
    except ValueError:
        folder_year = hdr['DATE-BEG'][0][0:4]
    return folder_year


def get_g3basename(g3file):
    """ Get the basename for a g3 file that could be compressed or not"""
    if os.path.splitext(g3file)[1] == '.gz':
        basename = os.path.splitext(os.path.splitext(g3file)[0])[0]
    else:
        basename = os.path.splitext(g3file)[0]
    return os.path.basename(basename)


def configure_logger(logger, MP=False, logfile=None, level=logging.NOTSET,
                     log_format=None, log_format_date=None):
    """
    Configure an existing logger with specified settings. Sets the format,
    logging level, and handlers for the given logger. If a logfile is provided,
    logs are written to both the console and the file with rotation. If no log
    format or date format is provided, default values are used.

    Parameters:
    - logger (logging.Logger): The logger to configure.
    - logfile (str, optional): Path to the log file. If `None`, logs to the console.
    - level (int): Logging level (e.g., `logging.INFO`, `logging.DEBUG`).
    - log_format (str, optional): Log message format (default is detailed format with function name).
    - log_format_date (str, optional): Date format for logs (default is `'%Y-%m-%d %H:%M:%S'`).
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

    # Update logger to see process inforation
    if MP is True:
        FORMAT = FORMAT.replace("[%(levelname)s]", "[%(levelname)s-%(processName)s]")

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
    if logger_has_stdout_handler(logger):
        print("Skipping adding stdout handler")
        return
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.setLevel(level)
    handlers.append(sh)
    logger.addHandler(sh)
    return


def create_logger(logger=None, MP=False, logfile=None,
                  level=logging.NOTSET, log_format=None, log_format_date=None):
    """
    Configures and returns a logger with specified settings.
    Sets up logging based on provided level, format, and output file. Can be
    used for both `setup_logging` and other components.

    Parameters:
    - logger (logging.Logger, optional): The logger to configure. If `None`, a new logger
      is created.
    - logfile (str, optional): Path to the log file. If `None`, logs to the console.
    - level (int): Logging level (e.g., `logging.INFO`, `logging.DEBUG`).
    - log_format (str, optional): Format for log messages (e.g., `'%(asctime)s - %(message)s'`).
    - log_format_date (str, optional): Date format for logs (e.g., `'%Y-%m-%d %H:%M:%S'`).

    Returns:
    logging.Logger: The configured logger instance.

    Raises:
    - ValueError: If the log level or format is invalid.
    """

    if logger is None:
        logger = logging.getLogger(__name__)
    configure_logger(logger, MP=MP, logfile=logfile, level=level,
                     log_format=log_format, log_format_date=log_format_date)
    logging.basicConfig(handlers=logger.handlers, level=level)
    logger.propagate = False
    logger.info(f"Logging Created at level:{level}")
    return logger


def logger_has_stdout_handler(logger):
    """
    Check whether the given logger has a StreamHandler that
    writes to sys.stdout.
    Parameters:
     logger (logging.Logger): The logger instance to inspect.
    Returns:
     bool: True if a StreamHandler writing to sys.stdout is found,
           False otherwise.
    """
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            return True
    return False


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
            os.makedirs(dirname, mode=0o755, exist_ok=True)
        except OSError as e:
            if e.errno != errno.EEXIST:
                LOGGER.warning(f"Problem creating {dirname} -- proceeding with trepidation")


def chunker(seq, size):
    "Chunk a sequence in chunks of a given size"
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def stage_file(file, stage_prefix='spt3g_ingest-stage-'):
    """
    Stage input file to the stage directory
    """
    tmp_dir = mkdtemp(prefix=stage_prefix)
    file_copy = os.path.join(tmp_dir, os.path.basename(file))
    logger.info(f"Will stage: {file} --> {file_copy}")
    # Make sure that the folder exists:
    create_dir(os.path.dirname(file_copy))
    shutil.copy2(file, file_copy)
    return file_copy


def remove_staged_file(file):
    """
    Remove previously stage file
    """
    logger.info(f"Removing: {file}")
    os.remove(file)
    tmp_dir = os.path.dirname(file)
    logger.info(f"Removing tmp dir: {tmp_dir}")
    shutil.rmtree(tmp_dir)


def relocate_g3file(g3file, outdir, symlink=False, dryrun=False, manifest=None):
    "Function to relcate a g3 file by date"
    # Get the metadata for folder information
    hdr = digest_g3file(g3file)
    # field_name = hdr['FIELD'][0]
    folder_year = get_folder_year(hdr)
    folder_date = get_folder_date(hdr)
    # path = os.path.join(folder_year, field_name, folder_date)
    # path = os.path.join(folder_year, field_name)
    path = os.path.join(folder_year, folder_date)
    dirname = os.path.join(outdir, path)

    # We need to tweak the FILEPATH keyword in the header with
    # the final location
    hdr['FILEPATH'] = (f"{path}", "Folder path based on date-obs")

    if manifest is not None:
        manifest.write(f"{g3file} {dirname}\n")

    # Make sure that directory exists
    create_dir(dirname)

    if symlink is True:
        basename = os.path.basename(g3file)
        dest = os.path.join(dirname, basename)
        source = os.path.abspath(g3file)
        LOGGER.info(f"Making symlink: {source} --> {dest}")
        if dryrun is False:
            os.symlink(source, dest)
        else:
            LOGGER.info(f"DRYRUN: ln -s {source} {dest}")
    else:
        LOGGER.info(f"Moving: {g3file} --> {dirname}")
        if dryrun is False:
            shutil.move(g3file, dirname)
        else:
            LOGGER.info(f"DRYRUN: mv {g3file} {dirname}")

    return hdr


def digest_g3file(g3file):
    "Function to digest a g3 file"
    # Get the metadata for folder information
    hdr = get_metadata(g3file)
    # ID and FILENAME
    hdr['ID'] = (os.path.basename(g3file).split('.g3')[0], 'ID')
    hdr['FILENAME'] = (os.path.basename(g3file), 'The Filename')
    # Store the relative filepath, remove basepath from Taiga
    # We need to chage this to /project/ncsa/caps/spt3g on ICC
    string_to_remove_caps = "/data/spt3g/"
    string_to_remove_icc = "/project/ncsa/caps/spt3g/"
    filepath = os.path.dirname(g3file)
    filepath = filepath.replace(string_to_remove_caps, "")
    filepath = filepath.replace(string_to_remove_icc, "")
    hdr['FILEPATH'] = (filepath, 'The Filepath')
    return hdr


def remove_g3_units(frame, units):
    "Remove units for g3 frame"
    if frame.type != core.G3FrameType.Map:
        return frame
    t_scale = units if frame['T'].weighted else 1./units
    w_scale = units * units
    for k in ['T', 'Q', 'U']:
        if k in frame:
            frame[k] = frame.pop(k) * t_scale
    for k in ['Wunpol', 'Wpol']:
        if k in frame:
            frame[k] = frame.pop(k) * w_scale
    return frame


def crossRAzero(ras):
    """
    Check if the RA coordinates cross RA=0 and adjust accordingly.

    Parameters:
    - ras (array): An array of RA coordinates.

    Returns:
    - tuple: A tuple (CROSSRA0, ras) where CROSSRA0 is a boolean indicating if
      RA crosses zero, and ras is the adjusted RA array.
    """    # Make sure that they are numpy objetcs
    ras = np.array(ras)
    racmin = ras.min()
    racmax = ras.max()
    if (racmax - racmin) > 180.:
        # Currently we switch order. Perhaps better to +/-360.0?
        # Note we want the total extent which is not necessarily the maximum
        # and minimum in this case
        ras2 = ras
        wsm = np.where(ras > 180.0)
        ras2[wsm] = ras[wsm] - 360.
        CROSSRA0 = True
        ras = ras2
    else:
        CROSSRA0 = False
    return CROSSRA0, ras


def save_skymap_fits_trim(frame, fitsfile, field, hdr=None,
                          compress=False, overwrite=True):
    """
    Save a trimmed version of the sky map to a FITS file.
    Parameters:
    - frame: A frame object containing the map data.
    - fitsfile (str): The path to the output FITS file.
    - field (str): The field name to be used for trimming.
    - hdr (astropy.io.fits.Header): Header to be included in FITS (optional).
    - compress (bool): If True, compresses the FITS file.
    - overwrite (bool): If True, overwrites the existing FITS file.
    """
    if frame.type != core.G3FrameType.Map:
        logger.debug(f"Ignoring frame: {frame.type} -- not a FlatSkyMap or HealpixSkyMap")
        return frame

    if 'Coadd' in frame['Id']:
        logger.debug(f"Ignoring frame: {frame['Id']} -- is a Coadd frame")
        return frame

    ctype = None
    if compress is True:
        ctype = 'GZIP_2'
    elif isinstance(compress, str):
        ctype = compress

    # Get the T and weight frames
    T = frame['T']
    W = frame.get('Wpol', frame.get('Wunpol', None))

    data_sci = np.asarray(T)
    if W is not None:
        data_wgt = np.asarray(W.TT)
    logger.debug("Read data and weight")

    # Get the box size and center position to trim
    xc, yc, xsize, ysize = get_field_bbox(field, T.wcs)
    # Trim sci and wgt image using astropy cutour2D
    cutout_sci = Cutout2D(data_sci, (xc, yc), (ysize, xsize), wcs=T.wcs)
    if W is not None:
        cutout_wgt = Cutout2D(data_wgt, (xc, yc), (ysize, xsize), wcs=T.wcs)
    if hdr is None:
        hdr = maps.fitsio.create_wcs_header(T)
    hdr.update(cutout_sci.wcs.to_header())
    hdr_sci = copy.deepcopy(hdr)
    hdr_wgt = copy.deepcopy(hdr)
    hdr_wgt['ISWEIGHT'] = True

    hdul = astropy.io.fits.HDUList()
    if compress:
        logger.debug(f"Will compress using: {ctype} compression")
        hdu_sci = astropy.io.fits.CompImageHDU(
            data=cutout_sci.data,
            name='SCI',
            header=hdr_sci,
            compression_type=ctype)
        if W:
            hdu_wgt = astropy.io.fits.CompImageHDU(
                data=cutout_wgt.data,
                name='WGT',
                header=hdr_wgt,
                compression_type=ctype)
    else:
        hdu_sci = astropy.io.fits.ImageHDU(data=cutout_sci.data, header=hdr)
        if W:
            hdu_wgt = astropy.io.fits.ImageHDU(data=cutout_wgt.data, header=hdr)
    hdul.append(hdu_sci)
    if W:
        hdul.append(hdu_wgt)
    hdul.writeto(fitsfile, overwrite=overwrite)
    del data_sci
    del hdr_sci
    del hdu_sci
    if W:
        del data_wgt
        del hdr_wgt
        del hdu_wgt


def get_field_bbox(field, wcs, gridsize=100):
    """
    Get the image extent and central position in pixels for a given WCS.

    Parameters:
    - field (str): The name of the field.
    - wcs: WCS (World Coordinate System) object.
    - gridsize (int): Number of grid points along each axis.

    Returns:
    - tuple: (xc, yc, xsize, ysize) where xc, yc are the center coordinates
      and xsize, ysize are the image sizes in pixels.
    """
    deg = core.G3Units.deg
    (ra, dec) = sources.get_field_extent(field,
                                         ra_pad=1.5*deg,
                                         dec_pad=3*deg,
                                         sky_pad=True)
    # we convert back from G3units to degrees
    ra = (ra[0]/deg, ra[1]/deg)
    dec = (dec[0]/deg, dec[1]/deg)
    # Get the new ras corners in and see if we cross RA=0
    crossRA0, ra = crossRAzero(ra)
    # Create a grid of ra, dec to estimate the projected extent
    # for the frame WCS
    ras = np.linspace(ra[0], ra[1], gridsize)
    decs = np.linspace(dec[0], dec[1], gridsize)
    ra_grid, dec_grid = np.meshgrid(ras, decs)
    # Transform ra, dec grid to image positions using astropy
    (x_grid, y_grid) = wcs.wcs_world2pix(ra_grid, dec_grid, 0)
    # Get min, max values for x,y grid
    xmin = math.floor(x_grid.min())
    xmax = math.ceil(x_grid.max())
    ymin = math.floor(y_grid.min())
    ymax = math.ceil(y_grid.max())
    # Get the size in pixels rounded to the nearest hundred
    xsize = round((xmax - xmin), -2)
    ysize = round((ymax - ymin), -2)
    xc = round((xmax+xmin)/2.)
    yc = round((ymax+ymin)/2.)
    logger.debug(f"Found center: ({xc}, {yc})")
    logger.debug(f"Found size: ({xsize}, {ysize})")
    return xc, yc, xsize, ysize


def metadata_to_astropy_header(metadata):
    """
    Convert a dictionary of FITS metadata into an astropy.io.fits.Header object.

    Parameters
    ----------
    metadata : dict
        A dictionary where each key is a FITS header keyword (str), and each value
        is a tuple of the form (value, comment), where:
        - value : any
            The value to assign to the FITS header keyword.
        - comment : str
            A comment describing the keyword.

    Returns
    -------
    header : astropy.io.fits.Header
        An Astropy Header object populated with the given keywords, values, and comments.

    Examples
    --------
    >>> metadata = {
    ...     'OBJECT': ('M51', 'Target name'),
    ...     'EXPTIME': (1200.0, 'Exposure time in seconds')
    ... }
    >>> header = metadata_to_astropy_header(metadata)
    >>> print(header['OBJECT'])
    'M51'
    """
    header = astropy.io.fits.Header()
    for key, value in metadata.items():
        header[key] = (value[0], value[1])
    return header


def get_coadd_filename(band, season=None, field=None):

    """
    Figure out the correct coadd filename -- this will change over time.
    """

    short_season = season.replace("spt3g-", "")
    if season == 'spt3g-winter':
        filename = f"map_coadd_{band}_{short_season}_2019-2023_tonly.g3.gz"
    elif 'wide' in season:
        filename = f"map_coadd_{band}_{short_season}_yearAB_tonly.g3.gz"
    elif 'summer' in season:
        filename = f"map_coadd_{band}_{short_season}_2019-2022_tonly.g3"
    elif season == 'spt3g-galaxy':
        filename = f"map_coadd_{band}_{field}_2024_tonly.g3.gz"
    else:
        logger.warning(f"Cannot find filename for bans:{band}, season:{season}, field:{field}")
    return filename


def load_coadd_frame(g3coaddfile, g3coadds=None,
                     polarized=False, bands=['90GHz', '150GHz', '220GHz']):
    """
    Load frame(s) from single g3 coadd file
    g3coadds is a input/output dictionary that it is used when called
    from multiprocessing.
    """

    t0 = time.time()
    logger.info(f"Reading coadd file: {g3coaddfile}")

    # Loop over frames
    for frame in core.G3File(g3coaddfile):
        logger.debug(f"Reading frame: {frame}")

        if frame.type != core.G3FrameType.Map:
            continue
        # if frame["Id"] not in self.config.band:
        # Need to reformat the "Id" on the frame as it comes in the form of
        # Coadd{BAND} (i.e.: Coadd150GHz) and we just want to get the BAND
        # Extract just the relevant band from frame["Id"]
        try:
            band = re.findall("90GHz|150GHz|220GHz", frame['Id'])[0]
        except IndexError:
            band = frame['Id']
            continue
        if band not in bands:
            logger.warning(f"Ignoring frame: {frame['Id']} not in {bands}")
            continue
        if not polarized:
            maps.map_modules.MakeMapsUnpolarized(frame)
        maps.map_modules.RemoveWeights(frame, zero_nans=True)
        del frame["Wunpol"], frame["Wpol"]
        tmap = frame.pop("T")
        tmap.compact(zero_nans=True)

        # coaddId = COADD_ID.format(band=band, short_season=short_season)
        g3coadd_frame = {"T": tmap}
        logger.info(f"Loaded coadd frame for T: {band}")
        if polarized:
            for p in "QU":
                pmap = frame.pop(p)
                g3coadd_frame[p] = pmap
                # self.g3coadds[coaddId][p] = pmap
                logger.info("Loaded coadd frame for {p}")

        # Update dictionary in case it exists
        if g3coadds is not None:
            key = os.path.basename(g3coaddfile)
            g3coadds[key] = g3coadd_frame

        size_mb = sys.getsizeof(g3coadd_frame)/(1024 * 1024)
        logger.debug(f"Size of coadd frame: {size_mb}[Mb]")
        logger.info(f"Total time: {elapsed_time(t0)} for loading coadd: {g3coaddfile}")

        return g3coadd_frame


def get_archive_root():
    address = socket.getfqdn()
    if address.find('spt3g') >= 0:
        archive_root = '/data/spt3g'
    elif address.find('campuscluster') >= 0:
        archive_root = '/projects/ncsa/caps/spt3g'
    else:
        archive_root = ''
        logger.warning(f"archive_root undefined for: {address}")
    return archive_root


def get_coadd_archive_path():
    archive_root = get_archive_root()
    coadds_path = os.path.join(archive_root, "archive/transients-coadds")
    return coadds_path

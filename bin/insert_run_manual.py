#!/usr/bin/env python
import re
import sys
from astropy.time import Time
from spt3g_ingest import sqltools
import spt3g_ingest.data_types as data_types
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = False
logger = LOGGER


def extract_names_from_file(file_path, suffix='psth'):
    names = []
    # Build the regex pattern dynamically based on the suffix
    pattern = re.compile(rf'\d{{4}}/\d{{4}}-\d{{2}}/(.+?)_{suffix}\.fits')

    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            match = pattern.match(line)
            if match:
                names.append(match.group(1)+".g3.gz")

    return names


"""
To run:

cd /data/spt3g/dblib

insert_run_manual.py run1_psth.files psth
insert_run_manual.py run1_fltd.files fltd
insert_run_manual.py run1_cfltd.files cfltd

ls  2020/2020-*/*psth.fits > ~/run2_psth.files
ls  2020/2020-*/*cfltd.fits > ~/run2_cfltd.files
ls  2020/2020-*/*fltd.fits > ~/run2_fltd.files

insert_run_manual.py run2_psth.files psth
insert_run_manual.py run2_fltd.files fltd
insert_run_manual.py run2_cfltd.files cfltd
"""

run_files = sys.argv[1]
try:
    filetype = sys.argv[2]
except Exception:
    filetype = 'psth'

tablename = 'g3runinfo'
dbname = '/data/spt3g/dblib/spt3g_runs.db'

# Check that the table exists in the DB
sqltools.check_dbtable(dbname, tablename, Fd=data_types.g3RunFd)

# Get the names of the files
g3names = extract_names_from_file(run_files, suffix=filetype)

query_template = """
INSERT INTO {tablename} (ID, {filetype}, DATE_{filetype})
VALUES ('{g3file}', 1, '{date}')
ON CONFLICT(ID) DO UPDATE SET
    {filetype} = excluded.{filetype},
    DATE_{filetype} = excluded.DATE_{filetype}
"""

nfiles = len(g3names)
k = 1
for g3file in g3names:
    date = Time.now().isot
    print(f"Doing {k}/{nfiles} {filetype} -- {g3file}")
    query = query_template.format(**{'tablename': tablename,
                                     'g3file': g3file,
                                     'filetype': filetype,
                                     'date': date})
    sqltools.commit_with_retry(g3file, query, dbname)
    k = k + 1

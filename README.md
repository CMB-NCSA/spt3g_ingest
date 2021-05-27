# spt3g_ingest

Description
-----------

Set of tools to ingest and filter SPT3G maps as FITS files.

Example
-------
```bash
# Setup the path for spt3g_software and spt3g_ingest
source ~/spt3g_ingest/setpath.sh ~/spt3g_ingest 
/opt/spt/spt3g_software/build/env-shell.sh 

# Ingest all files on a directory:
g3_to_fits.py /data/spt3g/raw/*.g3.gz --outdir /data/spt3g/products/maps --compress --clobber

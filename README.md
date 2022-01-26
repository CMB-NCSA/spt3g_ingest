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

# Example 1:
# Ingest files in and just dump the raw maps
g3_worker /data/spt3g/raw/*.g3.gz --outdir /data/spt3g/products/maps --compresss GZIP_2 --clobber

# Example 2:
# Ingest files and filter them for transients
g3_worker /data/spt3g/raw/*.g3.gz --outdir /data/spt3g/products/maps --mask /data/spt3g/masks/mask_2021_50mJy.g3 --filter_transient --coadd /data/spt3g/raw/yearly_* --compress GZIP_2 --clobber  

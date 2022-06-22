# spt3g_ingest

## Description

Set of tools to ingest and filter g3 maps from SPT as FITS files, so they can be consumed by the thumbnail cutter `spt3g_cutter` that runs under the hood of the [**SPT3G Thumbnail Server**](http://spt3g.ncsa.illinois.edu) at NCSA.

Here we describe the current workflow which involves the following steps:
 1) Relocate (i.e. ingest) incoming files, 
 2) Filter and transform g3 files into FITS (done at ICC)
 3) Ingest into the database.

## Operational Steps

### 1. File Transfer and Relocation

During the observing season (or as needed) new `g3.gz` maps are manually synced from U. Chicago to the spt3g server at NCSA. The newly arrived files are staged in  the `/data/spt3g/incoming` folder until these are relocated to its permanent place `/data/spt3g/raw` where they live in a sub-folder keyed to `YYYY-MM`

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

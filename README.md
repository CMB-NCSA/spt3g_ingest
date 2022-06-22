# spt3g_ingest

## Description

Set of tools to ingest and filter g3 maps from SPT as FITS files, so they can be consumed by the thumbnail cutter `spt3g_cutter` that runs under the hood of the [**SPT3G Thumbnail Server**](http://spt3g.ncsa.illinois.edu) at NCSA.

Here we describe the current workflow which involves the following steps:
 1) Relocate (i.e. ingest) incoming files, 
 2) Filter and transform g3 files into FITS (done at ICC)
 3) Ingest into the database.

## Operational Steps

The `spt3g_ingest` tools run inside containers, which are defined in the [`spt3g_ingest-recipes`](https://github.com/CMB-NCSA/spt3g_ingest-recipes) repo, and available in docker hub. For testing and development we run inside docker containers on machines provisioned via [Radiant.](https://wiki.ncsa.illinois.edu/display/PUBCR/Radiant)
For the large jobs on the Illiois Campus Cluster (ICC) we use singularity images based on the Docker images.

For developing on Radiant we use a start-up script 

```
%> cat  ./start_docker.sh
NAME=spt3gingest
IMAGE=menanteau/spt3g_ingest
TAG=ubuntu_0.3.3_8a5d1d1c
docker run -ti \
       -h `hostname -s`-docker  \
       -v /data/spt3g:/data/spt3g\
       -v $HOME/spt3g-devel/home-felipe:/home/felipe\
       --name $NAME\
        $IMAGE:$TAG bash
```

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

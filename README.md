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

#### Relocation:

In order to relocate the incoming g3 files, the following command will move them to their final destination on `/data/spt3g/raw/YYYY-MM` (the `--dryrun` will not move the files): 
```
relocate_g3files /data/spt3g/incoming/*g3.gz  --outdir /data/spt3g/raw
```

It's a good idea to cordon the new files to a separate folder until we have fully processed them. Use the `--dryrun` to test first.

```
relocate_g3files /data/spt3g/incoming/*g3.gz  --outdir /data/spt3g/raw-2022Jun22
```
The script will write a file `manifest.txt` with the results of the relocation.

Once the files have been relocated, we need to transfer them to the mirror archive on the ICC. The best way to this is to use `bbcp` with a call that looks like:
```
 bbcp -s 12 -w 264000  -v -P 8 -N io "gtar  -c -O -C /data/spt3g/ raw-2022Jun22   " 'golubh2.campuscluster.illinois.edu:gtar  -x -C /projects/caps/spt3g/raw/'
```

### 2. Filter and transform g3 files into FITS 

This step is performed that the ICC, where a typical call looks like:
```
g3_worker ${INPUTLIST}
  --outdir /projects/caps/spt3g/2022-June-maps
  --mask /projects/caps/spt3g/masks/mask_2021_50mJy.g3
  --filter_transient
  --indirect_write
  --indirect_write_path /dev/shm
  --np ${cpus_per_task}
```

However, to submit a large batch job we use the module [spt3g_jobs](https://github.com/CMB-NCSA/spt3g_jobs) which is already installed on the ICC. These are the steps to submit a large job:

```
# Set the environment
source /projects/caps/spt3g/opt/miniconda3/bin/activate
```

```
# 320 wide all bands, 2022 data
chunk_filelist /projects/caps/spt3g/raw/2022-*/*g3.gz  --outdir ~/slurm-jobs/lists --base files_allbands_2022  --nchunks 40
create_jobs -c spt3g_ingest_320wide_allbands.yaml  --loop_list /home/felipe/slurm-jobs/lists/files_allbands_2022_main.list --submit_dir submit_dir_320wide.allbands_2022
sbatch submit_dir_320wide.allbands_2022/submitN.sl
```
### 4. Copy files back to archive and Ingest
To ingest the new files, for example:

```
ingest_fits /data/spt3g/products/maps/2022-06/*.fits --tablename file_info_v2
```

# Tools to load g3 files and manipulate/convert into FITS

from spt3g import core, maps
import os


def convert_to_fits(g3file, fitsfile=None, overwrite=True, compress=False):

    f1 = core.G3File(g3file)
    print(f"Loading: {g3file}")
    for frame in f1:
        if frame.type == core.G3FrameType.Map:
            if fitsfile is None:
                basename = get_basename(g3file)
                fitsfile = f"{basename}.fits"
            print(f"type:{frame.type} -- Id:{frame['Id']}")
            print(f"Saving {g3file} to: {fitsfile}")
            maps.fitsio.save_skymap_fits(fitsfile, frame['T'],
                                         overwrite=overwrite, compress=compress)
            print(f"Created: {fitsfile}")


def get_basename(g3file):
    """ Get the basename for a g3 file that could be compressed or not"""
    if os.path.splitext(g3file)[1] == '.gz':
        basename = os.path.splitext(os.path.splitext(g3file)[0])[0]
    else:
        basename = os.path.splitext(g3file)[0]
    return basename

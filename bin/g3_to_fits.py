#!/usr/bin/env python

import sys
from spt3g_ingest import fitstools

g3file = sys.argv[1]
fitsfile = sys.argv[2]
fitstools.convert_to_fits(g3file, fitsfile)

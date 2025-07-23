from collections import OrderedDict

# Create a ordered dictionary, order is important for ingestion
Fd = OrderedDict()

# From file info not in header
# INGESTION_ID is unique per table
Fd['ID'] = 'TEXT'
Fd['FILENAME'] = 'TEXT'
Fd['FILEPATH'] = 'TEXT'
Fd['INGESTION_DATE'] = 'TEXT'
Fd['SIZEINBYTES'] = 'INTEGER'
Fd['MD5SUM'] = 'TEXT'

# From header
Fd['NAXIS1'] = 'INT'
Fd['NAXIS2'] = 'INT'
Fd['WCSAXES'] = 'INT'
Fd['CRPIX1'] = 'FLOAT'
Fd['CRPIX2'] = 'FLOAT'
Fd['CDELT1'] = 'FLOAT'
Fd['CDELT2'] = 'FLOAT'
Fd['CUNIT1'] = 'VARCHAR(10)'
Fd['CUNIT2'] = 'VARCHAR(10)'
Fd['CTYPE1'] = 'VARCHAR(10)'
Fd['CTYPE2'] = 'VARCHAR(10)'
Fd['CRVAL1'] = 'FLOAT'
Fd['CRVAL2'] = 'FLOAT'
Fd['LONPOLE'] = 'FLOAT'
Fd['LATPOLE'] = 'FLOAT'
Fd['DATEREF'] = 'VARCHAR(20)'
Fd['MJDREFI'] = 'INT'
Fd['MJDREFF'] = 'INT'
Fd['RADESYS'] = 'VARCHAR(6)'
Fd['EQUINOX'] = 'INT'
Fd['PARENT'] = 'VARCHAR(100)'
Fd['FITSNAME'] = 'VARCHAR(100)'
Fd['FILETYPE'] = 'VARCHAR(100)'
Fd['DATE_BEG'] = 'TEXT'
Fd['DATE_END'] = 'TEXT'
Fd['OBJECT'] = 'VARCHAR(30)'
Fd['FIELD'] = 'VARCHAR(30)'
Fd['OBS_ID'] = 'VARCHAR(100)'
Fd['BAND'] = 'VARCHAR(10)'

# Create a ordered dictionary, order is important for ingestion
g3Fd = OrderedDict()

# From file info not in header
g3Fd['ID'] = 'TEXT'
g3Fd['FILENAME'] = 'TEXT'
g3Fd['FILEPATH'] = 'TEXT'
g3Fd['FILETYPE'] = 'VARCHAR(100)'
g3Fd['INGESTION_DATE'] = 'TEXT'
g3Fd['SIZEINBYTES'] = 'INTEGER'
g3Fd['MD5SUM'] = 'TEXT'
# From header
g3Fd['PARENT'] = 'VARCHAR(100)'
g3Fd['DATE_BEG'] = 'TEXT'
g3Fd['DATE_END'] = 'TEXT'
g3Fd['FIELD'] = 'VARCHAR(30)'
g3Fd['SEASON'] = 'VARCHAR(30)'
g3Fd['OBSID'] = 'VARCHAR(100)'
g3Fd['BAND'] = 'VARCHAR(10)'

# For the run table
g3RunFd = OrderedDict()
g3RunFd['ID'] = 'TEXT'
g3RunFd['PSTH'] = 'INT'
g3RunFd['FLTD'] = 'INT'
g3RunFd['CFLTD'] = 'INT'
g3RunFd['DATE_PSTH'] = 'TEXT'
g3RunFd['DATE_FLTD'] = 'TEXT'
g3RunFd['DATE_CFLTD'] = 'TEXT'

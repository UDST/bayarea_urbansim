from __future__ import print_function

import os
import string
import sys

import pandas as pd
import simplejson

from urbansim.utils import misc


if pd.version.version == "0.12.0":
    raise Exception("ERROR: Seriously, don't blame me, but Pandas .12 is broken")

if int(string.split(pd.version.version, '.')[1]) > 12:
    pd.options.mode.chained_assignment = None


sys.path.insert(0, ".")
import dataset
dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(), 'bayarea.h5'))

for year in range(1):
    print("Running year %d" % (year + 1))

    print("Running developer_run")
    import developer_run
    retval = developer_run.developer_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "developer_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    

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

num = misc.get_run_number()

sys.path.insert(0, ".")
import dataset
dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(), 'bayarea.h5'))

for year in range(30):
    print("Running year %d" % (year + 1))

    print("Running nrh_simulate")
    import nrh_simulate
    retval = nrh_simulate.nrh_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "nrh_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running elcm_simulate")
    import elcm_simulate
    retval = elcm_simulate.elcm_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "elcm_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running jobs_run")
    import jobs_run
    retval = jobs_run.jobs_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "jobs_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    
dset.save_output(os.path.join(misc.runs_dir(), 'run_%d.h5' % num))

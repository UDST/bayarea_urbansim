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

    print("Running networks_run")
    import networks_run
    retval = networks_run.networks_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "networks_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running rsh_simulate")
    import rsh_simulate
    retval = rsh_simulate.rsh_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "rsh_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running nrh_simulate")
    import nrh_simulate
    retval = nrh_simulate.nrh_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "nrh_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running networks2_run")
    import networks2_run
    retval = networks2_run.networks2_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "networks2_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running feasibility_run")
    import feasibility_run
    retval = feasibility_run.feasibility_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "feasibility_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    

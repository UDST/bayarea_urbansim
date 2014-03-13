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
    print("Running hlcms_simulate")
    import hlcms_simulate
    retval = hlcms_simulate.hlcms_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "hlcms_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running rrh_simulate")
    import rrh_simulate
    retval = rrh_simulate.rrh_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "rrh_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running hlcmr_simulate")
    import hlcmr_simulate
    retval = hlcmr_simulate.hlcmr_simulate(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "hlcmr_simulate.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running hhlds_run")
    import hhlds_run
    retval = hhlds_run.hhlds_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "hhlds_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running feasibility_run")
    import feasibility_run
    retval = feasibility_run.feasibility_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "feasibility_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    print("Running developer_run")
    import developer_run
    retval = developer_run.developer_run(dset, year=2010 + year)
    if retval:
        open(os.path.join(misc.output_dir(), "developer_run.json"), "w").write(simplejson.dumps(retval, sort_keys=True, indent=4))
    
dset.save_output(os.path.join(misc.runs_dir(), 'run_%d.h5' % num))

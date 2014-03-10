import os, sys, string
import simplejson
from synthicity.utils import misc
import pandas as pd

if pd.version.version == "0.12.0":
  raise Exception("ERROR: Seriously, don't blame me, but Pandas .12 is broken")

if int(string.split(pd.version.version,'.')[1]) > 12: pd.options.mode.chained_assignment = None


sys.path.insert(0,".")
import dataset
dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))

for year in range(1): 
  print "Running year %d" % (year+1)

  print "Running networks_run"
  import networks_run
  retval = networks_run.networks_run(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"networks_run.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running rrh_estimate"
  import rrh_estimate
  retval = rrh_estimate.rrh_estimate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"rrh_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running rrh_simulate"
  import rrh_simulate
  retval = rrh_simulate.rrh_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"rrh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running hlcmr_estimate"
  import hlcmr_estimate
  retval = hlcmr_estimate.hlcmr_estimate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"hlcmr_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
   


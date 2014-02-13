import os, sys
import simplejson
from synthicity.utils import misc

num = misc.get_run_number()

sys.path.insert(0,".")
import dataset
dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))

for year in range(3): 
  print "Running rsh_simulate"
  import rsh_simulate
  retval = rsh_simulate.rsh_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"rsh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running hlcms_simulate"
  import hlcms_simulate
  retval = hlcms_simulate.hlcms_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"hlcms_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running rrh_simulate"
  import rrh_simulate
  retval = rrh_simulate.rrh_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"rrh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running hlcmr_simulate"
  import hlcmr_simulate
  retval = hlcmr_simulate.hlcmr_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"hlcmr_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running nrh_simulate"
  import nrh_simulate
  retval = nrh_simulate.nrh_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"nrh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
  print "Running elcm_simulate"
  import elcm_simulate
  retval = elcm_simulate.elcm_simulate(dset,year=2010+year)
  if retval: open(os.path.join(misc.output_dir(),"elcm_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
   

dset.save_output(os.path.join(misc.runs_dir(),'run_%d.h5'%num))

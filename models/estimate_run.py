import os, sys
import simplejson
from synthicity.utils import misc

sys.path.insert(0,".")
import dataset
dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))

print "Running rsh_estimate"
import rsh_estimate
retval = rsh_estimate.rsh_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"rsh_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running rsh_simulate"
import rsh_simulate
retval = rsh_simulate.rsh_simulate(dset)
if retval: open(os.path.join(misc.output_dir(),"rsh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running hlcms_estimate"
import hlcms_estimate
retval = hlcms_estimate.hlcms_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"hlcms_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running rrh_estimate"
import rrh_estimate
retval = rrh_estimate.rrh_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"rrh_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running rrh_simulate"
import rrh_simulate
retval = rrh_simulate.rrh_simulate(dset)
if retval: open(os.path.join(misc.output_dir(),"rrh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running hlcmr_estimate"
import hlcmr_estimate
retval = hlcmr_estimate.hlcmr_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"hlcmr_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running nrh_estimate"
import nrh_estimate
retval = nrh_estimate.nrh_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"nrh_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running nrh_simulate"
import nrh_simulate
retval = nrh_simulate.nrh_simulate(dset)
if retval: open(os.path.join(misc.output_dir(),"nrh_simulate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
print "Running elcm_estimate"
import elcm_estimate
retval = elcm_estimate.elcm_estimate(dset)
if retval: open(os.path.join(misc.output_dir(),"elcm_estimate.json"),"w").write(simplejson.dumps(retval,sort_keys=True,indent=4))
 
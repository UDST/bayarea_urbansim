import sys
import dataset
import models
import time

args = sys.argv[1:]

assert len(args) >= 2
dsetname = args[0]
args = args[1:]

dset = dataset.LocalDataset(dsetname)
for arg in args:
    print "Executing %s" % arg
    t1 = time.time()
    getattr(models, arg)(dset)
    print "Finished executing in %.2fs" % (time.time()-t1)

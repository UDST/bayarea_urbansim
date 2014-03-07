import sys
sys.path.insert(0,'.')
import dataset
dset = dataset.BayAreaDataset('data/bayarea.h5')
import code
code.interact(local=locals())

import os, json, sys, time
import dataset,variables
from synthicity.utils import misc

args = sys.argv[1:]

if __name__ == '__main__':  
  dset = dataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))
  for arg in args: misc.run_model(arg,dset)   

import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys
from patsy import dmatrix

def hhlds_run(dset,year=None,show=True):
  assert "transitionmodel2" == "transitionmodel2" # should match!
  returnobj = {}
  t1 = time.time()
  
  # keep original indexes
  if "household_id" not in dset.households.columns: dset.households = dset.households.reset_index() 

  # TEMPLATE configure table
  households = dset.households
  # ENDTEMPLATE

  # TEMPLATE growth rate
  newdf = households[np.random.sample(len(households.index)) < 0.05]
  # ENDTEMPLATE

  # TEMPLATE fields to zero out
  newdf["building_id"] = -1
  # ENDTEMPLATE

  print "Adding %d rows" % len(newdf.index)
  if show: print newdf.describe()

  # concat and make index unique again
  dset.households = pd.concat([dset.households,newdf],axis=0).reset_index(drop=True)
  if show: print dset.households.describe()

  dset.store_attr("household_id",year,copy.deepcopy(dset.households.household_id))
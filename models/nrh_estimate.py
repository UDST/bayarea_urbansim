import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys

def nrh_estimate(dset,year=None,show=True):

  assert "hedonicmodel" == "hedonicmodel" # should match!
  returnobj = {}
  t1 = time.time()
  
  # TEMPLATE configure table
  buildings = dset.costar
  # ENDTEMPLATE

  # TEMPLATE merge 
  t_m = time.time()
  buildings = pd.merge(buildings,dset.nodes,**{u'right_index': True, u'left_on': u'_node_id'})
  print "Finished with merge in %f" % (time.time()-t_m)
  # ENDTEMPLATE
  
  print "Finished specifying in %f seconds" % (time.time()-t1)
  t1 = time.time()

  # TEMPLATE creating segments
  segments = buildings.groupby([u'general_type'])
  # ENDTEMPLATE
    
  for name, segment in segments:
    outname = "nrh" if name is None else "nrh_"+name
    
    # TEMPLATE computing vars
    est_data = pd.DataFrame(index=segment.index)
    est_data["accessibility"] = (segment.nets_all_regional1_30.apply(np.log1p)).astype('float')
    est_data["reliability"] = (segment.nets_all_regional2_30.apply(np.log1p)).astype('float')
    est_data["ln_stories"] = (segment.stories.apply(np.log1p)).astype('float')
    est_data = sm.add_constant(est_data,prepend=False)
    est_data = est_data.fillna(0)
    # ENDTEMPLATE

    # TEMPLATE dependent variable
    depvar = segment["averageweightedrent"]
    depvar = depvar.apply(np.log)
    # ENDTEMPLATE
    
    if name: print "Estimating hedonic for %s with %d observations" % (name,len(segment.index))
    if show: print est_data.describe()
    dset.save_tmptbl("EST_DATA",est_data) # in case we want to explore via urbansimd

    model = sm.OLS(depvar,est_data)
    results = model.fit()
    if show: print results.summary()

    misc.resultstocsv((results.rsquared,results.rsquared_adj),est_data.columns,
                        zip(results.params,results.bse,results.tvalues),outname+"_estimate.csv",hedonic=1,
                        tblname=outname)
    d = {}
    d['rsquared'] = results.rsquared
    d['rsquared_adj'] = results.rsquared_adj
    d['columns'] =  est_data.columns.tolist()
    d['est_results'] =  zip(results.params,results.bse,results.tvalues)
    returnobj[name] = d

    dset.store_coeff(outname,results.params.values,results.params.index)
      
  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj
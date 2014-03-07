import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys
from patsy import dmatrix

def rsh_estimate(dset,year=None,show=True):

  assert "hedonicmodel" == "hedonicmodel" # should match!
  returnobj = {}
  t1 = time.time()
  
  # TEMPLATE configure table
  units = dset.homesales
  units = units[units.unit_lot_size > 0]
  units = units[units.year_built > 1000]
  units = units[units.year_built < 2020]
  units = units[units.unit_sqft > 100]
  units = units[units.unit_sqft < 10000]
  units = units[units.sale_price_flt > 30]
  units = units[units.sale_price_flt < 1000]
  # ENDTEMPLATE

  # TEMPLATE merge 
  t_m = time.time()
  units = pd.merge(units,dset.nodes,**{'right_index': True, 'left_on': '_node_id'})
  print "Finished with merge in %f" % (time.time()-t_m)
  # ENDTEMPLATE
  
  print "Finished specifying in %f seconds" % (time.time()-t1)
  t1 = time.time()

  segments = [(None,units)]
    
  for name, segment in segments:
    name = str(name)
    outname = "rsh" if name is None else "rsh_"+name
    
    # TEMPLATE computing vars
    print "WARNING: using patsy, ind_vars will be ignored"
    est_data = dmatrix("I(year_built < 1940) + I(year_built > 2005) + np.log1p(unit_sqft) + np.log1p(unit_lot_size) + sum_residential_units + ave_unit_sqft + ave_lot_sqft + ave_income + poor + jobs + sfdu + renters", data=segment, return_type='dataframe')
    # ENDTEMPLATE

    # TEMPLATE dependent variable
    depvar = segment["sale_price_flt"]
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
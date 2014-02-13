import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy

def nrh_estimate(dset,year=None,show=True):

  assert "hedonicmodel" == "hedonicmodel" # should match!
  returnobj = {}
  t1 = time.time()
  
  # TEMPLATE configure table
  buildings = dset.fetch('costar')
  # ENDTEMPLATE

  # TEMPLATE merge 
  t_m = time.time()
  buildings = pd.merge(buildings,dset.fetch('nodes'),**{u'right_index': True, u'left_on': u'_node_id'})
  print "Finished with merge in %f" % (time.time()-t_m)
  # ENDTEMPLATE
  
  # TEMPLATE specifying output names
  output_csv, output_title, coeff_name, output_varname = [u'coeff-nonreshedonic-%s.csv', u'NON-RESIDENTIAL HEDONIC MODEL (%s)', u'nonresidential_rent_%s', u'nonresidential_rent']
  # ENDTEMPLATE

  print "Finished specifying in %f seconds" % (time.time()-t1)
  t1 = time.time()

  
  # TEMPLATE creating segments
  segments = buildings.groupby([u'general_type'])
  # ENDTEMPLATE
    
  for name, segment in segments:
    
    # TEMPLATE computing vars
    est_data = pd.DataFrame(index=segment.index)
    est_data["accessibility"] = (segment.nets_all_regional1_30.apply(np.log1p)).astype('float')
    est_data["reliability"] = (segment.nets_all_regional2_30.apply(np.log1p)).astype('float')
    est_data["ln_stories"] = (segment.stories.apply(np.log1p)).astype('float')
    est_data = sm.add_constant(est_data,prepend=False)
    est_data = est_data.fillna(0)
    # ENDTEMPLATE

    if name is not None: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
    else: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv, output_title, coeff_name
        
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

    tmp_outcsv = output_csv if name is None else output_csv%name
    tmp_outtitle = output_title if name is None else output_title%name
    misc.resultstocsv((results.rsquared,results.rsquared_adj),est_data.columns,
                        zip(results.params,results.bse,results.tvalues),tmp_outcsv,hedonic=1,
                        tblname=output_title)
    d = {}
    d['rsquared'] = results.rsquared
    d['rsquared_adj'] = results.rsquared_adj
    d['columns'] =  est_data.columns.tolist()
    d['est_results'] =  zip(results.params,results.bse,results.tvalues)
    returnobj[name] = d

    dset.store_coeff("nrh",results.params.values,results.params.index)
      
  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj
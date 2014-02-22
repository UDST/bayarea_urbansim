import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys
from patsy import dmatrix

SAMPLE_SIZE=100

def elcm_estimate(dset,year=None,show=True):

  assert "locationchoicemodel" == "locationchoicemodel" # should match!
  returnobj = {}
  
  # TEMPLATE configure table
  choosers = dset.nets
  # ENDTEMPLATE
  
  # TEMPLATE randomly choose estimatiors
  choosers = choosers.ix[np.random.choice(choosers.index, 10000,replace=False)]
  # ENDTEMPLATE
  # TEMPLATE specifying alternatives
  alternatives = dset.nodes.join(dset.variables.compute_nonres_building_proportions(dset,year))
  # ENDTEMPLATE
  
  t1 = time.time()

  # TEMPLATE creating segments
  segments = choosers.groupby(['naics11cat'])
  # ENDTEMPLATE
    
  for name, segment in segments:

    name = str(name)
    outname = "elcm" if name is None else "elcm_"+name

    global SAMPLE_SIZE
    sample, alternative_sample, est_params = interaction.mnl_interaction_dataset(
                                        segment,alternatives,SAMPLE_SIZE,chosenalts=segment["_node_id"])

    print "Estimating parameters for segment = %s, size = %d" % (name, len(segment.index)) 

    # TEMPLATE computing vars
    data = pd.DataFrame(index=alternative_sample.index)
    if 0: pass
    else:
      data["total sqft"] = (alternative_sample.totsum.apply(np.log1p)).astype('float')
      data["ln_weighted_rent"] = (alternative_sample.weightedrent.apply(np.log1p)).astype('float')
      data["retpct"] = alternative_sample["retpct"]
      data["indpct"] = alternative_sample["indpct"]
      data["accessibility"] = (alternative_sample.nets_all_regional1_30.apply(np.log1p)).astype('float')
      data["reliability"] = (alternative_sample.nets_all_regional2_30.apply(np.log1p)).astype('float')
    data = data.fillna(0)
    # ENDTEMPLATE
    if show: print data.describe()

    d = {}
    d['columns'] = fnames = data.columns.tolist()

    data = data.as_matrix()
    if np.amax(data) > 500.0:
      raise Exception("WARNING: the max value in this estimation data is large, it's likely you need to log transform the input")
    fit, results = interaction.estimate(data,est_params,SAMPLE_SIZE)
 
    fnames = interaction.add_fnames(fnames,est_params)
    if show: print misc.resultstotable(fnames,results)
    misc.resultstocsv(fit,fnames,results,outname+"_estimate.csv",tblname=outname)
    
    d['null loglik'] = float(fit[0])
    d['converged loglik'] = float(fit[1])
    d['loglik ratio'] = float(fit[2])
    d['est_results'] = [[float(x) for x in result] for result in results]
    returnobj[name] = d
    
    dset.store_coeff(outname,zip(*results)[0],fnames)

  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj


import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy

SAMPLE_SIZE=100

##############
#  ESTIMATION
##############

def elcm_estimate(dset,year=None,show=True):

  assert "locationchoicemodel" == "locationchoicemodel" # should match!
  returnobj = {}
  
  # TEMPLATE configure table
  choosers = dset.fetch('nets')
  # ENDTEMPLATE
  
    
  # TEMPLATE specifying output names
  output_csv, output_title, coeff_name, output_varname = [u'coeff-nonreslocation-rent-%s.csv', u'NON-RESIDENTIAL LOCATION CHOICE MODELS BY NAICS SECTOR (%s)', u'nonreslocation_%s', u'firms_building_ids']
  # ENDTEMPLATE
 
  # TEMPLATE specifying alternatives
  alternatives = dset.fetch('nodes').join(variables.compute_nonres_building_proportions(dset,year))
  # ENDTEMPLATE
  
  
  t1 = time.time()

    # TEMPLATE creating segments
  segments = choosers.groupby([u'naics11cat'])
  # ENDTEMPLATE
    
  for name, segment in segments:

    name = str(name)
    if name is not None: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
    else: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv, output_title, coeff_name

    # TEMPLATE dependent variable
    depvar = "_node_id"
    # ENDTEMPLATE
    global SAMPLE_SIZE
        
    sample, alternative_sample, est_params = interaction.mnl_interaction_dataset(
                                        segment,alternatives,SAMPLE_SIZE,chosenalts=segment[depvar])

    print "Estimating parameters for segment = %s, size = %d" % (name, len(segment.index)) 

    # TEMPLATE computing vars
    data = pd.DataFrame(index=alternative_sample.index)
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
    d['columns'] =  data.columns.tolist()
    data = data.as_matrix()
    fnames = [u'total sqft', u'ln_weighted_rent', u'retpct', u'indpct', u'accessibility', u'reliability']
    
    fit, results = interaction.estimate(data,est_params,SAMPLE_SIZE)
    
    fnames = interaction.add_fnames(fnames,est_params)
    if show: print misc.resultstotable(fnames,results)
    misc.resultstocsv(fit,fnames,results,tmp_outcsv,tblname=tmp_outtitle)
    
    d['null loglik'] = float(fit[0])
    d['converged loglik'] = float(fit[1])
    d['loglik ratio'] = float(fit[2])
    d['est_results'] = [[float(x) for x in result] for result in results]
    returnobj[name] = d
    
    dset.store_coeff(tmp_coeffname,zip(*results)[0],fnames)

  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj


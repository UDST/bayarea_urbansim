import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys
from patsy import dmatrix

SAMPLE_SIZE=100

def elcm_simulate(dset,year=None,show=True):

  returnobj = {}
  t1 = time.time()
  # TEMPLATE configure table
  jobs = dset.nets[dset.nets.lastmove>2007]
  # ENDTEMPLATE
  
  # TEMPLATE dependent variable
  depvar = "_node_id"
  # ENDTEMPLATE

  movers = jobs # everyone moves
  
  print "Total new agents and movers = %d (out of %d %s)" % (len(movers.index),len(jobs.index),"jobs")

  # TEMPLATE specifying alternatives
  alternatives = dset.nodes.join(dset.variables.compute_nonres_building_proportions(dset,year))
  # ENDTEMPLATE
  
  
  print "Finished specifying model in %f seconds" % (time.time()-t1)

  t1 = time.time()

  pdf = pd.DataFrame(index=alternatives.index) 
  # TEMPLATE creating segments
  segments = movers.groupby(['naics11cat'])
  # ENDTEMPLATE
  
  for name, segment in segments:

    segment = segment.head(1)

    name = str(name)
    outname = "elcm" if name is None else "elcm_"+name
  
    SAMPLE_SIZE = alternatives.index.size # don't sample
    sample, alternative_sample, est_params = \
             interaction.mnl_interaction_dataset(segment,alternatives,SAMPLE_SIZE,chosenalts=None)
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
    data = data.as_matrix()

    coeff = dset.load_coeff(outname)
    probs = interaction.mnl_simulate(data,coeff,numalts=SAMPLE_SIZE,returnprobs=1)
    pdf['segment%s'%name] = pd.Series(probs.flatten(),index=alternatives.index) 

  print "Finished creating pdf in %f seconds" % (time.time()-t1)
  if len(pdf.columns) and show: print pdf.describe()
  returnobj["elcm"] = misc.pandasdfsummarytojson(pdf.describe(),ndigits=10)
  pdf.describe().to_csv(os.path.join(misc.output_dir(),"elcm_simulate.csv"))
    
  return returnobj


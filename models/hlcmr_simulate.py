import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys

SAMPLE_SIZE=100

##############
#  ESTIMATION
##############

############
# SIMULATION
############

def hlcmr_simulate(dset,year=None,show=True):

  returnobj = {}

  t1 = time.time()
  # TEMPLATE configure table
  choosers = dset.fetch_households(tenure='rent')
  # ENDTEMPLATE
  
  # TEMPLATE dependent variable
  depvar = "_node_id"
  # ENDTEMPLATE

  movers = choosers # everyone moves
  
  print "Total new agents and movers = %d" % len(movers.index)

  # TEMPLATE specifying alternatives
  alternatives = dset.nodes.join(dset.variables.compute_res_building_averages(dset,year,sales=0,rent=1))
  # ENDTEMPLATE
  
  lotterychoices = False
  
  
  print "Finished specifying model in %f seconds" % (time.time()-t1)

  t1 = time.time()

  pdf = pd.DataFrame(index=alternatives.index) 
    # TEMPLATE creating segments
  segments = movers.groupby([u'income_quartile'])
  # ENDTEMPLATE
  
  for name, segment in segments:

    segment = segment.head(1)

    name = str(name)
    outname = "hlcmr" if name is None else "hlcmr_"+name
  
    SAMPLE_SIZE = alternatives.index.size # don't sample
    sample, alternative_sample, est_params = \
             interaction.mnl_interaction_dataset(segment,alternatives,SAMPLE_SIZE,chosenalts=None)
    # TEMPLATE computing vars
    data = pd.DataFrame(index=alternative_sample.index)
    data["ln_rent"] = (alternative_sample.rent.apply(np.log1p)).astype('float')
    data["accessibility"] = (alternative_sample.nets_all_regional1_30.apply(np.log1p)).astype('float')
    data["reliability"] = (alternative_sample.nets_all_regional2_30.apply(np.log1p)).astype('float')
    data["average_income"] = (alternative_sample.demo_averageincome_average_local.apply(np.log)).astype('float')
    data["ln_units"] = (alternative_sample.residential_units.apply(np.log1p)).astype('float')
    data["ln_renters"] = (alternative_sample.hoodrenters.apply(np.log1p)).astype('float')
    data = data.fillna(0)
    # ENDTEMPLATE
    data = data.as_matrix()

    coeff = dset.load_coeff(outname)
    probs = interaction.mnl_simulate(data,coeff,numalts=SAMPLE_SIZE,returnprobs=1)
    pdf['segment%s'%name] = pd.Series(probs.flatten(),index=alternatives.index) 

  print "Finished creating pdf in %f seconds" % (time.time()-t1)
  if len(pdf.columns) and show: print pdf.describe()
  returnobj[name] = misc.pandasdfsummarytojson(pdf.describe(),ndigits=10)
  pdf.describe().to_csv(os.path.join(misc.output_dir(),"hlcmr_simulate.csv"))
  t1 = time.time()
    
  
  
  print "Finished assigning agents in %f seconds" % (time.time()-t1)
  return returnobj


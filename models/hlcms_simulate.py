import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys
from patsy import dmatrix

SAMPLE_SIZE=100

def hlcms_simulate(dset,year=None,show=True):

  returnobj = {}
  t1 = time.time()
  # TEMPLATE configure table
  choosers = dset.fetch_households(tenure='sales')
  # ENDTEMPLATE
  
  # TEMPLATE dependent variable
  depvar = "_node_id"
  # ENDTEMPLATE

  movers = choosers # everyone moves
  
  print "Total new agents and movers = %d" % len(movers.index)

  # TEMPLATE specifying alternatives
  alternatives = dset.nodes.join(dset.variables.compute_res_building_averages(dset,year,sales=1,rent=0))
  # ENDTEMPLATE
  
  
  print "Finished specifying model in %f seconds" % (time.time()-t1)

  t1 = time.time()

  pdf = pd.DataFrame(index=alternatives.index) 
  # TEMPLATE creating segments
  segments = movers.groupby(['income_quartile'])
  # ENDTEMPLATE
  
  for name, segment in segments:

    segment = segment.head(1)

    name = str(name)
    outname = "hlcms" if name is None else "hlcms_"+name
  
    SAMPLE_SIZE = alternatives.index.size # don't sample
    sample, alternative_sample, est_params = \
             interaction.mnl_interaction_dataset(segment,alternatives,SAMPLE_SIZE,chosenalts=None)
    # TEMPLATE computing vars
    data = pd.DataFrame(index=alternative_sample.index)
    if 0: pass
    else:
      data["ln_price"] = (alternative_sample.sales_price.apply(np.log1p)).astype('float')
      data["sales_price x income"] = ((alternative_sample.sales_price*alternative_sample.HHINCOME).apply(np.log)).astype('float')
      data["accessibility"] = (alternative_sample.nets_all_regional1_30.apply(np.log1p)).astype('float')
      data["reliability"] = (alternative_sample.nets_all_regional2_30.apply(np.log1p)).astype('float')
      data["average_income"] = (alternative_sample.demo_averageincome_average_local.apply(np.log)).astype('float')
      data["income x income"] = ((alternative_sample.demo_averageincome_average_local*alternative_sample.HHINCOME).apply(np.log)).astype('float')
      data["ln_units"] = (alternative_sample.residential_units.apply(np.log1p)).astype('float')
    data = data.fillna(0)
    # ENDTEMPLATE
    data = data.as_matrix()

    coeff = dset.load_coeff(outname)
    probs = interaction.mnl_simulate(data,coeff,numalts=SAMPLE_SIZE,returnprobs=1)
    pdf['segment%s'%name] = pd.Series(probs.flatten(),index=alternatives.index) 

  print "Finished creating pdf in %f seconds" % (time.time()-t1)
  if len(pdf.columns) and show: print pdf.describe()
  returnobj["hlcms"] = misc.pandasdfsummarytojson(pdf.describe(),ndigits=10)
  pdf.describe().to_csv(os.path.join(misc.output_dir(),"hlcms_simulate.csv"))
    
  return returnobj


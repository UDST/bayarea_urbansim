import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy

SAMPLE_SIZE=100

##############
#  ESTIMATION
##############

############
# SIMULATION
############

def hlcms_simulate(dset,year=None,show=True):

  t1 = time.time()
  # TEMPLATE configure table
  choosers = dset.fetch_households(tenure='sales')
  # ENDTEMPLATE
  
  # TEMPLATE specifying output names
  output_csv, output_title, coeff_name, output_varname = [u'coeff-reslocation-sales-%s.csv', u'RESIDENTIAL LOCATION CHOICE MODELS (SALES-%s)', u'res_sales_location_%s', u'sales_household_building_ids']
  # ENDTEMPLATE
  
  # TEMPLATE dependent variable
  depvar = "_node_id"
  # ENDTEMPLATE

  movers = choosers # everyone moves
  
  print "Total new agents and movers = %d" % len(movers.index)

  # TEMPLATE specifying alternatives
  alternatives = dset.fetch('nodes').join(dset.variables.compute_res_building_averages(dset,year,sales=1,rent=0))
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
    if name is not None: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
    else: tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv, output_title, coeff_name
  
    SAMPLE_SIZE = alternatives.index.size # don't sample
    sample, alternative_sample, est_params = \
             interaction.mnl_interaction_dataset(segment,alternatives,SAMPLE_SIZE,chosenalts=None)
    # TEMPLATE computing vars
    data = pd.DataFrame(index=alternative_sample.index)
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

    coeff = dset.load_coeff(tmp_coeffname)
    probs = interaction.mnl_simulate(data,coeff,numalts=SAMPLE_SIZE,returnprobs=1)
    pdf['segment%s'%name] = pd.Series(probs.flatten(),index=alternatives.index) 

  print "Finished creating pdf in %f seconds" % (time.time()-t1)
  if len(pdf.columns): print pdf.describe()
  t1 = time.time()
    
  
  
  print "Finished assigning agents in %f seconds" % (time.time()-t1)


import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy, os, sys

def rsh_simulate(dset,year=None,show=True):

  assert "hedonicmodel" == "hedonicmodel" # should match!
  returnobj = {}
  t1 = time.time()
  
  # TEMPLATE configure table
  buildings = dset.building_filter(residential=1)
  # ENDTEMPLATE

  # TEMPLATE merge 
  t_m = time.time()
  buildings = pd.merge(buildings,dset.fetch('nodes'),**{u'right_index': True, u'left_on': u'_node_id'})
  print "Finished with merge in %f" % (time.time()-t_m)
  # ENDTEMPLATE
  
  print "Finished specifying in %f seconds" % (time.time()-t1)
  t1 = time.time()

  simrents = []
  segments = [(None,buildings)]
    
  for name, segment in segments:
    outname = "rsh" if name is None else "rsh_"+name
    
    # TEMPLATE computing vars
    est_data = pd.DataFrame(index=segment.index)
    est_data["historic"] = (segment.year_built < 1940).astype('float')
    est_data["new"] = (segment.year_built > 2005).astype('float')
    est_data["accessibility"] = (segment.nets_all_regional1_30.apply(np.log1p)).astype('float')
    est_data["reliability"] = (segment.nets_all_regional2_30.apply(np.log1p)).astype('float')
    est_data["average_income"] = (segment.demo_averageincome_average_local.apply(np.log)).astype('float')
    est_data["ln_unit_sqft"] = (segment.unit_sqft.apply(np.log1p)).astype('float')
    est_data["ln_lot_size"] = (segment.unit_lot_size.apply(np.log1p)).astype('float')
    est_data = sm.add_constant(est_data,prepend=False)
    est_data = est_data.fillna(0)
    # ENDTEMPLATE

      
    print "Generating rents on %d buildings" % (est_data.shape[0])
    vec = dset.load_coeff(outname)
    vec = np.reshape(vec,(vec.size,1))
    rents = est_data.dot(vec).astype('f4')
    rents = rents.apply(np.exp)
    simrents.append(rents[rents.columns[0]])
    returnobj[name] = misc.pandassummarytojson(rents.describe())
    rents.describe().to_csv(os.path.join(misc.output_dir(),"_simulate.csv"))
      
  simrents = pd.concat(simrents)
  dset.buildings["residential_sales_price"] = simrents.reindex(dset.buildings.index)
  dset.store_attr("residential_sales_price",year,simrents)

  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj
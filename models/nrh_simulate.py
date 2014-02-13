import pandas as pd, numpy as np, statsmodels.api as sm
from synthicity.urbanchoice import *
from synthicity.utils import misc
import time, copy

def nrh_simulate(dset,year=None,show=True):

  assert "hedonicmodel" == "hedonicmodel" # should match!
  returnobj = {}
  t1 = time.time()
  
  # TEMPLATE configure table
  buildings = dset.building_filter(residential=0)
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

  simrents = []
  
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
      
    print "Generating rents on %d buildings" % (est_data.shape[0])
    vec = dset.load_coeff("nrh")
    vec = np.reshape(vec,(vec.size,1))
    rents = est_data.dot(vec).astype('f4')
    rents = rents.apply(np.exp)
    simrents.append(rents[rents.columns[0]])
      
  simrents = pd.concat(simrents)
  dset.buildings[output_varname] = simrents.reindex(dset.buildings.index)
  dset.store_attr(output_varname,year,simrents)

  print "Finished executing in %f seconds" % (time.time()-t1)
  return returnobj
import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc, spotproforma

# TODO better way of doing this?
def get_possible_rents_by_use(dset):
  parcels = dset.parcels 
  # need a prevailing rent for each parcel
  nodeavgrents = pd.read_csv(os.path.join(misc.data_dir(),'avenodeprice.csv'),index_col='node_id')
  # convert from price per sqft to yearly rent per sqft
  nodeavgrents['rent'] = np.pmt(spotproforma.INTERESTRATE,spotproforma.PERIODS,nodeavgrents.price*-1)

  # need predictions of rents for each parcel
  avgrents = pd.DataFrame(index=parcels.index)
  avgrents['residential'] = nodeavgrents.rent.ix[parcels._node_id].values
  # make a stupid assumption converting the residential 
  # rent which I have to non-residential rent which I don't have
  avgrents['office'] = nodeavgrents.rent.ix[parcels._node_id].values*1.0
  avgrents['retail'] = nodeavgrents.rent.ix[parcels._node_id].values*.8
  avgrents['industrial'] = nodeavgrents.rent.ix[parcels._node_id].values*.5
  return avgrents

# TODO
# BIG QUESTION - should the above "possible rents" be greater than the
# here computed actual rents?  probably, right? 
def current_rent_per_parcel(far_predictions,avgrents):
  # this is bad - need the right rents for each type
  return far_predictions.total_sqft*avgrents.residential

DEV = None
def feasibility_run(dset,year=2010):

  global DEV
  if DEV is None: 
    print "Running pro forma"
    DEV = spotproforma.Developer()
  
  parcels = dset.fetch('parcels').join(dset.fetch('zoning_for_parcels'),how="left")

  avgrents = get_possible_rents_by_use(dset)

  # compute total_sqft on the parcel, total current rent, and current far
  far_predictions = pd.DataFrame(index=parcels.index)
  far_predictions['total_sqft'] = dset.buildings.groupby('parcel_id').building_sqft.sum().fillna(0)
  far_predictions['year_built'] = dset.buildings.groupby('parcel_id').year_built.min().fillna(1960)
  far_predictions['currentrent'] = current_rent_per_parcel(far_predictions,avgrents)
  far_predictions['parcelsize'] = parcels.shape_area*10.764
  far_predictions.parcelsize[far_predictions.parcelsize<300] = 300 # some parcels have unrealisticly small sizes

  print "Get zoning:", time.ctime()
  zoning = dset.fetch('zoning').dropna(subset=['max_far'])

  parcels = pd.merge(parcels,zoning,left_on='zoning',right_index=True) # only keeps those with zoning

  # need to map building types in zoning to allowable forms in the developer model 
  type_d = { 
  'residential': [1,2,3],
  'industrial': [7,8,9],
  'retail': [10,11],
  'office': [4],
  'mixedresidential': [12],
  'mixedoffice': [14],
  }
  

  # we have zoning by like 16 building types and rents/far predictions by 4 building types
  # so we have to convert one into the other - would probably be better to have rents
  # segmented by the same 16 building types if we had good observations for that
  parcel_predictions = pd.DataFrame(index=parcels.index)
  for form in ["residential"]: #type_d.iteritems():
    
    btypes = type_d[form]
    for btype in [3]:

      print form, btype
      # is type allowed
      tmp = parcels[parcels['type%d'%btype]=='t'][['max_far','max_height']] # is type allowed
      # at what far
      far_predictions['type%d_zonedfar'%btype] = tmp['max_far'] # at what far
      far_predictions['type%d_zonedheight'%btype] = tmp['max_height'] # at what height

      # do the lookup in the developer model - this is where the profitability is computed
      far_predictions[str(btype)+'_feasiblefar'], far_predictions[str(btype)+'_profit'] = \
                                           DEV.lookup(form,avgrents[spotproforma.uses].as_matrix(),\
                                           far_predictions.currentrent*2.5,far_predictions.parcelsize,
                                           far_predictions['type%d_zonedfar'%btype],
                                           far_predictions['type%d_zonedheight'%btype])

      far_predictions[str(btype)+'_feasiblefar'][far_predictions.year_built < 1945] = 0.0
      far_predictions[str(btype)+'_profit'][far_predictions.year_built < 1945] = 0.0
      #far_predictions[str(btype)+'_feasiblefar'][far_predictions.year_built > 1995] = 0.0
      #far_predictions[str(btype)+'_profit'][far_predictions.year_built > 1995] = 0.0

  far_predictions = far_predictions.join(avgrents)
  print "Feasibility and zoning\n", far_predictions.describe()
  far_predictions['currentrent'] /= .06
  far_predictions.to_csv('far_predictions.csv',index_col='parcel_id',float_format="%.2f")
  print "Finished developer", time.ctime()

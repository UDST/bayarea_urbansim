import dataset, simplejson as json, math
import pandas as pd, numpy as np
dset = dataset.BayAreaDataset('data/bayarea.h5')

keep = [u'building_id', u'parcel_id', u'building_type', u'residential_units', u'non_residential_sqft', u'building_sqft', u'stories', u'building_type_id', u'year_built', u'tenure']
buildings = dset.buildings.reset_index()[keep].to_dict('records')
buildings_d = {}
for building in buildings:
  pid = building['parcel_id']
  buildings_d.setdefault(pid,[])
  del building['parcel_id']
  if building['tenure'] == -1: del building['tenure']
  buildings_d[pid].append(building)

zoning = dset.zoning.reset_index().to_dict('records')
for zone in zoning:
  zone['max_far'] = round(zone['max_far'],2)
  types = []
  for i in range(1,15):
    fname = 'type%d'%i
    assert fname in zone
    if zone[fname] == "t": types.append(i)
    del zone[fname]
  if types: zone["allowable_types"] = types
  for key, value in zone.items():
    if type(value) == type(0.0) and math.isnan(value): 
      del zone[key]

zoning = dict([(zone['id'],zone) for zone in zoning])

parcels = dset.parcels.join(dset.zoning_for_parcels,how="left").reset_index()
keep = [u'parcel_id', u'shape_area', u'county_id', u'zone_id', u'x', u'y', u'_node_id', u'zoning']
parcels = parcels[keep].to_dict('records')
  
for parcel in parcels:
  parcel['shape_area'] = round(parcel['shape_area'],2)
  for key, value in parcel.items():
    if np.isnan(value): 
      del parcel[key]

def merge_parcel(parcel):
  if 'zoning' in parcel:
    if parcel['zoning'] in zoning: 
      parcel['zoning'] = zoning[parcel['zoning']]
    else:
      del parcel['zoning']
  if parcel['parcel_id'] in buildings_d:
    parcel['buildings'] = buildings_d[parcel['parcel_id']]
  return parcel

dlist = [json.dumps(merge_parcel(parcel))+"\n" for parcel in parcels]
open('parcels.json','w').writelines(dlist)

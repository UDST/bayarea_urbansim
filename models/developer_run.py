import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc

TARGETVACANCY = .15 # really should be a target per TAZ
MAXPARCELSIZE = 200000 # really need some subdivision
AVEUNITSIZE = 1300 # really should be an accessibility variable

def developer_run(dset,year=2010):
  numhh = len(dset.households.index)
  numunits = dset.buildings.residential_units.sum()
  print "Number of households: %d" % numhh
  print "Number of units: %d" % numunits
  assert TARGETVACANCY < 1.0
  targetunits = max(numhh/(1-TARGETVACANCY) - numunits,0)
  print "Current vacancy = %.2f" % (1-numhh/numunits)
  print "Target vacancy = %.2f, target of new units = %d" % (TARGETVACANCY,targetunits)

  df = pd.read_csv(os.path.join(misc.data_dir(),'far_predictions.csv'),index_col='parcel_id')
  fnames = ["type%d_profit"%i for i in range(1,4)]
  fnames_d = dict((fnames[i],i+1) for i in range(len(fnames)))
  df["max_profit"] = df[fnames].max(axis=1)
  df["max_btype"] = df[fnames].idxmax(axis=1).map(fnames_d)
  # this is a little bit wonky too - need to take the far from the max profit above
  df["max_feasiblefar"] = df[["type%d_feasiblefar"%i for i in range(1,4)]].max(axis=1)
  df = df[df.max_feasiblefar>0] # feasible buildings only for this building type
  df = df[df.parcelsize<MAXPARCELSIZE] 
  df['newsqft'] = df.parcelsize*df.max_feasiblefar
  df['newunits'] = np.ceil(df.newsqft/AVEUNITSIZE).astype('int')
  df['netunits'] = df.newunits-df.total_units
 
  choices = np.random.choice(df.index.values,size=len(df.index),replace=False, \
                                  p=(df.max_profit.values/df.max_profit.sum()))
  netunits = df.netunits.loc[choices]
  totunits = netunits.values.cumsum()
  ind = np.searchsorted(totunits,targetunits,side="right")
  build = choices[:ind]

  df = df.loc[build]
  
  print "Describe of buildings built\n", df.total_units.describe()
  print "Describe of profit\n", df.max_profit.describe()
  print "Buildings by type\n", df.max_btype.value_counts()
  print "Units by type\n", df.groupby('max_btype').netunits.sum().order(ascending=False)
  
  df = df.join(dset.parcels).reset_index()
  # format new buildings to concatenate with old buildings
  df['parcel_id'] = df.index
  df['building_type'] = df.max_btype
  df['residential_units'] = df.newunits
  df['non_residential_sqft'] = np.zeros(len(df.index))
  df['building_sqft'] = df.newsqft
  df['stories'] = df.max_feasiblefar
  df['building_type_id'] = df.max_btype
  df['year_built'] = np.ones(len(df.index))*year
  df['tenure'] = np.zeros(len(df.index))
  df['rental'] = df.tenure
  df['rent'] = np.zeros(len(df.index))
  df['general_type'] = 'Residential'
  df['county'] = np.zeros(len(df.index))
  df['id'] = np.zeros(len(df.index))
  df['building'] = np.zeros(len(df.index))
  df['scenario'] = np.zeros(len(df.index))
  df['unit_sqft'] = AVEUNITSIZE
  df['lot_size'] = df.shape_area*10.764
  df['unit_lot_size'] = df.lot_size/df.residential_units
  df = df[['parcel_id','building_type','residential_units','non_residential_sqft', u'building_sqft', u'stories', u'building_type_id', u'year_built', u'tenure', u'id', u'building', u'scenario', u'county', u'lot_size', u'rent', u'rental', u'general_type', u'unit_sqft', u'unit_lot_size','x','y','_node_id0','_node_id1','_node_id2','_node_id']]
  maxind = np.max(dset.buildings.index.values)
  df.index = df.index+maxind+1
  print df.describe()
  dset.buildings = pd.concat([dset.buildings,df],verify_integrity=True)
  dset.buildings.index.name = 'building_id'

import numpy as np, pandas as pd
import time, os
from synthicity.utils import misc
from synthicity.urbansim import dataset, networks
import warnings

warnings.filterwarnings('ignore',category=pd.io.pytables.PerformanceWarning)

USECHTS = 1

# this is the central location to do all the little data format issues that will be needed by all models

class BayAreaDataset(dataset.Dataset):

  def __init__(self,filename):
    super(BayAreaDataset,self).__init__(filename)

  def fetch(self,name,addnodeid=0,convertsrid=0,addbuildingid=0,pya=None,direct=0):

    if direct: return self.store[name]
    if name in self.d: return self.d[name]

    tbl = None
    if name == 'buildings': tbl = self.fetch_buildings()
    elif name == 'homesales': tbl = self.fetch_homesales()
    elif name == 'costar': tbl = self.fetch_costar()
    elif name == 'households': tbl = self.fetch_households()
    elif name == 'batshh': tbl = self.fetch_batshh()
    elif name == 'bats': tbl = self.fetch_bats()
    elif name == 'nets': tbl = self.fetch_nets()
    elif name == 'networks': tbl = self.fetch_networks()
    elif name == 'apartments': tbl = self.fetch_apartments()
    elif name == 'zoning_for_parcels': tbl = self.fetch_zoning_for_parcels()
    else: tbl = self.store[name]
   
    if addnodeid:
        assert self.networks 
        tbl = self.networks.addnodeid(tbl,convertsrid)

    if addbuildingid:
        assert self.networks
        tbl = self.networks.addbuildingid(self,tbl)

    self.d[name] = tbl
    return tbl

  def fetch_costar(self):
    costar = self.store['costar']
    costar = costar[costar['averageweightedrent']>0]
    costar['stories'] = costar['number_of_stories']
    costar['general_type'] = costar['PropertyType']
    costar = costar[costar.general_type.isin(["Office","Retail","Industrial","Flex"])]
    return costar

  def fetch_apartments(self):
    apartments = self.join_for_field(self.store['apartments'],'parcels','parcel_id','_node_id')
    apartments = apartments.dropna()
    apartments['rent'] = (apartments['MinOfLowRent']+apartments['MaxOfHighRent'])/2.0
    apartments['rent'] /= apartments['AvgOfSquareFeet']
    apartments['unit_sqft'] = apartments['AvgOfSquareFeet'] 
    return apartments

  def fetch_buildings(self,add_xy_from_parcels=1):
    buildings = self.store['buildings']

    buildings['rent'] = np.nan

    buildings['rental'] = buildings['tenure']==1

    for t in ['HS','HT','HM']: buildings['general_type'][buildings['building_type'] == t] = 'Residential'
    for t in ['RS','RB']: buildings['general_type'][buildings['building_type'] == t] = 'Retail'
    for t in ['IL','IW','IH']: buildings['general_type'][buildings['building_type'] == t] = 'Industrial'
    for t in ['OF']: buildings['general_type'][buildings['building_type'] == t] = 'Office'
    for t in ['HO','SC','MR','ME','MT','GV','RC','PS']: 
      buildings['general_type'][buildings['building_type'] == t] = 'Flex'

    buildings['residential_units'][buildings['residential_units'] == 0] = np.nan
    buildings['residential_units'] = buildings['residential_units'].fillna(1) 
    buildings['residential_units'][buildings['residential_units']>300] = 300

    buildings['stories'][buildings['stories'] == 0] = np.nan
    buildings['stories'] = buildings['stories'].fillna(1) 

    #buildings['year_built'] = buildings['year_built'][buildings['year_built'] < 1870] = np.nan
    #buildings['year_built'] = buildings['year_built'][buildings['year_built'] > 2015] = np.nan

    print "WARNING: imputing building square feet where it doesn't exist"
    buildings['building_sqft'][buildings['building_sqft'] == 0] = np.nan
    buildings['building_sqft'] = buildings['building_sqft'].fillna(buildings['building_sqft'].mean())
    buildings['unit_sqft'] = buildings['building_sqft'] / buildings['residential_units']
    buildings['unit_sqft'][buildings['unit_sqft']>3000] = 3000

    print "WARNING: imputing lot size where it doesn't exist"
    buildings['lot_size'][buildings['lot_size'] == 0] = np.nan
    buildings['lot_size'] = buildings['lot_size'].fillna(buildings['lot_size'].mean())
    buildings['unit_lot_size'] = buildings['lot_size'] / buildings['residential_units']

    if add_xy_from_parcels and 'x' not in buildings.columns: # don't add twice
        buildings = pd.merge(buildings,self.fetch('parcels',addnodeid=1)[['x','y']],
                                left_on='parcel_id',right_index=True,how='left')

    return buildings

  def fetch_households(self,tenure=None):

    households = self.store['households']
    households['income'][households['income'] < 0] = 0
    households['income_quartile'] = pd.qcut(households['income'],4).labels

    if tenure == "rent": households = households[households['tenure']==1]
    elif tenure == "sales": households = households[households['tenure']<>1]
    
    households['HHINCOME'] = households['income']/10000.0

    return households

  def fetch_homesales(self):

    homesales = self.store['homesales']

    homesales['Sale_price'] = homesales['Sale_price'].str.replace('$','')
    homesales['Sale_price'] = homesales['Sale_price'].str.replace(',','')
    homesales['Sale_price_flt'] = homesales['Sale_price'].astype('f4')
    homesales['Sale_price_flt'] /= homesales['SQft'] 
    homesales = homesales[homesales.SQft>0]
    homesales["year_built"] = homesales["Year_built"]
    homesales["unit_lot_size"] = homesales["Lot_size"]
    homesales["unit_sqft"] = homesales["SQft"]

    return homesales

  def fetch_nets(self):

    nets = self.store['nets']
    nets = self.join_for_field(nets,'buildings','building_id','_node_id')
    return nets

  def fetch_batshh(self,tenure=None):

    if USECHTS:
      batshh = pd.read_csv(os.path.join(misc.data_dir(),'bats2013MTC_household.csv')) 

      batshh = batshh[batshh['INCOM'] < 90] # remove bogus income records
      batshh['income_quartile'] = pd.qcut(batshh['INCOM'],4).labels
      batshh['HHINCOME'] = batshh['INCOM']
  
      if tenure == "sales": batshh = batshh[batshh['OWN']==1]
      elif tenure == "rent": batshh = batshh[batshh['OWN']==2]
    
      return batshh
    else: 
      batshh = self.store['batshh']

      batshh = batshh[batshh['HHINCOME'] < 16] # remove bogus income records
      batshh['income_quartile'] = pd.qcut(batshh['HHINCOME'],4).labels
  
      if tenure == "rent": batshh = batshh[batshh['TENURE']==1]
      elif tenure == "sales": batshh = batshh[batshh['TENURE']<>1]
    
      return batshh

  def fetch_zoning_for_parcels(self):

    zoning_for_parcels = self.store['zoning_for_parcels']

    zoning_for_parcels['index'] = zoning_for_parcels.index
    zoning_for_parcels = zoning_for_parcels.drop_duplicates(cols='index')
    del zoning_for_parcels['index']

    return zoning_for_parcels

  def fetch_bats(self):

    bats = self.store['bats']

    bats = bats[bats['HHINCOME'] < 16] # remove bogus income records
    bats = bats[bats['HHVEH'] < 10] # remove bogus vehicle records
    bats = bats[bats['AGE'] < 100] # remove bogus age records
    bats = bats.dropna(subset=['origpoint','destpoint']) # remove points without origins
    bats['EMPLOYED'] = bats['EMPLOYED'] == 1
    bats['FEMALE'] = bats['GENDER'] == 2
    bats['SINGLEFAMILY'] = bats['DWELLTYP'] == 1
    bats['OWNER'] = bats['TENURE'] == 2
    bats['CHILD'] = bats['AGE'] < 16 
    bats['YOUNGADULT'] = (bats['AGE'] >= 16) * (bats['AGE'] < 28)

    return bats

  def fetch_networks(self,reload=True,maxdistance=30,rootdir=None,custom_impedances=None):
    if not reload: return networks.Networks(os.path.join(misc.data_dir(),'network%d.pkl'))
    
    network = networks.Networks()
    network.process_network(maxdistance,rootdir,walkminutes=1,custom_impedances=custom_impedances)
    self.networks = network
    return self.networks

  # this is a shortcut function to join the table with dataset.fetch(tblname) 
  # using the foreign_key in order to add fieldname to the source table
  def join_for_field(self,table,tblname,foreign_key,fieldname):
    if type(table) == type(''): table = self.fetch(table)
    if foreign_key == None: # join on index
        return pd.merge(table,self.fetch(tblname)[[fieldname]],left_index=True,right_index=True)
    return pd.merge(table,self.fetch(tblname)[[fieldname]],left_on=foreign_key,right_index=True)

  # the norental and noowner leave buildings with unassigned tenure
  def building_filter(self,norental=0,noowner=0,residential=1,nofilter=0):
    buildings = self.fetch('buildings')
    if nofilter: return buildings
    if residential: buildings = buildings[(buildings['general_type'] == 'Residential')]
    else:           buildings = buildings[(buildings['general_type'] <> 'Residential')]
    if norental:    buildings = buildings[buildings['tenure'] <> 1]
    if noowner:     buildings = buildings[buildings['tenure'] <> 2]
    return buildings 

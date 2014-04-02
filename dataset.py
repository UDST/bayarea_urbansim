import numpy as np, pandas as pd
import time, os
from urbansim.utils import misc, geomisc, networks
from urbansim.urbansim import dataset
import warnings
import variables

warnings.filterwarnings('ignore',category=pd.io.pytables.PerformanceWarning)

USECHTS = 1

# this is the central location to do all the little data format issues that will be needed by all models

class BayAreaDataset(dataset.Dataset):

  BUILDING_TYPE_MAP = {
    1: "Residential",
    2: "Residential",
    3: "Residential",
    4: "Office",
    5: "Hotel",
    6: "School",
    7: "Industrial",
    8: "Industrial",
    9: "Industrial",
    10: "Retail",
    11: "Retail",
    12: "Residential",
    13: "Retail",
    14: "Office"
  }


  def __init__(self,filename):
    super(BayAreaDataset,self).__init__(filename)
    self.variables = variables

  def fetch(self,name,addzoneid=0,addnodeid=0,convertsrid=0,addbuildingid=0,pya=None,direct=0):
    if direct: return self.store[name]
    if name in self.d: return self.d[name]

    tbl = None
    if name == 'buildings': tbl = self.fetch_buildings()
    elif name == 'homesales': tbl = self.fetch_homesales()
    elif name == 'costar': tbl = self.fetch_costar()
    elif name == 'households': tbl = self.fetch_households()
    elif name == 'batshh': tbl = self.fetch_batshh()
    elif name == 'bats': tbl = self.fetch_bats()
    elif name == 'jobs': tbl = self.fetch_jobs()
    elif name == 'nets': tbl = self.fetch_nets()
    elif name == 'nodes': tbl = self.fetch_nodes()
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

    if addzoneid:
        tbl = self.join_for_field(tbl,'buildings','building_id','zone_id')

    self.d[name] = tbl
    return tbl

  def fetch_nodes(self):
    # default will fetch off disk unless networks have already been run
    print "WARNING: fetching precomputed nodes off of disk"
    return pd.read_csv(os.path.join(misc.data_dir(),'nodes.csv'),index_col='node_id')

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

    sqft_job = pd.read_csv(os.path.join(misc.configs_dir(),'building_sqft_job.csv'),index_col='building_type_id')
    buildings['sqft_per_job'] = sqft_job.ix[buildings.building_type_id.fillna(-1)].values
    buildings['non_residential_units'] = buildings.non_residential_sqft/buildings.sqft_per_job
    buildings['non_residential_units'] = buildings.non_residential_units.fillna(0).astype('int')
    del buildings['non_residential_sqft']

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
    
    buildings = self.join_for_field(buildings,'parcels','parcel_id','zone_id')

    return buildings

  def fetch_households(self):

    households = self.store['households']
    households['income'][households['income'] < 0] = 0
    households['income_quartile'] = pd.qcut(households['income'],4).labels

    households['HHINCOME'] = households['income']/10000.0
    
    return households

  def fetch_homesales(self):

    homesales = self.store['homesales']

    homesales['Sale_price'] = homesales['Sale_price'].str.replace('$','')
    homesales['Sale_price'] = homesales['Sale_price'].str.replace(',','')
    homesales['sale_price_flt'] = homesales['Sale_price'].astype('f4')
    homesales['sale_price_flt'] /= homesales['SQft'] 
    homesales = homesales[homesales.SQft>0]
    homesales["year_built"] = homesales["Year_built"]
    homesales["unit_lot_size"] = homesales["Lot_size"]
    homesales["unit_sqft"] = homesales["SQft"]

    return homesales

  def fetch_jobs(self):
    return self.fetch_nets()

  def fetch_nets(self):

    nets = self.store['nets']
    # go from establishments to jobs
    nets = nets.ix[np.repeat(nets.index.values,nets.emp11.values)].reset_index() 
    nets.index.name = 'job_id'
    #del nets['building_id']
    #nonres_buildlings = self.building_filter(residential=0)[['x','y']]
    #labels, dists = geomisc.spatial_join_nearest(nets,'x','y',nonres_buildlings,'x','y')
    #nets["building_id"] = labels
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

  # the norental and noowner leave buildings with unassigned tenure
  def building_filter(self,norental=0,noowner=0,residential=1,nofilter=0,year=None):
    buildings = self.buildings 
   
    #if year is not None:
    #  buildings['sales_price'] = self.load_attr('residential_sales_price',year)

    if nofilter: return buildings
    if residential: buildings = buildings[(buildings['general_type'] == 'Residential')]
    else:           buildings = buildings[(buildings['general_type'] <> 'Residential')]
    if norental:    buildings = buildings[buildings['tenure'] <> 1]
    if noowner:     buildings = buildings[buildings['tenure'] <> 2]
    return buildings 

LocalDataset = BayAreaDataset

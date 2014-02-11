import time, numpy as np, pandas as pd
from synthicity.utils import misc

# computes a weighted rent by non-residential building type and the proportion of each building type
def compute_nonres_building_proportions(dataset,year):

  buildings = dataset.fetch('buildings')
  buildings = buildings[buildings['general_type'] <> 'Residential']
  buildings['rent'] = dataset.load_attr('nonresidential_rent',year)

  tmp = buildings[['_node_id','building_sqft','general_type']]
  tmp['weightedrent'] = tmp['building_sqft']*buildings['rent']

  totsum = tmp.groupby('_node_id').sum().rename(columns={"building_sqft":"totsum"})
  totsum["weightedrent"] /= totsum["totsum"]
  del tmp["weightedrent"]

  offsum = tmp[tmp['general_type']=='Office'].groupby('_node_id').sum().rename(columns={"building_sqft":"offsum"})
  indsum = tmp[tmp['general_type']=='Industrial'].groupby('_node_id').sum().rename(columns={"building_sqft":"indsum"})
  retsum = tmp[tmp['general_type']=='Retail'].groupby('_node_id').sum().rename(columns={"building_sqft":"retsum"})

  nodesums = totsum.join(offsum).join(indsum).join(retsum)
  for fname in ["offsum","retsum","indsum"]: nodesums[fname] = nodesums[fname].fillna(0)
  for fname in ["off","ret","ind"]: nodesums[fname+"pct"] = nodesums[fname+"sum"]/nodesums["totsum"]

  return nodesums

def compute_households(dataset,year,rent=0,sales=0):

  households = dataset.join_for_field(dataset.fetch('households'),'buildings','building_id','_node_id')

  if rent: households = households[households['tenure']==1]
  if sales: households = households[households['tenure']<>1]

  nodesums = households[['_node_id','persons']].fillna(1).groupby('_node_id').sum() \
                                          .rename(columns={'persons':'noderenters'})

  return nodesums

def quantile_list(df, quantiles):
  return type(df)(dict([(q,df.quantile(q)) for q in quantiles]))

def discretize_population(segment,numsegments=None):
  width = 1.0/numsegments
  quantiles = np.arange(numsegments)*width+width/2.0
  return quantile_list(segment,quantiles).transpose()

def compute_res_building_empty_units(dataset,year,rent=0,sales=0):

  #if rent: assert 0
  buildings = dataset.building_filter(residential=1,norental=0,noowner=0)

  households = dataset.fetch('households')['building_id'].value_counts()

  if rent: buildings['rent'] = dataset.load_attr('residential_rent',year)
  else: buildings['rent'] = 0
  if sales: buildings['sales_price'] = dataset.load_attr('residential_sales_price',year)
  else: buildings['sales_price'] = 0

  supply = buildings['residential_units'].sub(households,fill_value=0)
  supply = supply[supply > 0]
  
  bids = np.repeat(supply.index,supply.values.astype('int'))
  buildings = buildings.ix[bids]
  buildings['residential_units'] = 1

  return buildings

def compute_res_building_averages(dataset,year,rent=0,sales=0):

  buildings = dataset.fetch('buildings')
  buildings = buildings[buildings['general_type'] == 'Residential']

  if rent: buildings['rent'] = dataset.load_attr('residential_rent',year)
  else: buildings['rent'] = 0
  if sales: buildings['sales_price'] = dataset.load_attr('residential_sales_price',year)
  else: buildings['sales_price'] = 0

  #return buildings

  nodeaverages = buildings[['_node_id','unit_sqft','unit_lot_size','rent','sales_price']] \
                                     .groupby('_node_id').mean()
  nodesums = buildings[['_node_id','residential_units']].groupby('_node_id').sum()

  return nodeaverages.join(nodesums)

def add_current_rents(dataset,buildings,year,rent=0,convert_res_rent_to_yearly=0,convert_res_sales_to_yearly=0):
  buildings['nonres_rent'] = dataset.load_attr('nonresidential_rent',year)
  if not rent: 
    buildings['res_rent'] = dataset.load_attr('residential_sales_price',year)
    if convert_res_sales_to_yearly: buildings.res_rent = np.pmt(developer.INTERESTRATE,developer.PERIODS,buildings.res_rent)*-1
  else: buildings['res_rent'] = dataset.load_attr('residential_rent',year)*(12 if convert_res_rent_to_yearly else 1)
  buildings['rent'] = buildings['nonres_rent'].combine_first(buildings['res_rent'])
  return buildings

def compute_access_variable(dataset,df,varname,agg=1,decay=2):
  assert dataset.networks.pya # need to generate pyaccess first
  pya = dataset.networks.pya
  pya.initializeAccVars(1)
  nodeidflds = ['_node_id%d'%i for i in range(pya.numgraphs)]
  nodes = np.transpose(np.asarray(df[nodeidflds],"int32"))
  num, dist = 0, 15.0
  pya.initializeAccVar(num,nodes,np.asarray(df[varname],dtype='float32'),preaggregate=0) 
  return pya.getAllWeightedAverages(num,localradius=15,regionalradius=30,minimumobservations=10,agg=agg,decay=decay) 

def compute_pems_variables(dataset,pya=None,year=None,impedancebasenumber=1):

  print "Computing accessibility variables"

  if pya is None: pya = dataset.networks.pya

  pya.initializeAccVars(4)
  nodeidflds = ['_node_id%d'%i for i in range(pya.numgraphs)]
  fullhouseholds = pd.merge(dataset.fetch('households'),dataset.fetch('buildings',addnodeid=1,pya=pya)[nodeidflds],
                                left_on='building_id',right_index=True,how='left')
  households = fullhouseholds[fullhouseholds['income'] > 0]
  households = households.dropna(subset=nodeidflds)
  households = households.dropna(subset=['income'])
  nodes = np.transpose(np.asarray(households[nodeidflds],"int32"))

  num, dist = 0, 15.0
  pya.initializeAccVar(num,nodes,np.asarray(households['income'],dtype='float32'),preaggregate=0) 
  averageincome = pya.getAllWeightedAverages(num,localradius=15,regionalradius=30,minimumobservations=10) 
  averageincome[np.where(averageincome<30000)] = 30000
  averageincome[np.where(averageincome>180000)] = 180000

  households = fullhouseholds[fullhouseholds['tenure']==1] 
  households = households.dropna(subset=nodeidflds)
  nodes = np.transpose(np.asarray(households[nodeidflds],dtype="int32"))

  num, dist = 3, 10.0
  pya.initializeAccVar(num,nodes,np.asarray(households['persons'].fillna(1),dtype='float32')) 
  renters = pya.getAllAggregateAccessibilityVariables(dist,num,pya.AGG_SUM,pya.DECAY_LINEAR)

  firms = pd.merge(dataset.fetch('nets',addnodeid=1,addbuildingid=1,pya=pya),
                                dataset.fetch('buildings',addnodeid=1,pya=pya)[nodeidflds],
                                left_on='building_id',right_index=True,how='left')
  firms = firms.dropna(subset=nodeidflds)
  nodes = np.transpose(np.asarray(firms[nodeidflds],dtype="int32"))

  num, dist = 1, 30
  pya.initializeAccVar(num,nodes,np.asarray(firms['emp11'].fillna(1),dtype='float32'))

  accessibility = pya.getAllAggregateAccessibilityVariables(dist,num,pya.AGG_SUM,pya.DECAY_LINEAR,gno=1,
                                                                                 impno=impedancebasenumber)
  accessibility = pya.convertgraphs(1,0,accessibility)
  reliability =   pya.getAllAggregateAccessibilityVariables(dist,num,pya.AGG_SUM,pya.DECAY_LINEAR,gno=1,
                                                                                 impno=impedancebasenumber+1)
  reliability = pya.convertgraphs(1,0,reliability)
  reliability = accessibility-reliability

  xys = np.transpose(pya.getgraphxys())

  d = {'demo_averageincome_average_local': averageincome,
       'nets_all_regional1_30':            accessibility,
       'nets_all_regional2_30':            reliability,
       'hoodrenters':            		   renters,
       'x':                                xys[0],
       'y':                                xys[1]}

  df = pd.DataFrame(d,index=pya.getGraphIDS())
  print df.describe()
  return df

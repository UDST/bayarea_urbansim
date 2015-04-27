import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db
import statsmodels.formula.api as smf
import statsmodels.api as sm
from spandex.targets import scaling as scl

#Connect to the database
loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

# Load TAZ residential unit control totals and other zonal targets (used as additional source of zonal explanatory variables in the price imputation)
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')

taz_controls_csv2 = loader.get_path('hh/tazsumm_redfin.csv')
targetvalues = pd.read_csv(taz_controls_csv2, index_col='taz')

#Load the core data tables.  Only buildlings table will be updated (the price attributes)
buildings = db_to_df('select * from buildings').set_index('building_id')
parcels = db_to_df('select * from parcels')
# Load TAZ-level synthetic population
hh_path = loader.get_path('hh/synth/hhFile.p2011s3a1.2010.csv')
hh = pd.read_csv(hh_path)
hh = hh.set_index('HHID')
hh.index.name = 'household_id'
hh = hh.rename(columns = {'TAZ':'taz'})

targetunits['area'] = parcels.groupby('taz').calc_area.sum()
targetunits.area[targetunits.area.isnull()] = targetunits.area.mean()

targetunits['empdensity'] = targetunits.etot_10/targetunits.area
targetunits['resdensity'] = targetunits.targetunits/targetunits.area
targetunits['mf_sf_ratio'] = targetunits.targetMF/(targetunits.targetSF+1)
targetunits['nr_res_ratio'] = targetunits.targetnonressqft/(targetunits.targetunits+1)

targetunits['mean_income'] = hh.groupby('taz').HINC.mean()
targetunits['mean_hhsize'] = hh.groupby('taz').PERSONS.mean()
targetunits['mean_hhchildren'] = hh.groupby('taz').NOC.mean()
targetunits['mean_numvehicles'] = hh.groupby('taz').VEHICL.mean()

for col in ['mean_income', 'mean_hhsize', 'mean_hhchildren', 'mean_numvehicles']:
    targetunits[col][targetunits[col].isnull()] = targetunits[col].mean()

buildings = pd.merge(buildings, targetvalues, left_on = 'taz', right_index = True, how = 'left')
buildings = pd.merge(buildings, targetunits, left_on = 'taz', right_index = True, how = 'left')


#Residential price imputation
resprice_estimation_dataset = buildings[(buildings.sqft_per_unit > 200) & (buildings.redfin_sale_year > 2009) & (buildings.redfin_sale_price > 90000) & (buildings.redfin_sale_price < 9000000) & (buildings.res_type.isin(['single', 'multi']))]
specification = 'np.log(redfin_sale_price) ~ I(year_built < 1940) + I(year_built > 1990) + year_built + mean_income + mean_hhsize + mean_hhchildren + mean_numvehicles + mf_sf_ratio + resdensity + empdensity + nr_res_ratio + yearbuilt_av + yearbuilt_sd + sqft_per_unit + stories + I(res_type == "multi") + I(county_id == 1) + I(county_id == 13) + I(county_id == 41) + I(county_id == 55) + I(county_id == 85) + I(county_id == 81) + I(county_id == 95) + I(county_id == 97)'
model = smf.ols(formula=specification, data=resprice_estimation_dataset)
results = model.fit()
print results.summary()

resbuildings = buildings[buildings.res_type.isin(['single', 'multi']) & (buildings.residential_units > 0)]
sim_data = results.predict(resbuildings)
sim_data = np.exp(sim_data)
sim_data = pd.Series(sim_data, index = resbuildings.index)
buildings['res_price'] = 0
buildings['res_price_per_sqft'] = 0
buildings.loc[sim_data.index,'res_price'] = sim_data
#Now that regression equation is applied, scale residential prices to match zonal target
targets_residential_price = pd.DataFrame(
    {'column_name': ['res_price']*len(targetvalues),
     'target_value': targetvalues.salepr2010_av.values,
     'target_metric': ['mean']*len(targetvalues),
     'filters': ('(residential_units > 0) & (taz == ' + pd.Series(targetvalues.index.values).astype('str')) + ')',
     'clip_low': [np.nan]*len(targetvalues),
     'clip_high': [np.nan]*len(targetvalues),
     'int_result': [np.nan]*len(targetvalues)})
buildings = scl.scale_to_targets_from_table(buildings, targets_residential_price)

buildings.res_price_per_sqft[(buildings.res_price > 0) * (buildings.sqft_per_unit > 0)] = buildings.res_price/buildings.sqft_per_unit

#Nonresidential price imputation
nonresprice_estimation_dataset = buildings[(buildings.costar_property_type.str.len()>2) & (buildings.res_type == 'other') & (~buildings.costar_rent.isin(['', '-', 'Negotiable', 'Withheld']))]
nonresprice_estimation_dataset['observed_costar_rent'] = nonresprice_estimation_dataset.costar_rent.astype('float')

specification = 'np.log(observed_costar_rent) ~ non_residential_sqft + targetnonressqft + I(development_type_id == "OF") + I(development_type_id == "RT") + I(year_built < 1940) + I(year_built > 1990) + year_built + mean_income + mean_hhsize + mean_hhchildren + mean_numvehicles + mf_sf_ratio + resdensity + empdensity + nr_res_ratio + yearbuilt_av + yearbuilt_sd + stories + I(county_id == 1) + I(county_id == 13) + I(county_id == 41) + I(county_id == 55) + I(county_id == 85) + I(county_id == 81) + I(county_id == 95) + I(county_id == 97) + e11_10 + e21_10 + e22_10 + e23_10 + e3133_10 + e42_10 + e4445_10 + e4849_10 + e51_10 + e52_10 + e53_10 + e54_10 + e55_10 + e56_10 + e61_10 + e62_10 + e71_10 + e72_10 + e81_10 + e92_10 + etot_10'
model = smf.ols(formula=specification, data=nonresprice_estimation_dataset)
results = model.fit()
print results.summary()

nonresbuildings = buildings[(buildings.res_type == 'other') & (buildings.non_residential_sqft > 0)]
sim_data = results.predict(nonresbuildings)
sim_data = np.exp(sim_data)
sim_data = pd.Series(sim_data, index = nonresbuildings.index)
buildings['nonres_rent_per_sqft'] = 0
buildings.loc[sim_data.index,'nonres_rent_per_sqft'] = sim_data

summary_output_path = loader.get_path('out/regeneration/summaries/price_summary.csv')
pd.DataFrame({'avg_nonres_rent_per_sqft':buildings[buildings.nonres_rent_per_sqft>0].groupby('taz').nonres_rent_per_sqft.mean(), 'avg_res_price_per_sqft':buildings[buildings.res_price_per_sqft>0].groupby('taz').res_price_per_sqft.mean(),}).to_csv(summary_output_path)

##Now export back to the database
buildings2 = buildings[['parcel_id','county_id', 'land_use_type_id', 'res_type', 'improvement_value', 'year_assessed', 'year_built', 'building_sqft', 'non_residential_sqft', 'residential_units', 'sqft_per_unit', 'stories', 'development_type_id', 'taz', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_property_type', 'costar_rent', 'nonres_rent_per_sqft', 'res_price_per_sqft']]

df_to_db(buildings2, 'buildings', schema=loader.tables.public)
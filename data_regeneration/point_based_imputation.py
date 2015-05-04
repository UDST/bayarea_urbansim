
import pandas as pd, numpy as np
import pandas.io.sql as sql
from pandas.io.excel import read_excel
from spandex.io import exec_sql,  df_to_db
from spandex import TableLoader

loader = TableLoader()

##Read Redfin CSV and load to database
redfin_csv_path = loader.get_path('built/bldg/homeprices/redfin_03feb14.csv')
redfin = pd.read_csv(redfin_csv_path)
redfin.index.name = 'idx'
df_to_db(redfin, 'redfin', schema=loader.tables.staging)

##Lat/long to point geometry, with the right SRID
exec_sql("ALTER TABLE staging.redfin ADD COLUMN geom geometry;")
exec_sql("UPDATE staging.redfin SET geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326);")
exec_sql("CREATE INDEX redfin_gidx on staging.redfin using gist (geom);")
exec_sql("SELECT UpdateGeometrySRID('staging', 'redfin', 'geom', 2768);")
exec_sql("UPDATE staging.redfin SET geom = ST_TRANSFORM(ST_SetSRID(geom, 4326), 2768);")

##Append the unique parcel identifier to the Redfin records
exec_sql("alter table staging.redfin add gid integer default 0;")
exec_sql("update staging.redfin set gid = a.gid from parcels a where st_within(staging.redfin.geom, a.geom);")

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

##Load as dataframes for the imputation
parcels = db_to_df('select * from parcels;')
redfin = db_to_df('select * from staging.redfin;')

#Imputation-  incorporate Redfin data into the parcel table

redfin_year_built = redfin.groupby('gid').yearbuilt.median()
redfin_sqft = redfin.groupby('gid').sqft.median()

redfin_year_built = redfin_year_built[(redfin_year_built>1800)&(redfin_year_built<2016)]
redfin_sqft = redfin_sqft[(redfin_sqft>199)&(redfin_sqft<25000)]

parcels = parcels.set_index('gid')
parcels['redfin_year_built'] = redfin_year_built
parcels['redfin_sqft_per_unit'] = redfin_sqft

#If null year_built, just take the Redfin year built value no matter what
idx_imputed = parcels.year_built.isnull()*parcels.redfin_year_built.isnull()  ## These are the records that will be imputed
parcels.year_built[parcels.year_built.isnull()] = parcels.redfin_year_built[parcels.year_built.isnull()]
parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + ', rf_yrblt'  ## Populate imputation flag for affected records

#If null sqft_per_unit, just take the Redfin sqft_per_unit value no matter what
idx_imputed = parcels.sqft_per_unit.isnull()*parcels.redfin_sqft_per_unit.isnull()
parcels.sqft_per_unit[parcels.sqft_per_unit.isnull()] = parcels.redfin_sqft_per_unit[parcels.sqft_per_unit.isnull()]
parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + ', rf_sqftunit'

#If year_built out of bounds, and Redfin value exists, take Redfin value
out_of_bounds = np.logical_or((parcels.year_built < 1800), (parcels.year_built > 2015))
redfin_available = ~parcels.redfin_year_built.isnull()
parcels.year_built[out_of_bounds*redfin_available] = parcels.redfin_year_built[out_of_bounds*redfin_available]
parcels.imputation_flag[out_of_bounds*redfin_available] = parcels.imputation_flag[out_of_bounds*redfin_available] + ', rf_yrblt'

#If sqft_per_unit out of bounds, and Redfin value exists, take Redfin value
out_of_bounds = np.logical_or((parcels.sqft_per_unit < 150), (parcels.sqft_per_unit > 50000))
redfin_available = ~parcels.redfin_sqft_per_unit.isnull()
parcels.sqft_per_unit[out_of_bounds*redfin_available] = parcels.redfin_sqft_per_unit[out_of_bounds*redfin_available]
parcels.imputation_flag[out_of_bounds*redfin_available] = parcels.imputation_flag[out_of_bounds*redfin_available] + ', rf_sqftunit'

#Append other Redfin data to parcels table, just for future reference/imputation/modeling
redfin_sale_price = redfin.groupby('gid').lastsalepr.median()
redfin_sale_year = redfin.groupby('gid').saleyear.median()
redfin_home_type = redfin.groupby('gid').hometype.max()

parcels['redfin_sale_price'] = redfin_sale_price
parcels['redfin_sale_year'] = redfin_sale_year
parcels['redfin_home_type'] = redfin_home_type

parcels_redfin = parcels[['year_built', 'sqft_per_unit', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'imputation_flag']]
parcels_redfin.redfin_home_type[parcels_redfin.redfin_home_type.isnull()] = ''

df_to_db(parcels_redfin, 'parcels_redfin', schema=loader.tables.staging)

#Update the master parcel table on the database
exec_sql("update parcels set year_built = a.year_built from staging.parcels_redfin a where a.gid = parcels.gid;")
exec_sql("update parcels set sqft_per_unit = a.sqft_per_unit from staging.parcels_redfin a where a.gid = parcels.gid;")
exec_sql("update parcels set imputation_flag = a.imputation_flag from staging.parcels_redfin a where a.gid = parcels.gid;")

exec_sql("ALTER TABLE parcels ADD COLUMN redfin_sale_price numeric;")
exec_sql("ALTER TABLE parcels ADD COLUMN redfin_sale_year numeric;")
exec_sql("ALTER TABLE parcels ADD COLUMN redfin_home_type text;")

exec_sql("update parcels set redfin_sale_price = a.redfin_sale_price from staging.parcels_redfin a where a.gid = parcels.gid;")
exec_sql("update parcels set redfin_sale_year = a.redfin_sale_year from staging.parcels_redfin a where a.gid = parcels.gid;")
exec_sql("update parcels set redfin_home_type = a.redfin_home_type from staging.parcels_redfin a where a.gid = parcels.gid;")


##########  COSTAR

costar_xls_path = loader.get_path('built/bldg/costar/2011/costar_allbayarea.xlsx')
costar = pd.read_excel(costar_xls_path)

costar_solano_path = loader.get_path('built/bldg/costar/2011/costar__clean2011_sol_020315.csv')
costar_sol = pd.read_csv(costar_solano_path)

costar2 = costar[['PropertyID', 'Building Name', 'Latitude', 'Longitude', 'Rentable Building Area', 'Year Built', 'PropertyType', 'Secondary Type', 'Total Available Space (SF)', 'Number Of Elevators', 'Last Sale Date', 'Last Sale Price', 'Average Weighted Rent', 'Number Of Stories']]
costar_sol2 = costar_sol[['PropertyID', 'Building Name', 'Latitude', 'Longitude', 'Rentable Building Area', 'Year Built', 'PropertyType', 'Secondary Type', 'Total Available Space (SF)', 'Number Of Elevators', 'Last Sale Date', 'Last Sale Price', 'Average Weighted Rent', 'Number Of Stories']]

costar2.columns = ['propertyid', 'building_name', 'latitude', 'longitude', 'rentable_area', 'year_built', 'property_type', 'secondary_type', 'available_space', 'elevators', 'last_sale_date', 'last_sale_price', 'rent', 'stories']
costar_sol2.columns = ['propertyid', 'building_name', 'latitude', 'longitude', 'rentable_area', 'year_built', 'property_type', 'secondary_type', 'available_space', 'elevators', 'last_sale_date', 'last_sale_price', 'rent', 'stories']

costar2 = pd.concat([costar2, costar_sol2])

for tex_col in ['building_name', 'property_type', 'secondary_type', 'last_sale_date', ]:
       costar2[tex_col] = costar2[tex_col].fillna(' ')
       costar2[tex_col] = costar2[tex_col].str.encode('utf-8')

costar2.last_sale_date = costar2.last_sale_date.fillna(' ')
costar2.last_sale_price = costar2.last_sale_price.str.replace(",", "").astype('float')
costar2.stories = costar2.stories.fillna(0).astype('int32')

costar2.index.name = 'idx'

df_to_db(costar2, 'costar', schema=loader.tables.staging)

##Lat/long to point geometry, with the right SRID
exec_sql("ALTER TABLE staging.costar ADD COLUMN geom geometry;")
exec_sql("UPDATE staging.costar SET geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326);")
exec_sql("CREATE INDEX costar_gidx on staging.costar using gist (geom);")
exec_sql("SELECT UpdateGeometrySRID('staging', 'costar', 'geom', 2768);")
exec_sql("UPDATE staging.costar SET geom = ST_TRANSFORM(ST_SetSRID(geom, 4326), 2768);")

##Append the unique parcel identifier to the Costar records
exec_sql("alter table staging.costar add gid integer default 0;")
exec_sql("update staging.costar set gid = a.gid from parcels a where st_within(staging.costar.geom, a.geom);")

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

##Load as dataframes for the imputation
parcels = db_to_df('select * from parcels;')
costar = db_to_df('select * from staging.costar;')

#Imputation-  incorporate Costar data into the parcel table

costar_year_built = costar.groupby('gid').year_built.median()
costar_sqft = costar.groupby('gid').rentable_area.median()
costar_stories = costar.groupby('gid').stories.max()

costar_year_built = costar_year_built[(costar_year_built>1800)&(costar_year_built<2016)]
costar_sqft = costar_sqft[(costar_sqft>199)&(costar_sqft<2000000)]
costar_stories = costar_stories[costar_stories > 0]

parcels = parcels.set_index('gid')
parcels['costar_year_built'] = costar_year_built
parcels['costar_non_residential_sqft'] = costar_sqft
parcels['costar_stories'] = costar_stories

#If year_built null, just take the Costar value no matter what
idx_imputed = parcels.year_built.isnull()*(~parcels.costar_year_built.isnull())
parcels.year_built[parcels.year_built.isnull()] = parcels.costar_year_built[parcels.year_built.isnull()]
parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + ', cs_yrblt'

#If non-residential sqft null, just take the Costar value no matter what
idx_imputed = parcels.non_residential_sqft.isnull()*(~parcels.costar_non_residential_sqft.isnull())
parcels.non_residential_sqft[parcels.non_residential_sqft.isnull()] = parcels.costar_non_residential_sqft[parcels.non_residential_sqft.isnull()]
parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + ', cs_nrsqft'

#If stories null, just take the Costar value no matter what
idx_imputed = parcels.stories.isnull()*(~parcels.costar_stories.isnull())
parcels.stories[parcels.stories.isnull()] = parcels.costar_stories[parcels.stories.isnull()]
parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + ', cs_stories'

#If year built out of bounds, and costar value exists, take costar value
out_of_bounds = np.logical_or((parcels.year_built < 1800), (parcels.year_built > 2015))
costar_available = ~parcels.costar_year_built.isnull()
parcels.year_built[out_of_bounds*costar_available] = parcels.costar_year_built[out_of_bounds*costar_available]
parcels.imputation_flag[out_of_bounds*costar_available] = parcels.imputation_flag[out_of_bounds*costar_available] + ', cs_yrblt'

#If non-residential sqft out of bounds, and costar value exists, take costar value
out_of_bounds = np.logical_or((parcels.non_residential_sqft < 150), (parcels.non_residential_sqft > 5000000))
costar_available = ~parcels.costar_non_residential_sqft.isnull()
parcels.non_residential_sqft[out_of_bounds*costar_available] = parcels.costar_non_residential_sqft[out_of_bounds*costar_available]
parcels.imputation_flag[out_of_bounds*costar_available] = parcels.imputation_flag[out_of_bounds*costar_available] + ', cs_nrsqft'

#If stories 0 or 1, and costar value exists, take costar value
out_of_bounds = parcels.stories < 2
costar_available = ~parcels.costar_stories.isnull()
parcels.stories[out_of_bounds*costar_available] = parcels.costar_stories[out_of_bounds*costar_available]
parcels.imputation_flag[out_of_bounds*costar_available] = parcels.imputation_flag[out_of_bounds*costar_available] + ', cs_stories'

#Append other Costar data to parcels table, just for future reference/imputation/modeling
costar_property_type = costar.groupby('gid').property_type.max()
costar_secondary_type = costar.groupby('gid').secondary_type.max()
costar_building_name = costar.groupby('gid').building_name.max()
costar_elevators = costar.groupby('gid').elevators.max()
costar_rent = costar.groupby('gid').rent.max()

parcels['costar_property_type'] = costar_property_type
parcels['costar_secondary_type'] = costar_secondary_type
parcels['costar_building_name'] = costar_building_name
parcels['costar_elevators'] = costar_elevators
parcels['costar_rent'] = costar_rent

parcels_costar = parcels[['year_built', 'non_residential_sqft', 'costar_property_type', 'costar_secondary_type', 'costar_building_name', 'costar_elevators', 'costar_rent', 'imputation_flag']]
for col in ['costar_property_type', 'costar_secondary_type', 'costar_building_name', 'costar_rent']:
    parcels_costar[col][parcels_costar[col].isnull()] = ''
    parcels_costar[col] = parcels_costar[col].str.encode('utf-8')

df_to_db(parcels_costar, 'parcels_costar', schema=loader.tables.staging)

#Update the master parcel table on the database
exec_sql("update parcels set year_built = a.year_built from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set non_residential_sqft = a.non_residential_sqft from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set imputation_flag = a.imputation_flag from staging.parcels_costar a where a.gid = parcels.gid;")

exec_sql("ALTER TABLE parcels ADD COLUMN costar_elevators numeric;")
exec_sql("ALTER TABLE parcels ADD COLUMN costar_property_type text;")
exec_sql("ALTER TABLE parcels ADD COLUMN costar_secondary_type text;")
exec_sql("ALTER TABLE parcels ADD COLUMN costar_building_name text;")
exec_sql("ALTER TABLE parcels ADD COLUMN costar_rent text;")

exec_sql("update parcels set costar_elevators = a.costar_elevators from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set costar_property_type = a.costar_property_type from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set costar_secondary_type = a.costar_secondary_type from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set costar_building_name = a.costar_building_name from staging.parcels_costar a where a.gid = parcels.gid;")
exec_sql("update parcels set costar_rent = a.costar_rent from staging.parcels_costar a where a.gid = parcels.gid;")


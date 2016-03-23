import pandas as pd, numpy as np
import pandas.io.sql as sql
from pandas.io.excel import read_excel
from spandex.io import exec_sql,  df_to_db
from spandex import TableLoader

loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

def csv_to_staging(path, table_name):
    """Loads csv to staging schema on database"""
    df = pd.read_csv(path)
    df.index.name = 'idx'
    print 'Loading %s.' % table_name
    df_to_db(df, table_name, schema=loader.tables.staging)
    
def field_in_table(field_name, table_name):
    """True if field in table else False"""
    return field_name in db_to_df("SELECT column_name FROM information_schema.columns  WHERE table_name='%s'" % table_name).column_name.values
    
def lat_long_to_point_geometry(tbl, schema, x_col = 'longitude', y_col = 'latitude', geom_field_name = 'geom', target_srid = 2768):
    """Creates point geometry on db table based on lat long fields"""
    print 'Creating point geometry from lat/long in table %s.' % tbl 
    if not field_in_table(geom_field_name, tbl):
        exec_sql("ALTER TABLE %s.%s ADD COLUMN %s geometry;" % (schema, tbl, geom_field_name))
    exec_sql("UPDATE %s.%s SET %s = ST_GeomFromText('POINT(' || %s || ' ' || %s || ')',4326);" % 
             (schema, tbl, geom_field_name, x_col, y_col))
    exec_sql("CREATE INDEX %s_gidx on %s.%s using gist (%s);" % (tbl, schema, tbl, geom_field_name))
    exec_sql("SELECT UpdateGeometrySRID('%s', '%s', '%s', %s);" % (schema, tbl, geom_field_name, target_srid))
    exec_sql("UPDATE %s.%s SET %s = ST_TRANSFORM(ST_SetSRID(%s, 4326), %s);" % (schema, tbl, geom_field_name, geom_field_name, target_srid))
    
def append_parcel_identifier(tbl, schema, tbl_geom, parcel_identifier):
    """Append parcel identifier to target table """
    print 'Appending parcel identifier to %s' % tbl
    if not field_in_table(parcel_identifier, tbl):
        exec_sql("alter table %s.%s add %s integer default 0;" %(schema, tbl, parcel_identifier))
    exec_sql("update %s.%s set %s = a.%s from parcels a where st_contains(a.geom, %s.%s.%s);" %
              (schema, tbl, parcel_identifier, parcel_identifier, schema, tbl, tbl_geom))
              
def imputation_variable(df, attribute, agg_function, lower_bound, upper_bound):
    # Summarize imputation data at parcel level and set bounds on valid values
    sr_grouped = df.groupby('gid')[attribute]
    if agg_function == 'median':
        var = sr_grouped.median()
    if agg_function == 'max':
        var = sr_grouped.max()
    if agg_function == 'sum':
        var = sr_grouped.sum()
    var = var[(var > lower_bound) & (var < upper_bound)] #set bounds on valid values to use for impute
    return var


######## *LOADING* ########

#### REDFIN
# Read Redfin CSV and load to database
csv_to_staging(loader.get_path('built/bldg/homeprices/redfin_03feb14.csv'), 'redfin')
# Lat/long to point geometry, with the right SRID
lat_long_to_point_geometry('redfin', 'staging', 'longitude', 'latitude', 'geom', 2768)
# Append the unique parcel identifier to the Redfin records
append_parcel_identifier('redfin', 'staging', 'geom', 'gid')

#### GOV BUILDINGS
# Read Gov Building CSV and load to database
csv_to_staging(loader.get_path('built/bldg/add_buildings1.csv'), 'public_bldgs')
# Lat/long to point geometry, with the right SRID
lat_long_to_point_geometry('public_bldgs', 'staging', 'x', 'y', 'geom', 2768)
# Append the unique parcel identifier to the Gov Building records
append_parcel_identifier('public_bldgs', 'staging', 'geom', 'gid')

#### COSTAR
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
lat_long_to_point_geometry('costar', 'staging', 'longitude', 'latitude', 'geom', 2768)
##Append the unique parcel identifier to the Costar records
append_parcel_identifier('costar', 'staging', 'geom', 'gid')


######## *IMPUTE* ########
print 'Start point-based impute.'

## Load dataframes for the imputation
parcels = db_to_df('select gid, year_built, sqft_per_unit, non_residential_sqft, stories, imputation_flag from parcels;').set_index('gid')
costar = db_to_df('select * from staging.costar;')
redfin = db_to_df('select gid, yearbuilt, sqft, lastsalepr, saleyear, hometype from staging.redfin;')
gov_buildings = db_to_df('select * from staging.public_bldgs;')
gov_buildings.sqft = gov_buildings.sqft.str.replace(',', '').astype('int')

## Assign imputation variables to parcels df
# Redfin
parcels['redfin_year_built'] = imputation_variable(redfin, 'yearbuilt', 'median', 1800, 2016)
parcels['redfin_sqft_per_unit'] = imputation_variable(redfin, 'sqft', 'median', 199, 25000)
# Gov
parcels['gov_sqft'] = imputation_variable(gov_buildings, 'sqft', 'max', 199, 2000000)
# Costar
parcels['costar_year_built'] = imputation_variable(costar, 'year_built', 'median', 1800, 2016)
parcels['costar_non_residential_sqft'] = imputation_variable(costar, 'rentable_area', 'max', 199, 2000000) #sum?
parcels['costar_stories'] = imputation_variable(costar, 'stories', 'max', 0, 100)

def impute_null(target_varname, source_varname, imputation_flag_note):
    idx_imputed = parcels[target_varname].isnull()*(~parcels[source_varname].isnull())  ## These are the records that will be imputed
    parcels[target_varname][parcels[target_varname].isnull()] = parcels[source_varname][parcels[target_varname].isnull()]
    parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + imputation_flag_note  ## Populate imputation flag for affected records
    print imputation_flag_note, '%s records affected' % idx_imputed.sum()
    
def impute_out_of_bounds(target_varname, source_varname, imputation_flag_note, floor, ceiling):
    out_of_bounds = np.logical_or((parcels[target_varname] < floor), (parcels[target_varname] > ceiling))
    impute_available = ~parcels[source_varname].isnull()
    idx_imputed = out_of_bounds*impute_available
    parcels[target_varname][idx_imputed] = parcels[source_varname][idx_imputed]
    parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + imputation_flag_note
    print imputation_flag_note, '%s records affected' % idx_imputed.sum()
    
def impute_greater_than(target_varname, source_varname, imputation_flag_note):
    """If target variable has value less than source variable, replace with source value."""
    idx_imputed = parcels[source_varname] > parcels[target_varname]
    parcels[target_varname][idx_imputed] = parcels[source_varname][idx_imputed]
    parcels.imputation_flag[idx_imputed] = parcels.imputation_flag[idx_imputed] + imputation_flag_note
    print imputation_flag_note, '%s records affected' % idx_imputed.sum()
    
#If null year_built, just take the Redfin year built value no matter what
impute_null('year_built', 'redfin_year_built', ', rf_yrblt')

#If null sqft_per_unit, just take the Redfin sqft_per_unit value no matter what
impute_null('sqft_per_unit', 'redfin_sqft_per_unit', ', rf_sqftunit')

#If year_built out of bounds, and Redfin value exists, take Redfin value
impute_out_of_bounds('year_built', 'redfin_year_built', ', rf_yrblt', 1800, 2015)

#If sqft_per_unit out of bounds, and Redfin value exists, take Redfin value
impute_out_of_bounds('sqft_per_unit', 'redfin_sqft_per_unit', ', rf_sqftunit', 150, 50000)

#If non-residential sqft null, just take the Gov value no matter what
impute_null('non_residential_sqft', 'gov_sqft', ', gov_nrsqft')

#If non-residential sqft out of bounds, and Gov value exists, take Gov value
impute_out_of_bounds('non_residential_sqft', 'gov_sqft', ', gov_nrsqft', 150, 5000000)

#If parcel nonres sqft less than indicated by Gov, use Gov nonres sqft
impute_greater_than('non_residential_sqft', 'gov_sqft', ', gov_nrsqft_boosted')

#If year_built null, just take the Costar value no matter what
impute_null('year_built', 'costar_year_built', ', cs_yrblt')

#If non-residential sqft null, just take the Costar value no matter what
impute_null('non_residential_sqft', 'costar_non_residential_sqft', ', cs_nrsqft')

#If stories null, just take the Costar value no matter what
impute_null('stories', 'costar_stories', ', cs_stories')

#If year built out of bounds, and costar value exists, take costar value
impute_out_of_bounds('year_built', 'costar_year_built', ', cs_yrblt', 1800, 2015)

#If non-residential sqft out of bounds, and costar value exists, take costar value
impute_out_of_bounds('non_residential_sqft', 'costar_non_residential_sqft', ', cs_nrsqft', 150, 5000000)

#If parcel nonres sqft less than indicated by Costar, use Costar nonres sqft
impute_greater_than('non_residential_sqft', 'costar_non_residential_sqft', ', cr_nrsqft_boosted')

#If stories 0 or 1, and costar value exists, take costar value
impute_out_of_bounds('stories', 'costar_stories', ', cs_stories', 2, 100)


#Append other imputation source data to parcels table, just for reference/imputation/modeling in later steps
parcels['redfin_sale_price'] = redfin.groupby('gid').lastsalepr.median()
parcels['redfin_sale_year'] = redfin.groupby('gid').saleyear.median()
parcels['redfin_home_type'] = redfin.groupby('gid').hometype.max()

parcels['gov_type'] = gov_buildings.groupby('gid').development_type_id.max()

parcels['costar_property_type'] = costar.groupby('gid').property_type.max()
parcels['costar_secondary_type'] = costar.groupby('gid').secondary_type.max()
parcels['costar_building_name'] = costar.groupby('gid').building_name.max()
parcels['costar_elevators'] = costar.groupby('gid').elevators.max()
parcels['costar_rent'] = costar.groupby('gid').rent.max()


######## *UPDATE* ########
print 'Point-based impute done. Loading results back to db.'

# Updated fields back to database
parcels_imputed = parcels[['year_built', 'sqft_per_unit', 'non_residential_sqft', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'gov_type', 'gov_sqft', 'costar_property_type', 'costar_secondary_type', 'costar_building_name', 'costar_elevators', 'costar_rent', 'imputation_flag']]
for col in ['redfin_home_type', 'costar_property_type', 'costar_secondary_type', 'costar_building_name', 'costar_rent']:
    parcels_imputed[col][parcels_imputed[col].isnull()] = ''
    parcels_imputed[col] = parcels_imputed[col].str.encode('utf-8')
df_to_db(parcels_imputed, 'parcels_imputed', schema=loader.tables.staging)

#Update the master parcel table on the database
exec_sql("update parcels set year_built = a.year_built from staging.parcels_imputed a where a.gid = parcels.gid;")
exec_sql("update parcels set sqft_per_unit = a.sqft_per_unit from staging.parcels_imputed a where a.gid = parcels.gid;")
exec_sql("update parcels set non_residential_sqft = a.non_residential_sqft from staging.parcels_imputed a where a.gid = parcels.gid;")
exec_sql("update parcels set imputation_flag = a.imputation_flag from staging.parcels_imputed a where a.gid = parcels.gid;")

def add_field_and_populate_with_imputed_value(varname, vartype):
    if not field_in_table(varname, 'parcels'):
        exec_sql("ALTER TABLE parcels ADD COLUMN %s %s;" % (varname, vartype))
    exec_sql("update parcels set %s = a.%s from staging.parcels_imputed a where a.gid = parcels.gid;" % (varname, varname))
    
add_field_and_populate_with_imputed_value('redfin_sale_price', 'numeric')
add_field_and_populate_with_imputed_value('redfin_sale_year', 'numeric')
add_field_and_populate_with_imputed_value('redfin_home_type', 'text')
add_field_and_populate_with_imputed_value('gov_type', 'numeric')
add_field_and_populate_with_imputed_value('gov_sqft', 'numeric')
add_field_and_populate_with_imputed_value('costar_elevators', 'numeric')
add_field_and_populate_with_imputed_value('costar_property_type', 'text')
add_field_and_populate_with_imputed_value('costar_secondary_type', 'text')
add_field_and_populate_with_imputed_value('costar_building_name', 'text')
add_field_and_populate_with_imputed_value('costar_rent', 'text')
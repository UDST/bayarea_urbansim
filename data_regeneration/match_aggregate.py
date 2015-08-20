#Imports
import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db
from spandex.targets import scaling as scl
import hashlib

#Connect to the database
loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

##Load parcels as dataframes for the imputation
parcels = db_to_df('select * from parcels;')
parcels = parcels.set_index('gid')

#Standardize the res_type field
parcels.res_type[parcels.res_type.isnull()] = 'other'
parcels.res_type[parcels.res_type==''] = 'other'
parcels.res_type[np.in1d(parcels.res_type, ['FLATS', 'APTS', 'CONDO', 'SRO', 'LIVEWORK', 'mixed'])] = 'multi'
parcels.res_type[parcels.res_type=='SINGLE'] = 'single'

# Load TAZ residential unit control totals and other zonal targets.
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')

taz_controls_csv2 = loader.get_path('hh/tazsumm_redfin.csv')
targetvalues = pd.read_csv(taz_controls_csv2, index_col='taz')

nonres_sqft_zone = pd.DataFrame({'observed':parcels.groupby('taz').non_residential_sqft.sum(), 'target':targetunits.targetnonressqft})

# For all employment points, translate to nonres-sqft by multiplying by 250.
# Filter out synthetic job-based buildings so that we keep only those that have no residential and have less than 500 existing sqft. 
# For each TAZ, calculate the difference needed to match aggregate target.
# If need to increment nrsqft upwards, sort synthetic buildings by sqft and take the top x that covers the needed difference
# If no valid job points and non existing nonres-sqft, introduce a synthetic building in the TAZ-  equal to the target, and put it on the biggest parcel.
# Do same in the case of no parcels (and add synthetic parcel)
# Scale to match

#No need to tag in imputation_flag column based on scaling- otherwise everything would be tagged.
nonres_sqft_zone['difference'] = nonres_sqft_zone.target - nonres_sqft_zone.observed

##Append the unique parcel identifier to the establisment point records.
if 'parcel_id' not in db_to_df("SELECT column_name FROM information_schema.columns  WHERE table_name='establishment_points'").column_name.values:
    exec_sql("alter table staging.establishment_points add parcel_id integer default 0;")
    exec_sql("update staging.establishment_points set parcel_id = a.gid from parcels a where st_within(staging.establishment_points.geom, a.geom);")

#Load the establishment points to be used for non-residential sqft imputation
estab_points = db_to_df('select emp_here, naics2, parcel_id from staging.establishment_points;')
estabs_joined = pd.merge(estab_points, parcels.reset_index(), left_on = 'parcel_id', right_on = 'gid')

# Filter out some synthetic job-based buildings so that we keep only those that have no existing residential and have less than 500 existing non-residential sqft. 
estabs_joined = estabs_joined[np.logical_or((estabs_joined.residential_units == 0), (estabs_joined.residential_units.isnull())) & np.logical_or((estabs_joined.non_residential_sqft < 500), (estabs_joined.non_residential_sqft.isnull()))]

# For all employment points, translate to nonres-sqft by multiplying by 250.
estabs_joined['nrsqft_assumed'] = 250 * estabs_joined.emp_here

#Aggregate employment-point-based-buildings to the parcel level
estabs_joined_nrsqft = estabs_joined.groupby('gid').nrsqft_assumed.sum()
estabs_joined_taz = estabs_joined.groupby('gid').taz.max()
estabs_for_impute = pd.DataFrame({'non_residential_sqft':estabs_joined_nrsqft, 'taz':estabs_joined_taz})

#Subset of parcels, just for convenience
parcels_abridged = parcels[['taz', 'res_type', 'residential_units', 'calc_area']]

#Function to add synthetic parcel in case that TAZ contains no existing parcels
def add_parcel(zone_id):
    new_pid = parcels.index.max() + 1
    parcels.loc[new_pid] = None
    parcels.taz.loc[new_pid] = zone_id  ##Should also append a specific county_id, but need a taz-county xref
    parcels.imputation_flag.loc[new_pid] = 'synthetic'
    for col in parcels.dtypes.iteritems():
        col_name = col[0]
        col_type = col[1]
        if (col_name != 'taz') & (col_name != 'imputation_flag'):
            if col_name == 'county_id':
                parcels[col_name].loc[new_pid] = 0
            elif col_type == 'object':
                parcels[col_name].loc[new_pid] = ' '
            else:
                parcels[col_name].loc[new_pid] = 0.0
    return new_pid

#Select parcel in each taz for new building, if needed later
#Get parcel_id of parcel with max area
parcel_with_max_area_by_taz = parcels_abridged.reset_index().groupby('taz').apply(lambda t: t[t.calc_area==t.calc_area.max()]).gid.reset_index()[['taz', 'gid']].set_index('taz')

#Example usage.  This gives the GID of parcel with max area in TAZ 4
#print parcel_with_max_area_by_taz.loc[4]
def select_parcel_for_new_building(zone_id):
    return int(parcel_with_max_area_by_taz.loc[zone_id].values.max())

parcel_du_by_taz = parcels.groupby(['taz', 'res_type']).residential_units.sum()
observed_parcel_du_taz = parcel_du_by_taz.index.get_level_values(0)

sf_parcels = parcels_abridged[(parcels_abridged.res_type == 'single') & (parcels_abridged.residential_units > 0)]
mf_parcels = parcels_abridged[(parcels_abridged.res_type == 'multi') & (parcels_abridged.residential_units > 0)]
sf_parcels_loc = sf_parcels.reset_index().set_index(['taz', 'gid']).loc
mf_parcels_loc = mf_parcels.reset_index().set_index(['taz', 'gid']).loc

res_parcel_updates = pd.Series()
new_res_buildings = []

nonres_parcel_updates = pd.Series()
new_nonres_buildings = []

def place_units(alternative_ids, number_of_units):
    probabilities = np.ones(len(alternative_ids))
    choices = np.random.choice(alternative_ids, size = number_of_units, replace = True)
    return pd.Series(choices).value_counts() #this is units_to_add by gid

def update_res_parcels(units_to_add_or_remove):
    #parcels.residential_units.loc[units_to_add_or_remove.index] = parcels.residential_units.loc[units_to_add_or_remove.index] + units_to_add_or_remove.values
    global res_parcel_updates
    res_parcel_updates = res_parcel_updates.append(units_to_add_or_remove)

def select_unit_deletions_by_gid(deletion_candidates_exploded, difference):
    deletions = np.random.choice(deletion_candidates_exploded, size = difference, replace = False)
    return -1*pd.Series(deletions).value_counts()

def explode_unit_candidates(loc, zone_id):
    candidates = loc[zone_id].residential_units
    return np.repeat(candidates.index.values, candidates.values.astype('int'))


def add_sf_units(zone_id, sf_difference, observed_sf_units):
    if observed_sf_units > 0:
        #allocate according to existing distribution
        alternative_ids = sf_parcels_loc[zone_id].index.values
        units_to_add = place_units(alternative_ids, sf_difference)
        update_res_parcels(units_to_add)
    else:
        pid = select_parcel_for_new_building(zone_id)
        new_res_buildings.append([pid, 'single', sf_difference])
        
def remove_sf_units(zone_id, sf_difference):
    deletion_candidates_exploded = explode_unit_candidates(sf_parcels_loc, zone_id)
    deletions_by_gid = select_unit_deletions_by_gid(deletion_candidates_exploded, sf_difference)
    update_res_parcels(deletions_by_gid)

def add_mf_units(zone_id, mf_difference, observed_mf_units):
    if observed_mf_units > 0:
        #allocate according to existing distribution
        alternative_ids = mf_parcels_loc[zone_id].index.values
        units_to_add = place_units(alternative_ids, mf_difference)
        update_res_parcels(units_to_add)
    else:
        pid = select_parcel_for_new_building(zone_id)
        new_res_buildings.append([pid, 'multi', mf_difference])

def remove_mf_units(zone_id, mf_difference):
    deletion_candidates_exploded = explode_unit_candidates(mf_parcels_loc, zone_id)
    deletions_by_gid = select_unit_deletions_by_gid(deletion_candidates_exploded, mf_difference)
    update_res_parcels(deletions_by_gid)
    
    
def add_or_remove_sf_units(zone_id, sf_difference, observed_sf_units):
    if sf_difference > 0:
        print '        *Adding %s single-family DU.' % sf_difference
        add_sf_units(zone_id, sf_difference, observed_sf_units)
    if sf_difference < 0:
        sf_difference = abs(sf_difference)
        print '        *Removing %s single-family DU.' % sf_difference
        remove_sf_units(zone_id, sf_difference)
        
def add_or_remove_mf_units(zone_id, mf_difference, observed_mf_units):
    if mf_difference > 0:
        print '        *Adding %s multi-family DU.' % mf_difference
        add_mf_units(zone_id, mf_difference, observed_mf_units)
    if mf_difference < 0:
        mf_difference = abs(mf_difference)
        print '        *Removing %s multi-family DU.' % mf_difference
        remove_mf_units(zone_id, mf_difference)
        
def flip_from_sf_to_mf_type(zone_id, number_of_units, observed_mf_units):
    print '        Trying to flilp %s single-family units to multi-family' % number_of_units
    
    sf_flip_alternatives = sf_parcels_loc[zone_id].residential_units.order(ascending = True)
    cumsum_idx = sf_flip_alternatives.cumsum().searchsorted(number_of_units)
    
    sampled_res_buildings = 0
    sampled_res_buildings1 = sf_flip_alternatives[:(cumsum_idx)]
    sampled_res_buildings2 = sf_flip_alternatives[:(cumsum_idx + 1)]
    
    if (sampled_res_buildings2.sum() > 0) & (sampled_res_buildings2.sum() <= number_of_units):
        sampled_res_buildings = sampled_res_buildings2
    elif (sampled_res_buildings1.sum() > 0) & (sampled_res_buildings1.sum() <= number_of_units):
        sampled_res_buildings = sampled_res_buildings1
        
    if type(sampled_res_buildings) is int:
        add_or_remove_mf_units(zone_id, number_of_units, observed_mf_units)
        return number_of_units
    else:
        #Remove from sf_parcels_loc
        buildings_to_drop = zip([zone_id,]*len(sampled_res_buildings), sampled_res_buildings.index)
        sf_parcels_loc.obj = sf_parcels_loc.obj.drop(buildings_to_drop)

        #Edit parcels so these are mf buildings
        parcels.res_type.loc[sampled_res_buildings.index.values] = 'multi'
        parcels.development_type_id.loc[sampled_res_buildings.index.values] = 'MF'
        parcels.imputation_flag.loc[sampled_res_buildings.index] = parcels.imputation_flag.loc[sampled_res_buildings.index] + ', restype_flip'  ## Populate imputation flag for affected records
        
        print '        Flipped %s single-family units to multi-family' % sampled_res_buildings.sum()
        if (number_of_units - sampled_res_buildings.sum()) > 0:
            add_or_remove_mf_units(zone_id, number_of_units - sampled_res_buildings.sum(), observed_mf_units)
        return number_of_units - sampled_res_buildings.sum()

def flip_from_mf_to_sf_type(zone_id, number_of_units, observed_sf_units):
    print '        Trying to flip %s multi-family units to single-family' % number_of_units
    
    mf_flip_alternatives = mf_parcels_loc[zone_id].residential_units.order(ascending = True)
    cumsum_idx = mf_flip_alternatives.cumsum().searchsorted(number_of_units)
    
    sampled_res_buildings = 0
    sampled_res_buildings1 = mf_flip_alternatives[:(cumsum_idx)]
    sampled_res_buildings2 = mf_flip_alternatives[:(cumsum_idx + 1)]
    
    if (sampled_res_buildings2.sum() > 0) & (sampled_res_buildings2.sum() <= number_of_units):
        sampled_res_buildings = sampled_res_buildings2
    elif (sampled_res_buildings1.sum() > 0) & (sampled_res_buildings1.sum() <= number_of_units):
        sampled_res_buildings = sampled_res_buildings1
        
    if type(sampled_res_buildings) is int:
        add_or_remove_sf_units(zone_id, number_of_units, observed_sf_units)
        return number_of_units
    else:
        #Remove from mf_parcels_loc
        buildings_to_drop = zip([zone_id,]*len(sampled_res_buildings), sampled_res_buildings.index)
        mf_parcels_loc.obj = mf_parcels_loc.obj.drop(buildings_to_drop)

        #Edit parcels so these are sf buildings
        parcels.res_type.loc[sampled_res_buildings.index.values] = 'single'
        parcels.development_type_id.loc[sampled_res_buildings.index.values] = 'SF'
        parcels.imputation_flag.loc[sampled_res_buildings.index] = parcels.imputation_flag.loc[sampled_res_buildings.index] + ', restype_flip'  ## Populate imputation flag for affected records
        
        print '        Flipped %s multi-family units to single-family' % sampled_res_buildings.sum()
        if (number_of_units - sampled_res_buildings.sum()) > 0:
            add_or_remove_sf_units(zone_id, number_of_units - sampled_res_buildings.sum(), observed_sf_units)
        return number_of_units - sampled_res_buildings.sum()


#NRSQFT imputation based on estab sites to match aggregate totals
for rec in nonres_sqft_zone.iterrows():
    zone_id = rec[0]
    print 'Non-residential sqft imputation for taz %s' % zone_id
    difference = rec[1]['difference']
    if np.isnan(difference):
        ##No parcels exist in this zone...
        print '    No parcels in this zone.  Adding synthetic parcel and needed non-residential sqft.'
        difference = rec[1]['target']
        if difference > 0:
            new_pid = add_parcel(zone_id)
            new_nonres_buildings.append([new_pid, 'nonres_generic', difference])
    else:
        if difference > 0:
            observed = rec[1]['observed']
            #If starting point is zero non-res sqft, then take estab points to match target regardless of distance from target
            #If some nonres-sqft already exists.  Only impute from estab points if target difference exceeds 5000 sqft
            if (observed == 0) or (difference > 5000):
                print '    Target non_residential_sqft to sample from estab-points: %s' % difference
                estabs_taz = estabs_for_impute[estabs_for_impute.taz == zone_id]
                if len(estabs_taz) > 0:
                    print '        Estab sites exist to sample from'
                    nonres_alternatives = estabs_taz.non_residential_sqft.order(ascending=True)
                    cumsum_idx = nonres_alternatives.cumsum().searchsorted(difference)
                    sampled_estabs = nonres_alternatives[:(cumsum_idx + 1)]
                    print '        Sampled non_residential_sqft to add: %s' % sampled_estabs.sum()
                    nonres_parcel_updates = nonres_parcel_updates.append(sampled_estabs)  
                else:
                    #No estab sites exist to sample from
                    if observed == 0:
                        #Add synthetic sqft corresponding to the target difference
                        print '        No estab sites exist and no existing nonres sqft: Add synthetic sqft to match target difference'
                        pid = select_parcel_for_new_building(zone_id)
                        new_nonres_buildings.append([pid, 'nonres_generic', difference])

####Summarize NRSQFT change
print parcels.groupby('county_id').non_residential_sqft.sum()
parcels.non_residential_sqft.loc[nonres_parcel_updates.index] = parcels.non_residential_sqft.loc[nonres_parcel_updates.index].fillna(0) + nonres_parcel_updates.values
parcels.imputation_flag.loc[nonres_parcel_updates.index] = parcels.imputation_flag.loc[nonres_parcel_updates.index] + ', estab_sqft'  ## Populate imputation flag for affected records
print parcels.groupby('county_id').non_residential_sqft.sum()


#RESUNIT imputation to match aggregate totals
zonal_residential_unit_controls = targetunits[['targetSF', 'targetMF']]
for taz_du in zonal_residential_unit_controls.iterrows():
    
    zone_id = int(taz_du[0])
    print 'Matching aggregate residential unit targets for zone %s.' % zone_id
    
    target_sf_units = int(taz_du[1]['targetSF'])
    target_mf_units = int(taz_du[1]['targetMF'])
    print '    Target of %s single-family DU.' % (target_sf_units)
    print '    Target of %s multi-family DU.' % (target_mf_units)
    
    #TAZ currently has residential units on parcels
    if zone_id in observed_parcel_du_taz:
        parcel_observed_du = parcel_du_by_taz.loc[zone_id]
        
        #Look up observed SF DU
        if 'single' in parcel_observed_du.keys():
            observed_sf_units = int(parcel_observed_du['single'])
        else:
            observed_sf_units = 0
            
        #Look up observed MF DU
        if 'multi' in parcel_observed_du.keys():
            observed_mf_units = int(parcel_observed_du['multi'])
        else:
            observed_mf_units = 0
            
        print '        Observed %s single-family DU.' % (observed_sf_units)
        print '        Observed %s multi-family DU.' % (observed_mf_units)
            
        #Calculate difference between target and observed
        sf_difference = target_sf_units - observed_sf_units
        mf_difference = target_mf_units - observed_mf_units
        
        print '            Target difference:  %s single-family DU.' % (sf_difference)
        print '            Target difference:  %s multi-family DU.' % (mf_difference)
        
        ####Impute as needed to cover difference
        
        # Imputation is needed somewhere
        if (sf_difference != 0) | (mf_difference != 0):
            
            # One of the residential categories requires no imputation
            if (sf_difference == 0) | (mf_difference == 0):
            
                if sf_difference != 0:
                    add_or_remove_sf_units(zone_id, sf_difference, observed_sf_units)

                if mf_difference != 0:
                    add_or_remove_mf_units(zone_id, mf_difference, observed_mf_units)
                
            # Both residential categories require imputation
            else:
                net_difference = sf_difference + mf_difference
                
                # Both residential categories require additions, or both require subtractions
                if ((sf_difference > 0) & (mf_difference > 0)) | ((sf_difference < 0) & (mf_difference < 0)):
                    add_or_remove_sf_units(zone_id, sf_difference, observed_sf_units)
                    add_or_remove_mf_units(zone_id, mf_difference, observed_mf_units)
                    
                # Single-family requires subtractions, and multi-family requires additions
                elif (sf_difference < 0) & (mf_difference > 0):
                    # If |decrease| in single-family units exceeds |increase| in multi-family units
                    if  abs(sf_difference) > abs(mf_difference):
                        # Take subset of the single-family decrease corresponding to the mult-family increase, and flip the type
                        remainder = flip_from_sf_to_mf_type(zone_id, mf_difference, observed_mf_units)
                        # Take rest of the single-family decrease, and remove these units, but only from among non-type-flipped parcels
                        add_or_remove_sf_units(zone_id, net_difference - remainder, observed_sf_units - (mf_difference - remainder))
                    # If |decrease| in single-family units is less than or equal to |increase| in multi-family units
                    elif abs(sf_difference) <= abs(mf_difference):
                        # Take all of the single-family decrease, and flip their type
                        remainder = flip_from_sf_to_mf_type(zone_id, abs(sf_difference), observed_mf_units)
                        # Take rest of the multifamily-increase, if any remains, and add units
                        if net_difference > 0:
                            add_or_remove_mf_units(zone_id, net_difference, observed_mf_units)
                        # If not all singlefamily could be type-flipped, then remove units
                        if remainder > 0:
                            allocated = abs(sf_difference) - remainder
                            add_or_remove_sf_units(zone_id, -remainder, observed_sf_units - allocated)
                            
                # Multi-family requires subtractions, and single-family requires additions
                elif (mf_difference < 0) & (sf_difference > 0):
                    # If |decrease| in multi-family units exceeds |increase| in single-family units
                    if abs(mf_difference) > abs(sf_difference):
                        # Take subset of the multi-family decrease corresponding to the single-family increase, and flip the type
                        remainder = flip_from_mf_to_sf_type(zone_id, sf_difference, observed_sf_units)
                        # Take rest of the multi-family decrease, and remove these units, but only from among non-type-flipped parcels
                        add_or_remove_mf_units(zone_id, net_difference - remainder, observed_mf_units - (sf_difference - remainder))
                    # If |decrease| in multi-family units is less than or equal to |increase| in single-family units
                    elif abs(mf_difference) <= abs(sf_difference):
                        # Take all of the multi-family decrease, and flip their type
                        remainder = flip_from_mf_to_sf_type(zone_id, abs(mf_difference), observed_sf_units)
                        # Take rest of the single-increase, if any remains, and add units
                        if net_difference > 0:
                            add_or_remove_sf_units(zone_id, net_difference, observed_sf_units)
                        # If not all multifamily could be type-flipped, then remove units
                        if remainder > 0:
                            allocated = abs(mf_difference) - remainder
                            add_or_remove_mf_units(zone_id, -remainder, observed_mf_units - allocated)
        
    else:
        #TAZ currently has ZERO parcels, add an artificial parcel
        print '        No parcels in this zone.  Adding synthetic parcel and needed residential units by type.'
        new_pid = add_parcel(zone_id)
        if target_sf_units > 0:
            new_res_buildings.append([new_pid, 'single', target_sf_units])
        if target_mf_units > 0:
            new_res_buildings.append([new_pid, 'multi', target_mf_units])
            
print parcels.groupby('county_id').residential_units.sum()
parcels.residential_units.loc[res_parcel_updates.index] = parcels.residential_units.loc[res_parcel_updates.index].fillna(0) + res_parcel_updates.values
parcels.imputation_flag.loc[res_parcel_updates.index] = parcels.imputation_flag.loc[res_parcel_updates.index] + ', du_zonetarget'  ## Populate imputation flag for affected records
print parcels.groupby('county_id').residential_units.sum()


new_res_buildings_df = pd.DataFrame(new_res_buildings, columns = ['gid', 'res_type', 'residential_units'])
new_nonres_buildings_df = pd.DataFrame(new_nonres_buildings, columns = ['gid', 'type', 'non_residential_sqft'])

new_res_buildings_df = pd.merge(new_res_buildings_df, parcels[['county_id', 'taz']], left_on = 'gid', right_index=True)
new_nonres_buildings_df = pd.merge(new_nonres_buildings_df, parcels[['county_id', 'taz']], left_on = 'gid', right_index=True)


print new_res_buildings_df.groupby('county_id').residential_units.sum()
print new_nonres_buildings_df.groupby('county_id').non_residential_sqft.sum()


##Deal with parcels where development type id is unknown, imputing using Costar/Redfin
problematic = parcels[parcels.development_type_id.isnull() & (parcels.res_type=='other')][['county_id','improvement_value','year_built','stories','sqft_per_unit','residential_units','non_residential_sqft','building_sqft','res_type','land_use_type_id','development_type_id','redfin_home_type', 'costar_property_type','costar_secondary_type']]
##Where no dev type, but we can get dev type from costar, then use costar for the dev type!

##Tag these as nonresidential dev_type based on costar type designation
problematic_nonres = problematic[(~(problematic.costar_property_type == '')) & ((problematic.year_built>0)|(problematic.improvement_value>0)|(problematic.stories>0)|(problematic.building_sqft>0)|(problematic.non_residential_sqft>0))]

##Tag these as residential res_type/dev_type based on redfin type designation
problematic_res = problematic[(~(problematic.redfin_home_type == '')) & (problematic.costar_property_type == '') & ((problematic.year_built>0)|(problematic.improvement_value>0)|(problematic.stories>0)|(problematic.sqft_per_unit>0)|(problematic.building_sqft>0))] ##2810

##After the above, export the county_id/land_use_type_id's that remain as a diagnostic output for Mike to investigate, then assume the rest have dev_type "unknown"
##Double check that all records with res_type 'single' or 'multi' have the appropriate development type

#Map between costar types and development_type_id categories
costar_devtype_map = {'Retail':'RT',
 'Office':'OF',
 'Industrial':'IW',
 'Flex':'IW',
 'Specialty':'OF',
 'Retail (Strip Center)':'RT',
 'Retail (Neighborhood Center)':'RT',
 'Hospitality':'HO',
 'Health Care':'HP',
 'Retail (Community Center)':'RT',
 'Sports & Entertainment':'RT',
 'Retail (Power Center)':'RT',
 'Retail (Regional Mall)':'RT',
 'Retail (Lifestyle Center)':'RT',
 'Retail (Super Regional Mall)':'RT',
 'Office (Strip Center)':'OF',
 'Retail (Theme/Festival Center)':'RT',
 'Office (Neighborhood Center)':'OF',
 'Office (Lifestyle Center)':'OF',
 'Retail (Outlet Center)':'RT',
 'Specialty (Neighborhood Center)':'RT',
 'Industrial (Lifestyle Center)':'IW',
 'Office (Regional Mall)':'OF',
 'Flex (Strip Center)':'OF',
 'General Retail':'RT',
 'General Retail (Strip Center)':'RT',
 'Hospitality (Neighborhood Center)':'HO',
 'Office (Super Regional Mall)':'OF'}

for costar_type in costar_devtype_map.keys():
    idx = problematic_nonres[problematic_nonres.costar_property_type == costar_type].index.values
    parcels.development_type_id.loc[idx] = costar_devtype_map[costar_type]
    parcels.imputation_flag.loc[idx] = parcels.imputation_flag.loc[idx] + ', costar_type'
    
#Map between redfin types and res_type categories
redfin_devtype_map1 = {'Single Family Residential':'single',
 'Condo/Coop':'multi',
 'Townhouse':'multi',
 'Vacant Land':'other',
 'Multi-Family (2-4 Unit)':'multi',
 'Unknown':'other',
 'Other':'other',
 'Mobile/Manufactured Home':'other',
 'Multi-Family (5+ Unit)':'multi',
 'Ranch':'single'}

#Map between redfin types and development_type_id categories
redfin_devtype_map2 = {'Single Family Residential':'SF',
 'Condo/Coop':'MF',
 'Townhouse':'MF',
 'Vacant Land':'other',
 'Multi-Family (2-4 Unit)':'MF',
 'Unknown':'other',
 'Other':'other',
 'Mobile/Manufactured Home':'SF',
 'Multi-Family (5+ Unit)':'MF',
 'Ranch':'SF'}

for redfin_type in redfin_devtype_map1.keys():
    idx = problematic_res[problematic_res.redfin_home_type == redfin_type].index.values
    parcels.imputation_flag.loc[idx] = parcels.imputation_flag.loc[idx] + ', redfin_type'
    parcels.res_type.loc[idx] = redfin_devtype_map1[redfin_type]

for redfin_type in redfin_devtype_map2.keys():
    idx = problematic_res[problematic_res.redfin_home_type == redfin_type].index.values
    parcels.development_type_id.loc[idx] = redfin_devtype_map2[redfin_type]
    
    
parcels.development_type_id[parcels.development_type_id.isnull()*(parcels.res_type=='single')] = 'SF'
parcels.development_type_id[parcels.development_type_id.isnull()*(parcels.res_type=='multi')] = 'MF'
parcels.development_type_id[parcels.development_type_id.isnull()] = 'other'  ##These are the parcels to print out lu_type diagnostics for...
##Note these 'other' parcels will most typically be vacant

#Standardize the development_type_id coding
devcode_map = {"HM":"MF",
"HS":"SF",
"HT":"MF",
"ME":"MR",
"RB":"RT",
"RC":"BR",
"REC":"BR",
"RS":"RT",
" ":"other",
"VT":"LD",
"VAC":"LD",
"VA":"LD",
}

for devcode in devcode_map.keys():
    parcels.development_type_id[parcels.development_type_id == devcode] = devcode_map[devcode]
    
parcels['proportion_undevelopable'] = 0.0 ##Populate this with spatial function
parcels.land_use_type_id[parcels.land_use_type_id.isnull()] = ' '
parcels.development_type_id.value_counts()
parcels.county_id[parcels.county_id==' '] = 0
parcels.county_id = parcels.county_id.astype('int')

#SF-specific devtype correction due to residential sometimes being mistakenly coded as RT
parcels.development_type_id[(parcels.county_id == 75) & (parcels.res_type == 'single') & (parcels.development_type_id == 'RT')] = 'SF'
parcels.development_type_id[(parcels.county_id == 75) & (parcels.res_type == 'multi') & (parcels.development_type_id == 'RT')] = 'MF'

# Assign development type id based on gov_type status
parcels.gov_type = parcels.gov_type.fillna(0).astype('int')
parcels.development_type_id[(parcels.residential_units == 0) & (~parcels.res_type.isin(['single', 'multi'])) & (parcels.gov_type == 12)] = 'HP'
parcels.development_type_id[(parcels.residential_units == 0) & (~parcels.res_type.isin(['single', 'multi'])) & (parcels.gov_type == 17)] = 'SC'
parcels.development_type_id[(parcels.residential_units == 0) & (~parcels.res_type.isin(['single', 'multi'])) & (parcels.gov_type == 18)] = 'SH'
parcels.development_type_id[(parcels.residential_units == 0) & (~parcels.res_type.isin(['single', 'multi'])) & (parcels.gov_type == 19)] = 'GV'

# Set SCL common areas as undevelopable
scl_parcels = parcels[parcels.county_id == 85]
scl_parcels = scl_parcels[scl_parcels.apn.str.startswith('-')]
parcels.proportion_undevelopable.loc[scl_parcels.index.values] = 1.0

##############
###BUILDINGS##
##############

idx = (parcels.improvement_value > 0) | (parcels.year_built > 0) | (parcels.building_sqft > 0) | (parcels.non_residential_sqft > 0) | (parcels.residential_units > 0) | (parcels.stories > 0) | (parcels.sqft_per_unit > 0) | ((parcels.costar_property_type.str.len()> 1) & (~parcels.costar_property_type.isin(['Land','Land (Community Center)']))) | ((parcels.redfin_home_type.str.len()> 1) & (~parcels.redfin_home_type.isin(['Vacant Land','Other','Unknown'])))
buildings = parcels[idx]
print len(buildings)

buildings = buildings[['county_id', 'land_use_type_id', 'res_type', 'improvement_value', 'year_assessed', 'year_built', 'building_sqft', 'non_residential_sqft', 'residential_units', 'sqft_per_unit', 'stories', 'development_type_id', 'taz', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_elevators', 'costar_property_type', 'costar_secondary_type', 'costar_building_name', 'costar_rent']].copy(deep=True)

buildings['building_id'] = np.arange(len(buildings)) + 1
buildings.index.name = 'parcel_id'
buildings = buildings.reset_index()

## Incorporate the synthetic buildings added as necessary in some situations to match aggregate unit targets
new_res_buildings_df = new_res_buildings_df.rename(columns = {'gid':'parcel_id'})
new_res_buildings_df['land_use_type_id'] = 'from_imputation'
new_res_buildings_df['development_type_id'] = ''
new_res_buildings_df.development_type_id[new_res_buildings_df.res_type == 'single'] = 'SF'
new_res_buildings_df.development_type_id[new_res_buildings_df.res_type == 'multi'] = 'MF'
new_res_buildings_df['building_id'] = np.arange(buildings.building_id.max() + 1, buildings.building_id.max() + len(new_res_buildings_df) + 1)

new_nonres_buildings_df = new_nonres_buildings_df.rename(columns = {'gid':'parcel_id'})
new_nonres_buildings_df['land_use_type_id'] = 'from_imputation'
new_nonres_buildings_df['development_type_id'] = 'OF'
new_nonres_buildings_df['building_id'] = np.arange(new_res_buildings_df.building_id.max() + 1, new_res_buildings_df.building_id.max() + len(new_nonres_buildings_df) + 1)

# Tag the associated parcels with "add_synth_bldg" in imputation flag
idx = np.unique(new_res_buildings_df.parcel_id)
parcels.imputation_flag.loc[idx] = parcels.imputation_flag.loc[idx] + ', add_synth_res_bldg'

idx = np.unique(new_nonres_buildings_df.parcel_id)
parcels.imputation_flag.loc[idx] = parcels.imputation_flag.loc[idx] + ', add_synth_nonres_bldg'

# Merge the synthetic buildings with the rest
buildings = pd.concat([buildings, new_res_buildings_df])
buildings = pd.concat([buildings, new_nonres_buildings_df])

## Building column cleaning/imputation
buildings.residential_units[buildings.residential_units.isnull()] = 0
buildings.non_residential_sqft[buildings.non_residential_sqft.isnull()] = 0

# Upper and lower bound configuration (move settings to separate config file?)
year_built_lower_bound = 1790
year_built_upper_bound = 2015

sqft_per_unit_lower_bound = 200
sqft_per_unit_upper_bound = 30000

targetvalues['year_built_av_nonres'] = buildings[(~buildings.res_type.isin(['single', 'multi'])) & (buildings.year_built > year_built_lower_bound) & (buildings.year_built < year_built_upper_bound)].groupby('taz').year_built.mean()
buildings = pd.merge(buildings, targetvalues, left_on = 'taz', right_index = True, how = 'left')

buildings = buildings.set_index('building_id')

## YEAR BUILT

#   Residential building with out-of-bounds or null year_built- replace with observed zonal average of good data points
idx = (buildings.res_type.isin(['single', 'multi'])) & ((buildings.year_built < year_built_lower_bound) | (buildings.year_built > year_built_upper_bound) | (buildings.year_built.isnull()))
buildings.year_built[idx] = buildings.yearbuilt_av
# Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', res_zone_yrblt'

# If any residential buildings with year_built value still out of bounds (e.g. if zonal average data from previous step had bad value), use average across all residential buildings
idx = (buildings.res_type.isin(['single', 'multi'])) & buildings.year_built.isnull()
buildings.year_built[idx] = buildings.year_built[(buildings.res_type.isin(['single', 'multi']))].mean()
# Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', res_region_yrblt'

#   Non-residential building with out-of-bounds or null year_built- replace with zonal average of good data points
idx = (~buildings.res_type.isin(['single', 'multi'])) & ((buildings.year_built < year_built_lower_bound) | (buildings.year_built > year_built_upper_bound) | (buildings.year_built.isnull()))
buildings.year_built[idx] = buildings.year_built_av_nonres
# Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', nr_zone_yrblt'

# If any non-residential buildings with year_built value still out of bounds (e.g. if zonal average data from previous step had bad value), use average across all non-residential buildings
idx = (~buildings.res_type.isin(['single', 'multi'])) & buildings.year_built.isnull()
buildings.year_built[idx] = buildings.year_built[(~buildings.res_type.isin(['single', 'multi']))].mean()
# Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', nr_region_yrblt'

## NON-RESIDENTIAL SQFT

# If nonresidential structure (and this is known by btype and resunits), and nonres sqft is 0 but building_sqft is positive, nonres_sqft should equal building_sqft
idx = (~buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units == 0) & (buildings.non_residential_sqft == 0) & (buildings.building_sqft > 0)
buildings.non_residential_sqft[idx] = buildings.building_sqft
#Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', nrsqft_from_bsqft'

# If nonresidential structure with zero residential units, building_sqft should equal non-residential sqft
idx = (~buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units == 0)
buildings.building_sqft[idx] = buildings.non_residential_sqft
#Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', bsqft_from_nrsqft'

# SQFT PER UNIT
# Residential sqft_per_unit should be building_sqft / residential_units
idx = (buildings.res_type.isin(['single', 'multi'])) & (buildings.building_sqft > 0) & (buildings.residential_units > 0) & np.logical_or((buildings.sqft_per_unit == 0), (buildings.sqft_per_unit.isnull())) & (buildings.non_residential_sqft == 0)
buildings.sqft_per_unit[idx] = buildings.building_sqft[idx] / buildings.residential_units[idx]

# Replacing sqft_per_unit nulls/zeros with county avg of good data points
for cid in np.unique(buildings.county_id):
    if cid != 0:
        idx = (buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units > 0) & (buildings.county_id == cid) & (buildings.sqft_per_unit > 0)
        mean_sqft_per_unit = buildings[idx].sqft_per_unit.mean()
        if mean_sqft_per_unit < 1000: mean_sqft_per_unit = 1000
        idx2 = (buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units > 0) & (buildings.county_id == cid) & (np.logical_or((buildings.sqft_per_unit.isnull()) , (buildings.sqft_per_unit < 100)))
        buildings.sqft_per_unit[idx2] = mean_sqft_per_unit
        # Imputation flag
        idx_parcel = np.unique(buildings.parcel_id[idx2])
        parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', cnty_sq_du'
        
# Applying bounds to sqft per unit

# Enforce lower bound
idx = (buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units > 0) & (buildings.sqft_per_unit < sqft_per_unit_lower_bound)
buildings.sqft_per_unit[idx] = sqft_per_unit_lower_bound
#Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', sq_du_lower_bound'

# Enforce upper bound
idx = (buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units > 0) & (buildings.sqft_per_unit > sqft_per_unit_upper_bound)
buildings.sqft_per_unit[idx] = sqft_per_unit_upper_bound
# Imputation flag
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', sq_du_upper_bound'

buildings.sqft_per_unit[buildings.sqft_per_unit < 0] = 0
buildings.sqft_per_unit[buildings.sqft_per_unit > sqft_per_unit_upper_bound] = sqft_per_unit_upper_bound

# Residential building_sqft should equal sqft_per_unit * residential_units
idx = (buildings.res_type.isin(['single', 'multi'])) & (buildings.residential_units > 0) & (buildings.sqft_per_unit > 0) & (buildings.non_residential_sqft == 0)
buildings.building_sqft[idx] = buildings.sqft_per_unit[idx] * buildings.residential_units[idx]

# Building_sqft should equal nonres sqft + sqft_per_unit*residential units
buildings.sqft_per_unit[buildings.sqft_per_unit.isnull()] = 0
buildings.building_sqft = buildings.non_residential_sqft + (buildings.sqft_per_unit * buildings.residential_units)

# STORIES
# If stories null or less than 1, set to 1
idx = buildings.stories.isnull() | (buildings.stories < 1)
buildings.stories[idx] = 1
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', stories1'

idx = (buildings.stories == 1) & (buildings.costar_elevators > 0)
buildings.stories[idx] = 3
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', stories3_cs_elevator'

idx = buildings.stories > 90
buildings.stories[buildings.stories > 99] = 1  ##Marin data problem
buildings.stories[buildings.stories > 98] = 2 ##SCL data problem
buildings.stories[buildings.stories > 90] = 1 ##SF data problem
idx_parcel = np.unique(buildings.parcel_id[idx])
parcels.imputation_flag.loc[idx_parcel] = parcels.imputation_flag.loc[idx_parcel] + ', stories_corrected'

# Replace nulls with zero
buildings.improvement_value[buildings.improvement_value.isnull()] = 0.0
buildings.year_assessed[buildings.year_assessed.isnull()] = 0.0
buildings.taz[buildings.taz.isnull()] = 0.0
buildings.res_type[buildings.res_type.isnull()] = 'other'

# Cut down to necessary building columns
buildings2 = buildings[['parcel_id','county_id', 'land_use_type_id', 'res_type', 'improvement_value', 'year_assessed', 'year_built', 'building_sqft', 'non_residential_sqft', 'residential_units', 'sqft_per_unit', 'stories', 'development_type_id', 'taz', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_property_type', 'costar_rent']]

print buildings2.development_type_id.value_counts()


############
##SCALING###
############

## Scaling for zonal targets:  (note:  scaling residential prices takes place in price_imputation)

targets_residential_year_built = pd.DataFrame(
    {'column_name': ['year_built']*len(targetvalues),
     'target_value': targetvalues.yearbuilt_av.values,
     'target_metric': ['mean']*len(targetvalues),
     'filters': ('(residential_units > 0) & (taz == ' + pd.Series(targetvalues.index.values).astype('str')) + ')',
     'clip_low': [np.nan]*len(targetvalues),
     'clip_high': [np.nan]*len(targetvalues),
     'int_result': [np.nan]*len(targetvalues)})

targets_non_residential_sqft = pd.DataFrame(
    {'column_name': ['non_residential_sqft']*len(targetunits),
     'target_value': targetunits.targetnonressqft.values,
     'target_metric': ['sum']*len(targetunits),
     'filters': ('(non_residential_sqft > 0) & (taz == ' + pd.Series(targetunits.index.values).astype('str')) + ')',
     'clip_low': [np.nan]*len(targetunits),
     'clip_high': [np.nan]*len(targetunits),
     'int_result': [np.nan]*len(targetunits)})

buildings2 = scl.scale_to_targets_from_table(buildings2, targets_residential_year_built)

targets_non_residential_sqft['taz'] = targetunits.index.values
targets_non_residential_sqft = targets_non_residential_sqft.set_index('taz')
targets_non_residential_sqft['existing_nrsqft'] = buildings2.groupby('taz').non_residential_sqft.sum()
targets_non_residential_sqft.target_value[targets_non_residential_sqft.target_value < targets_non_residential_sqft.existing_nrsqft] = targets_non_residential_sqft.existing_nrsqft[targets_non_residential_sqft.target_value < targets_non_residential_sqft.existing_nrsqft]
del targets_non_residential_sqft['existing_nrsqft']
buildings2 = scl.scale_to_targets_from_table(buildings2, targets_non_residential_sqft)

print buildings[buildings.building_sqft == 0].res_type.value_counts()  
print len(buildings2[(buildings2.building_sqft == 0) & (buildings2.res_type=='other')])

# Post scaling bound-checking
buildings2.year_built[buildings2.year_built > year_built_upper_bound] = year_built_upper_bound
buildings2.year_built[buildings2.year_built < year_built_lower_bound] = year_built_lower_bound


# COMPARE WITH TARGETS
targetunits['sf'] = buildings2[buildings2.res_type == 'single'].groupby('taz').residential_units.sum()
targetunits['mf'] = buildings2[buildings2.res_type == 'multi'].groupby('taz').residential_units.sum()
targetunits['nrsqft'] = buildings2.groupby('taz').non_residential_sqft.sum()
print targetunits[['sf','targetSF','mf','targetMF', 'nrsqft', 'targetnonressqft']].head()
targetunits[['sf','targetSF','mf','targetMF', 'nrsqft', 'targetnonressqft']].sum()
# summary_output_path = loader.get_path('out/regeneration/summaries/built_space_summary.csv')
# targetunits[['sf','targetSF','mf','targetMF', 'nrsqft', 'targetnonressqft']].to_csv(summary_output_path)
targetunits = targetunits[['sf','targetSF','mf','targetMF', 'nrsqft', 'targetnonressqft']]
df_to_db(targetunits, 'summary_built_space', schema=loader.tables.public)


# EXPORT BUILDINGS TO DB
print 'Loading processed buildings to db'
df_to_db(buildings2, 'buildings', schema=loader.tables.public)


## Create geom_id (serves similar purpose to joinnumA) on parcels based on integer representation of geometry hash
idx = []
geom_hashes = []
for i, geom in parcels.geom.iteritems():
    idx.append(i)
    md5_hash = str(hashlib.md5(geom).hexdigest())
    geom_hashes.append(int(md5_hash[0:11], 16))
    
parcel_identifier = pd.Series(geom_hashes, index = idx)
parcels['geom_id'] = parcel_identifier


# EXPORT PROCESSED PARCELS TO DB
parcels['parcel_acres'] = parcels.calc_area/4046.86
parcels['taz_id'] = parcels.taz
parcels['tax_exempt_status'] = parcels.tax_exempt
parcels2 = parcels[['development_type_id', 'land_value', 'parcel_acres', 'county_id', 'taz_id', 'proportion_undevelopable', 'tax_exempt_status', 'apn', 'parcel_id_local', 'geom_id', 'imputation_flag']]
devtype_devid_xref = {'SF':1, 'MF':2, 'MFS':3, 'MH':4, 'MR':5, 'GQ':6, 'RT':7, 'BR':8, 'HO':9, 'OF':10, 'OR':11, 'HP':12, 'IW':13, 
                      'IL':14, 'IH':15, 'VY':16, 'SC':17, 'SH':18, 'GV':19, 'VP':20, 'VA':21, 'PG':22, 'PL':23, 'TR':24, 'LD':25, 'other':-1}
for dev in devtype_devid_xref.keys():
    parcels2.development_type_id[parcels2.development_type_id == dev] = devtype_devid_xref[dev]
parcels2.taz_id = parcels2.taz_id.astype('int')
parcels2.development_type_id = parcels2.development_type_id.astype('int')
parcels2.tax_exempt_status = parcels2.tax_exempt_status.fillna(0).astype('int')
parcels2.land_value = parcels2.land_value.fillna(0.0)
parcels2.parcel_id_local = parcels2.parcel_id_local.fillna(' ')
parcels2.parcel_acres[parcels2.parcel_acres.isnull()] = 2.5
parcels2.index.name = 'parcel_id'

print 'Loading processed parcels to db'
df_to_db(parcels2, 'parcel', schema=loader.tables.public)

# Attach geom to the processed parcels
print 'Attaching geometry to the processed parcels'
exec_sql("""
alter table parcel add geom geometry(MultiPolygon); 
SELECT UpdateGeometrySRID('parcel', 'geom', 2768);
update parcel set geom = a.geom from parcels a where parcel.parcel_id = a.gid;
update parcel set geom_id = parcel_id where geom is null;
""")

# Create spatial index.
print 'Spatially indexing the processed parcels'
exec_sql("""
CREATE INDEX parcel_geom_gist ON parcel
  USING gist (geom);
""")

# Add XY coords to processed parcels
print 'Add x and y coords on the processed parcels'
exec_sql("alter table parcel add centroid geometry;")
exec_sql("update parcel set centroid = ST_centroid(geom);")
exec_sql('ALTER TABLE parcel ADD x numeric;')
exec_sql('ALTER TABLE parcel ADD y numeric;')
exec_sql("update parcel set x = ST_X(ST_Transform(centroid, 4326));")
exec_sql("update parcel set y = ST_Y(ST_Transform(centroid, 4326));")

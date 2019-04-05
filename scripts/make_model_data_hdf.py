import pandas as pd
import numpy as np


local_data_dir = '/home/data/spring_2019/2025/'
csv_fnames = {
    'parcels': 'parcels.csv',
    'buildings': 'buildings.csv',
    'jobs': 'jobs.csv',
    'establishments': 'establishments.csv',
    'households': 'households.csv',
    'persons': 'persons.csv',
    'rentals': 'craigslist.csv',
    'units': 'units.csv',
    'skims': 'skims.csv',
    'beam_skims': 'smart-1Apr2019-sc-b-ht-2025-20.skimsExcerpt.csv',
    'zones': 'zones.csv'
}

try:
    parcels = pd.read_csv(
        local_data_dir + csv_fnames['parcels'], index_col='parcel_id',
        dtype={'parcel_id': int, 'block_id': str, 'apn': str})
except ValueError:
    parcels = pd.read_csv(
        local_data_dir + csv_fnames['parcels'], index_col='primary_id',
        dtype={'primary_id': int, 'block_id': str, 'apn': str})

buildings = pd.read_csv(
    local_data_dir + csv_fnames['buildings'], index_col='building_id',
    dtype={'building_id': int, 'parcel_id': int})
buildings['res_sqft_per_unit'] = buildings[
    'residential_sqft'] / buildings['residential_units']
buildings['res_sqft_per_unit'][buildings['res_sqft_per_unit'] == np.inf] = 0

# building_types = pd.read_csv(
#     d + 'building_types.csv',
#     index_col='building_type_id', dtype={'building_type_id': int})

# building_types.head()

try:
    rentals = pd.read_csv(
        local_data_dir + csv_fnames['rentals'],
        index_col='pid', dtype={
            'pid': int, 'date': str, 'region': str,
            'neighborhood': str, 'rent': float, 'sqft': float,
            'rent_sqft': float, 'longitude': float,
            'latitude': float, 'county': str, 'fips_block': str,
            'state': str, 'bathrooms': str})
except ValueError:
    rentals = pd.read_csv(
        local_data_dir + csv_fnames['rentals'],
        index_col=0, dtype={
            'date': str, 'region': str,
            'neighborhood': str, 'rent': float, 'sqft': float,
            'rent_sqft': float, 'longitude': float,
            'latitude': float, 'county': str, 'fips_block': str,
            'state': str, 'bathrooms': str})

units = pd.read_csv(
    local_data_dir + csv_fnames['units'], index_col='unit_id',
    dtype={'unit_id': int, 'building_id': int})

try:
    households = pd.read_csv(
        local_data_dir + csv_fnames['households'],
        index_col='household_id', dtype={
            'household_id': int, 'block_group_id': str, 'state': str,
            'county': str, 'tract': str, 'block_group': str,
            'building_id': int, 'unit_id': int, 'persons': float})
except ValueError:
    households = pd.read_csv(
        local_data_dir + csv_fnames['households'],
        index_col=0, dtype={
            'household_id': int, 'block_group_id': str, 'state': str,
            'county': str, 'tract': str, 'block_group': str,
            'building_id': int, 'unit_id': int, 'persons': float})
    households.index.name = 'household_id'

try:
    persons = pd.read_csv(
        local_data_dir + csv_fnames['persons'], index_col='person_id',
        dtype={'person_id': int, 'household_id': int})
except ValueError:
    persons = pd.read_csv(
        local_data_dir + csv_fnames['persons'], index_col=0,
        dtype={'person_id': int, 'household_id': int})
    persons.index.name = 'person_id'

try:
    jobs = pd.read_csv(
        local_data_dir + csv_fnames['jobs'], index_col='job_id',
        dtype={'job_id': int, 'building_id': int})
except ValueError:
    jobs = pd.read_csv(
        local_data_dir + csv_fnames['jobs'], index_col=0,
        dtype={'job_id': int, 'building_id': int})
    jobs.index.name = 'job_id'

establishments = pd.read_csv(
    local_data_dir + csv_fnames['establishments'],
    index_col='establishment_id', dtype={
        'establishment_id': int, 'building_id': int,
        'primary_id': int})

zones = pd.read_csv(
    local_data_dir + 'zones.csv', index_col='zone_id')

# beam_nodes_fname = 'beam-network-nodes.csv'
# beam_links_fname = '10.linkstats.csv'
# beam_links_filtered_fname = 'beam_links_8am.csv'
# with open(b + beam_links_filtered_fname, 'w') as f:
#     p1 = subprocess.Popen(
#         ["cat", b + beam_links_fname], stdout=PIPE)
#     p2 = subprocess.Popen([
#         "awk", "-F", ",",
#         '(NR==1) || ($4 == "8.0" && $8 == "AVG")'],
#         stdin=p1.stdout, stdout=f)
#     p2.wait()

# nodesbeam = pd.read_csv(b + beam_nodes_fname).set_index('id')
# edgesbeam = pd.read_csv(b + beam_links_filtered_fname).set_index('link')

# nodeswalk = pd.read_csv(d + 'bayarea_walk_nodes.csv').set_index('osmid')
# edgeswalk = pd.read_csv(d + 'bayarea_walk_edges.csv').set_index('uniqueid')

# nodessmall = pd.read_csv(
#     d + 'bay_area_tertiary_strongly_nodes.csv').set_index('osmid')
# edgessmall = pd.read_csv(
#     d + 'bay_area_tertiary_strongly_edges.csv').set_index('uniqueid')

skims = pd.read_csv(local_data_dir + csv_fnames['skims'], index_col=0)
beam_skims = pd.read_csv(
    local_data_dir + csv_fnames['beam_skims'])

store = pd.HDFStore('../data/baus_model_data_2025.h5')
store.put('parcels', parcels)
store.put('buildings_preproc', buildings)
# store.put('building_types', building_types)
store.put('units', units)
store.put('rentals', rentals)
store.put('households_preproc', households)
store.put('persons', persons)
store.put('jobs_preproc', jobs)
store.put('establishments', establishments)
# store.put('nodesbeam',nodesbeam)
# store.put('edgesbeam',edgesbeam)
# store.put('nodeswalk',nodeswalk)
# store.put('edgeswalk',edgeswalk)
# store.put('nodessmall',nodessmall)
# store.put('edgessmall',edgessmall)
store.put('skims', skims)
store.put('zones', zones)
store.put('beam_skims', beam_skims)
store.keys()

store.close()

import pandas as pd
import numpy as np
import argparse
import os

"""
This script takes the individual UrbanSim input .csv files and
compiles them into an (python 2) .h5 data store object, stored
locally, and used for either estimation, simulation or both in
bayarea_urbansim UrbanSim implementation. The last simulation
step in bayarea_urbansim then converts the updated .h5 back to
individual csv's for use in ActivitySynth and elsewhere.
"""

baseyear = False
beam_bucket = 'urbansim-beam'
csv_fnames = {
    'parcels': 'parcels.csv',
    'buildings': 'buildings.csv',
    'jobs': 'jobs.csv',
    'establishments': 'establishments.csv',
    'households': 'households.csv',
    'persons': 'persons.csv',
    'rentals': 'craigslist.csv',
    'units': 'units.csv',
    'mtc_skims': 'mtc_skims.csv',
    'beam_skims_raw': '30.skims-smart-23April2019-baseline.csv.gz',
    'zones': 'zones.csv',
    # the following nodes and edges .csv's aren't used by bayarea_urbansim
    # they're just being loaded here so they can be passed through to the
    # output data directory for use in activitysynth
    'drive_nodes': 'bay_area_tertiary_strongly_nodes.{0}',
    'drive_edges': 'bay_area_tertiary_strongly_edges.{0}',
    'walk_nodes': 'bayarea_walk_nodes.{0}',
    'walk_edges': 'bayarea_walk_edges.{0}',
}
data_store_fname = 'baus_model_data.h5'
nodes_and_edges = False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Make H5 store from csvs.')

    parser.add_argument(
        '--baseyear', '-b', action='store_true',
        help='specify the simulation year')
    parser.add_argument(
        '--input-data-dir', '-i', action='store', dest='input_data_dir',
        help='full (pandas-compatible) path to input data directory',
        required=True)
    parser.add_argument(
        '--output-data-dir', '-o', action='store', dest='output_data_dir',
        help='full path to the LOCAL output data directory',
        required=True)
    parser.add_argument(
        '--output-fname', '-f', action='store', dest='output_fname',
        help='filename of the .h5 datastore')
    parser.add_argument(
        '--nodes-and-edges', '-n', action='store_true', dest='nodes_and_edges')

    options = parser.parse_args()

    if options.baseyear:
        baseyear = options.baseyear

    if options.nodes_and_edges:
        nodes_and_edges = options.nodes_and_edges

    if options.output_fname:
        data_store_fname = options.output_fname

    input_data_dir = options.input_data_dir
    output_data_dir = options.output_data_dir

    try:
        parcels = pd.read_csv(
            input_data_dir + csv_fnames['parcels'], index_col='parcel_id',
            dtype={'parcel_id': int, 'block_id': str, 'apn': str})
    except ValueError:
        parcels = pd.read_csv(
            input_data_dir + csv_fnames['parcels'], index_col='primary_id',
            dtype={'primary_id': int, 'block_id': str, 'apn': str})

    buildings = pd.read_csv(
        input_data_dir + csv_fnames['buildings'], index_col='building_id',
        dtype={'building_id': int, 'parcel_id': int})
    buildings['res_sqft_per_unit'] = buildings[
        'residential_sqft'] / buildings['residential_units']
    buildings['res_sqft_per_unit'][
        buildings['res_sqft_per_unit'] == np.inf] = 0

    # building_types = pd.read_csv(
    #     d + 'building_types.csv',
    #     index_col='building_type_id', dtype={'building_type_id': int})

    # building_types.head()

    try:
        rentals = pd.read_csv(
            input_data_dir + csv_fnames['rentals'],
            index_col='pid', dtype={
                'pid': int, 'date': str, 'region': str,
                'neighborhood': str, 'rent': float, 'sqft': float,
                'rent_sqft': float, 'longitude': float,
                'latitude': float, 'county': str, 'fips_block': str,
                'state': str, 'bathrooms': str})
    except ValueError:
        rentals = pd.read_csv(
            input_data_dir + csv_fnames['rentals'],
            index_col=0, dtype={
                'date': str, 'region': str,
                'neighborhood': str, 'rent': float, 'sqft': float,
                'rent_sqft': float, 'longitude': float,
                'latitude': float, 'county': str, 'fips_block': str,
                'state': str, 'bathrooms': str})

    units = pd.read_csv(
        input_data_dir + csv_fnames['units'], index_col='unit_id',
        dtype={'unit_id': int, 'building_id': int})

    try:
        households = pd.read_csv(
            input_data_dir + csv_fnames['households'],
            index_col='household_id', dtype={
                'household_id': int, 'block_group_id': str, 'state': str,
                'county': str, 'tract': str, 'block_group': str,
                'building_id': int, 'unit_id': int, 'persons': float})
    except ValueError:
        households = pd.read_csv(
            input_data_dir + csv_fnames['households'],
            index_col=0, dtype={
                'household_id': int, 'block_group_id': str, 'state': str,
                'county': str, 'tract': str, 'block_group': str,
                'building_id': int, 'unit_id': int, 'persons': float})
        households.index.name = 'household_id'

    try:
        persons = pd.read_csv(
            input_data_dir + csv_fnames['persons'], index_col='person_id',
            dtype={'person_id': int, 'household_id': int})
    except ValueError:
        persons = pd.read_csv(
            input_data_dir + csv_fnames['persons'], index_col=0,
            dtype={'person_id': int, 'household_id': int})
        persons.index.name = 'person_id'

    try:
        jobs = pd.read_csv(
            input_data_dir + csv_fnames['jobs'], index_col='job_id',
            dtype={'job_id': int, 'building_id': int})
    except ValueError:
        jobs = pd.read_csv(
            input_data_dir + csv_fnames['jobs'], index_col=0,
            dtype={'job_id': int, 'building_id': int})
        jobs.index.name = 'job_id'

    establishments = pd.read_csv(
        input_data_dir + csv_fnames['establishments'],
        index_col='establishment_id', dtype={
            'establishment_id': int, 'building_id': int,
            'primary_id': int})

    zones = pd.read_csv(
        input_data_dir + 'zones.csv', index_col='zone_id')

    mtc_skims = pd.read_csv(
        input_data_dir + csv_fnames['mtc_skims'], index_col=0)

    beam_skims_raw = pd.read_csv(
        input_data_dir + csv_fnames['beam_skims_raw'])
    beam_skims_raw.rename(columns={
        'generalizedCost': 'gen_cost', 'origTaz': 'from_zone_id',
        'destTaz': 'to_zone_id'}, inplace=True)

    # this data store is just a temp file that only needs to exist
    # while the simulation is running. data is stored as csv's
    # before and afterwards. therefore a temporary, relative filepath
    # is specified here.

    output_filepath = os.path.join(output_data_dir, data_store_fname)
    if os.path.exists(output_filepath):
        os.remove(output_filepath)
        print('Deleting existing data store to create the new one...')
    store = pd.HDFStore(output_filepath)

    store.put('parcels', parcels, format='t')
    store.put('units', units, format='t')
    store.put('rentals', rentals, format='t')

    # data pre-processing hasn't yet taken place if
    # starting with base-year input data
    if baseyear:

        store.put('households', households, format='t')
        store.put('jobs', jobs, format='t')
        store.put('buildings', buildings, format='t')

    # if starting from non-base-year (i.e. intra-simulation) data
    # then the pre-processing data steps should have already
    # occurred and we simply rename the main data tables so that
    # bayarea_urbansim doesn't try to re-pre-process them
    else:

        store.put('households_preproc', households, format='t')
        store.put('jobs_preproc', jobs, format='t')
        store.put('buildings_preproc', buildings, format='t')

    store.put('persons', persons, format='t')
    store.put('establishments', establishments, format='t')
    store.put('mtc_skims', mtc_skims, format='t')
    store.put('zones', zones, format='t')
    store.put('beam_skims_raw', beam_skims_raw, format='t')

    if nodes_and_edges:

        drive_nodes = pd.read_csv(
            input_data_dir + csv_fnames['drive_nodes']).set_index('osmid')
        drive_edges = pd.read_csv(
            input_data_dir + csv_fnames['drive_edges']).set_index('uniqueid')
        walk_nodes = pd.read_csv(
            input_data_dir + csv_fnames['walk_nodes']).set_index('osmid')
        walk_edges = pd.read_csv(
            input_data_dir + csv_fnames['walk_edges']).set_index('uniqueid')

        store.put('drive_nodes', drive_nodes, format='t')
        store.put('drive_edges', drive_edges, format='t')
        store.put('walk_nodes', walk_nodes, format='t')
        store.put('walk_edges', walk_edges, format='t')

    store.keys()

    store.close()
    print('UrbanSim model data now available at {0}'.format(
        os.path.abspath(output_filepath)))

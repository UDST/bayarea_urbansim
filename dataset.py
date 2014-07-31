import numpy as np
import pandas as pd
import os
import assumptions
from urbansim.utils import misc
import urbansim.sim.simulation as sim

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


@sim.table_source('nodes')
def nodes():
    # default will fetch off disk unless networks have already been run
    print "WARNING: fetching precomputed nodes off of disk"
    df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes.csv'), index_col='node_id')
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    return df


@sim.table_source('nodes_prices')
def nodes_prices():
    # default will fetch off disk unless networks have already been run
    print "WARNING: fetching precomputed nodes_prices off of disk"
    df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes_prices.csv'), index_col='node_id')
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    return df


@sim.table_source('building_sqft_per_job')
def building_sqft_per_job():
    df = pd.read_csv(os.path.join(misc.data_dir(), 'building_sqft_job.csv'),
                     index_col='building_type_id')
    return df


@sim.table_source('jobs')
def jobs(store):
    nets = store['nets']
    # go from establishments to jobs
    df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)].reset_index()
    df.index.name = 'job_id'
    return df


@sim.table_source('buildings')
def buildings(store):
    df = store['buildings']
    for col in ["residential_sales_price", "residential_rent", "non_residential_rent"]:
        df[col] = np.nan
    return df


@sim.table_source('households')
def households(store):
    df = store['households']
    return df


@sim.table_source('parcels')
def parcels(store):
    df = store['parcels']
    return df


@sim.table_source('homesales')
def homesales(store):
    df = store['homesales']
    df = df.reset_index(drop=True)
    return df


@sim.table_source('apartments')
def apartments(store):
    df = store['apartments']
    return df


@sim.table_source('costar')
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


@sim.table_source('zoning_for_parcels')
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')
    return df


@sim.table_source('zoning')
def zoning(store):
    df = store['zoning']
    return df


@sim.table_source('zoning_baseline')
def zoning_baseline(zoning, zoning_for_parcels):
    df = pd.merge(zoning_for_parcels.to_frame(),
                  zoning.to_frame(),
                  left_on='zoning',
                  right_index=True)
    return df


@sim.table_source('zoning_test')
def zoning_test():
    parcels_to_zoning = pd.read_csv(os.path.join(misc.data_dir(), 'parcels_to_zoning.csv'), low_memory=False)
    scenario_zoning = pd.read_excel(os.path.join(misc.data_dir(), 'zoning_scenario_test.xls'),
                                    sheetname='zoning_lookup')
    df = pd.merge(parcels_to_zoning,
                  scenario_zoning,
                  on=['jurisdiction', 'pda', 'tpp', 'expansion'],
                  how='left')
    df = df.set_index(df.parcel_id)
    return df


sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'apartments', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'buildings', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes_prices', 'buildings', cast_index=True, onto_on='_node_id')
sim.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
sim.broadcast('buildings', 'households', cast_index=True, onto_on='building_id')
sim.broadcast('buildings', 'jobs', cast_index=True, onto_on='building_id')
import numpy as np
import pandas as pd
import os
import assumptions
import utils
from urbansim.utils import misc
import urbansim.sim.simulation as sim

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


@sim.table_source('jobs')
def jobs(store):
    nets = store['nets']
    # go from establishments to jobs
    df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)]\
        .reset_index()
    df.index.name = 'job_id'
    return df


@sim.table_source('buildings')
def buildings(store, households, jobs, building_sqft_per_job):
    df = store['buildings']
    # non-res buildings get no residential units by default
    df.residential_units[df.building_type_id > 3] = 0
    for col in ["residential_sales_price", "residential_rent",
                "non_residential_rent"]:
        df[col] = 0

    # prevent overfull buildings (residential)
    df["residential_units"] = pd.concat([df.residential_units,
                                         households.building_id.value_counts()
                                         ], axis=1).max(axis=1)

    # prevent overfull buildings (non-residential)
    tmp_df = pd.concat([
        df.non_residential_sqft,
        jobs.building_id.value_counts() *
        df.building_type_id.fillna(-1).map(building_sqft_per_job)
    ], axis=1)
    df["non_residential_sqft"] = tmp_df.max(axis=1).apply(np.ceil)

    df = df[df.building_type_id > 0]
    df = df[df.building_type_id <= 14]
    df = utils.fill_nas_from_config('buildings', df)
    return df


@sim.table_source('households')
def households(store):
    df = store['households']
    # have to do it this way to prevent circular reference
    df.building_id.loc[~df.building_id.isin(store['buildings'].index)] = -1
    return df


@sim.table_source('parcels')
def parcels(store):
    df = store['parcels']
    return df


# a table of home sales data
@sim.table_source('homesales')
def homesales(store):
    df = store['homesales']
    df = df.reset_index(drop=True)
    return df


# a table of apartment rental data
@sim.table_source('apartments')
def apartments(store):
    df = store['apartments']
    return df


# non-residential rent data
@sim.table_source('costar')
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


# these are shapes - "zones" in the bay area
@sim.table_source('zones')
def zones(store):
    df = store['zones']
    return df


# this is the mapping of parcels to zoning attributes
@sim.table_source('zoning_for_parcels')
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')
    return df


# this is the actual zoning
@sim.table_source('zoning')
def zoning(store):
    df = store['zoning']
    return df


# zoning for use in the "baseline" scenario
# comes in the hdf5
@sim.table_source('zoning_baseline')
def zoning_baseline(zoning, zoning_for_parcels):
    df = pd.merge(zoning_for_parcels.to_frame(),
                  zoning.to_frame(),
                  left_on='zoning',
                  right_index=True)
    return df


# zoning for use in the "test" scenario - is often
# specified by the user e.g. in arcgis or excel and
# so is kept outside of the hdf5
@sim.table_source('zoning_test')
def zoning_test():
    parcels_to_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                                 'parcels_to_zoning.csv'),
                                    low_memory=False)
    scenario_zoning = pd.read_excel(os.path.join(misc.data_dir(),
                                                 'zoning_scenario_test.xls'),
                                    sheetname='zoning_lookup')
    df = pd.merge(parcels_to_zoning,
                  scenario_zoning,
                  on=['jurisdiction', 'pda', 'tpp', 'expansion'],
                  how='left')
    df = df.set_index(df.parcel_id)
    return df


# these are dummy returns that last until accessibility runs
@sim.table("nodes")
def nodes():
    return pd.DataFrame()


@sim.table("nodes_prices")
def nodes_prices():
    return pd.DataFrame()


# this specifies the relationships between tables
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'apartments', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'buildings', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes', 'parcels', cast_index=True, onto_on='_node_id')
sim.broadcast('nodes_prices', 'buildings', cast_index=True, onto_on='_node_id')
sim.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
sim.broadcast('buildings', 'households', cast_index=True,
              onto_on='building_id')
sim.broadcast('buildings', 'jobs', cast_index=True, onto_on='building_id')

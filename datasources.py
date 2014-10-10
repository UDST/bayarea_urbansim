import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import urbansim.sim.simulation as sim


# the starter submarket shifters is one per zone, all set to 1.0
sim.add_injectable("price_shifters",
                   pd.Series(1, sim.get_table('zones').index))


sim.add_injectable("building_sqft_per_job",
                   sim.get_injectable('settings')["building_sqft_per_job"])


@sim.table_source('jobs')
def jobs(store):
    nets = store['nets']
    # go from establishments to jobs
    df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)]\
        .reset_index()
    df.index.name = 'job_id'
    return df


# a table of home sales data
@sim.table_source('homesales')
def homesales(store):
    df = store['homesales']
    df = df.reset_index(drop=True)
    return df


# non-residential rent data
@sim.table_source('costar')
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


# this is the mapping of parcels to zoning attributes
@sim.table_source('zoning_for_parcels')
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')
    return df


# this is the actual baseline zoning, now editable in an excel file
# (the zoning from the h5 file doesn't have all the parameters)
# instead of creating a new h5 file I'm going to add zoning as a csv file
# which is easily browsable in excel and is only 170k bytes
@sim.table_source('zoning')
def zoning(store):
    df = store.zoning
    df2 = pd.read_csv(os.path.join(misc.data_dir(), "baseline_zoning.csv"),
                      index_col="id")
    # this function actually overwrites all columns in the h5 zoning that are
    # available in the csv zoning, but preserves the allowable building types
    for col in df2.columns:
        df[col] = df2[col]
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


# this is really bizarre, but the parcel table I have right now has empty
# zone_ids for a few parcels.  Not enough to worry about so just filling with
# the mode
@sim.table_source('parcels')
def parcels(store):
    df = store['parcels']
    cfg = {
        "fill_nas": {
            "zone_id": {
                "how": "mode",
                "type": "int"
            },
            "shape_area": {
                "how": "median",
                "type": "float"
            }
        }
    }
    df = utils.table_reprocess(cfg, df)
    df["zone_id"] = df.zone_id.replace(0, 1)
    return df


@sim.table_source('buildings')
def buildings(store, households, jobs, building_sqft_per_job, settings):
    # start with buildings from urbansim_defaults
    df = datasources.buildings(store, households, jobs,
                               building_sqft_per_job, settings)
    # set the vacancy rate in each building to 5% for testing purposes
    vacancy = .25
    df["residential_units"] = (households.building_id.value_counts() *
                               (1.0+vacancy)).apply(np.floor).astype('int')
    df["residential_units"] = df.residential_units.fillna(0)
    return df


# this specifies the relationships between tables
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')

import numpy as np
import pandas as pd
import os
import assumptions
from urbansim_defaults import datasources
from urbansim.utils import misc
import urbansim.sim.simulation as sim


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


# this specifies the relationships between tables
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')
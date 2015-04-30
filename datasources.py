import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import urbansim.sim.simulation as sim


@sim.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@sim.table('jobs', cache=True)
def jobs(store):
    # nets = store['nets']
    ###go from establishments to jobs
    # df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)]\
        # .reset_index()
    # df.index.name = 'job_id'
    df = store['jobs']
    return df


# a table of home sales data
@sim.table('homesales', cache=True)
def homesales(store):
    df = store['homesales']
    df = df.reset_index(drop=True)
    return df


# non-residential rent data
@sim.table('costar', cache=True)
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


# this is the mapping of parcels to zoning attributes
@sim.table('zoning_for_parcels', cache=True)
def zoning_for_parcels(store):
    df = store['zoning_for_parcels']
    df = df.reset_index().drop_duplicates(subset='parcel').set_index('parcel')
    return df


# this is the actual baseline zoning, now editable in an excel file
# (the zoning from the h5 file doesn't have all the parameters)
# instead of creating a new h5 file I'm going to add zoning as a csv file
# which is easily browsable in excel and is only 170k bytes
@sim.table('zoning', cache=True)
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
@sim.table('zoning_baseline', cache=True)
def zoning_baseline(zoning, zoning_for_parcels):
    df = pd.merge(zoning_for_parcels.to_frame(),
                  zoning.to_frame(),
                  left_on='zoning',
                  right_index=True)
    return df


# zoning for use in the "test" scenario - is often
# specified by the user e.g. in arcgis or excel and
# so is kept outside of the hdf5
@sim.table('zoning_test', cache=True)
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
@sim.table('parcels', cache=True)
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
    
    # Node id
    # import pandana as pdna
    # st = pd.HDFStore(os.path.join(misc.data_dir(), "osm_bayarea.h5"), "r")
    # nodes, edges = st.nodes, st.edges
    net = sim.get_injectable('net')
    # net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       # edges[["weight"]])
    # net.precompute(3000)
    # sim.add_injectable("net", net)
    
    # p = parcels.to_frame(parcels.local_columns)
    df['_node_id'] = net.get_node_ids(df['x'], df['y'])
    # sim.add_table("parcels", p)
    return df


@sim.table('parcels_geography', cache=True)
def parcels_geography():
    return pd.read_csv(os.path.join(misc.data_dir(), "parcels_geography.csv"),
                      index_col="parcel_id")


@sim.table('buildings', cache=True)
def buildings(store, households, jobs, building_sqft_per_job, settings):
    # start with buildings from urbansim_defaults
    df = datasources.buildings(store, households, jobs,
                               building_sqft_per_job, settings)

    df = df.drop(['development_type_id', 'improvement_value', 'sqft_per_unit', 'nonres_rent_per_sqft', 'res_price_per_sqft', 'redfin_sale_price', 'redfin_sale_year', 'redfin_home_type', 'costar_property_type', 'costar_rent'], axis=1)

    # set the vacancy rate in each building to 5% for testing purposes
    #vacancy = .25
    #df["residential_units"] = (households.building_id.value_counts() *
    #                           (1.0+vacancy)).apply(np.floor).astype('int')
    df["residential_units"] = df.residential_units.fillna(0)
    return df


# this specifies the relationships between tables
sim.broadcast('parcels_geography', 'buildings', cast_index=True,
              onto_on='parcel_id')
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')

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


# the estimation data is not in the buildings table - they are the same
@sim.table('homesales', cache=True)
def homesales(store):
    # we need to read directly from the store here.  Why?  The buildings
    # table itself drops a bunch of columns we need - most notably the
    # redfin_sales_price column.  Why?  Because the developer model will
    # append rows (new buildings) to the buildings table and we don't want
    # the developer model to know about redfin_sales_price (which is
    # meaningless for forecast buildings)
    df = store['buildings']
    df = df.dropna(subset=["redfin_sale_price"])
    df["price_per_sqft"] = df.eval('redfin_sale_price / sqft_per_unit')
    df = df.query("sqft_per_unit > 200")
    df = df.dropna(subset=["price_per_sqft"])
    return df


@sim.column('homesales', 'node_id', cache=True)
def node_id(homesales, parcels):
    return misc.reindex(parcels.node_id, homesales.parcel_id)


@sim.column('homesales', 'zone_id', cache=True)
def zone_id(homesales, parcels):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


@sim.column('homesales', 'zone_id', cache=True)
def zone_id(homesales, parcels):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


# non-residential rent data
@sim.table('costar', cache=True)
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


# zoning for use in the "baseline" scenario
# comes in the hdf5
@sim.table('zoning_baseline', cache=True)
def zoning_baseline(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), "zoning_parcels.csv"),
                     index_col="geom_id")

    # a zero zoning limit is actually a nan
    for s in ["max_far", "max_dua", "max_height"]:
        df[s].replace(0, np.nan, inplace=True)

    # need to reindex from geom id to the id used on parcels
    s = parcels.geom_id # get geom_id
    s = pd.Series(s.index, index=s.values) # invert series
    df["new_index"] = s.loc[df.index] # get right parcel_id for each geom_id
    df = df.dropna(subset=["new_index"])
    df = df.set_index("new_index", drop=True)

    d = {
        "hs": "type1",
        "ht": "type2",
        "hm": "type3",
        "of": "type4",
        "ho": "type5",
        "il": "type7",
        "iw": "type8",
        "ih": "type9",
        "rs": "type10",
        "rb": "type11",
        "mr": "type12",
        "mt": "type13",
        "me": "type14"
    }
    df.columns = [d.get(x, x) for x in df.columns]

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

    df = df.drop(['development_type_id', 'improvement_value', 'sqft_per_unit', 'nonres_rent_per_sqft', 'res_price_per_sqft', 'redfin_sale_price', 'redfin_home_type', 'costar_property_type', 'costar_rent'], axis=1)

    # set the vacancy rate in each building to 5% for testing purposes
    #vacancy = .25
    #df["residential_units"] = (households.building_id.value_counts() *
    #                           (1.0+vacancy)).apply(np.floor).astype('int')
    df["residential_units"] = df.residential_units.fillna(0)

    # we should only be using the "buildings" table during simulation, and in
    # simulation we want to normalize the prices to 2012 style prices
    df["redfin_sale_year"] = 2012
    return df


# this specifies the relationships between tables
sim.broadcast('parcels_geography', 'buildings', cast_index=True,
              onto_on='parcel_id')
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')

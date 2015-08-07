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
    df = pd.read_csv(os.path.join(misc.data_dir(), "2015_06_01_zoning_parcels.csv"),
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
                                                 '2015_06_01_parcels_to_zoning.csv'),
                                    low_memory=False)
    scenario_zoning = pd.read_excel(os.path.join(misc.data_dir(),
                                                 '2015_06_01_zoning_scenario_test.xls'),
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
    return pd.read_csv(os.path.join(misc.data_dir(), "2015_06_01_parcels_geography.csv"),
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


def households_building_id(residential_units, households):
    return misc.reindex(residential_units.building_id, households.unit_id)


@sim.table('residential_units', cache=True)
def residential_units(buildings, households):
    # in lieu of having a real units table in the base year, we're going to
    # build one from the buildings table.
    df = pd.DataFrame({
        "unit_residential_price": 0,
        # this is going to set all the units as degenerate "buildings" with one
        # unit each - in other words, every unit has one unit in it - duh
        "num_units": 1,
        # someone want to make this smarter? - right now no deed restriction
        # in the base year
        "deed_restricted": 0,
        # counter of the units in a building
        "unit_num": np.concatenate([np.arange(i) for i in \
                                    buildings.residential_units.values]),
        "building_id": np.repeat(buildings.index.values,
                                 buildings.residential_units.values)
    }).sort(columns=["building_id", "unit_num"]).reset_index(drop=True)
    # set a few units as randomly deed restricted (for testing)
    df.loc[np.random.choice(df.index, .03*len(df), replace=False),
           "deed_restricted"] = 1
    df.index.name = 'unit_id'

    # This is terribly, terrribly ugly - I don't want to explain every line,
    # but this converts from building_ids to unit_ids on the households
    # data frame.  I did this a somewhat prettier way with a for loop and it
    # was way too slow so I'm sticking with this for now.
    unit_lookup = df.reset_index().set_index(["building_id", "unit_num"])
    households = households.to_frame(households.local_columns)
    households = households.sort(columns=["building_id"], ascending=True)
    building_counts = households.building_id.value_counts().sort_index()
    households["unit_num"] = np.concatenate([np.arange(i) for i in \
                                             building_counts.values])
    unplaced = households[households.building_id == -1].index
    placed = households[households.building_id != -1].index
    indexes = [tuple(t) for t in \
               households.loc[placed, ["building_id", "unit_num"]].values]
    households.loc[placed, "unit_id"] = unit_lookup.loc[indexes].unit_id.values
    households.loc[unplaced, "unit_id"] = -1
    # this will only happen if there are overfull buildings at this point
    # actually there's this weird boundary case happening here - building_ids
    # that don't exist are filtered from households, then buildings with
    # invalid data are dropped (and some households are assigned to those
    # buildings) - this line protects against that and we can move on
    households["unit_id"] = households.unit_id.fillna(-1)
    households.drop(["unit_num", "building_id"], axis=1, inplace=True)
    sim.add_table("households", households)

    # now that building_id is dropped from households, we can add the
    # function to compute it from the relationship between households
    # and residential_units
    sim.add_column("households", "building_id", households_building_id)

    return df


@sim.column('residential_units', 'vacant_units')
def vacant_units(residential_units, households):
    return residential_units.num_units.sub(
        households.unit_id[households.unit_id != -1].value_counts(),
        fill_value=0)


@sim.column('residential_units', 'submarket_id')
def submarket_id(residential_units, buildings):
    return misc.reindex(buildings.zone_id, residential_units.building_id)


# this specifies the relationships between tables
sim.broadcast('parcels_geography', 'buildings', cast_index=True,
              onto_on='parcel_id')
sim.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
sim.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
sim.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
sim.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')
sim.broadcast('buildings', 'residential_units', cast_index=True,
              onto_on='building_id')

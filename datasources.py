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


@sim.table(cache=True)
def zoning_lookup():
     return pd.read_csv(os.path.join(misc.data_dir(), "zoning_lookup.csv"),
                     index_col="id")


# need to reindex from geom id to the id used on parcels
def geom_id_to_parcel_id(df, parcels):
    s = parcels.geom_id # get geom_id
    s = pd.Series(s.index, index=s.values) # invert series
    df["new_index"] = s.loc[df.index] # get right parcel_id for each geom_id
    df = df.dropna(subset=["new_index"])
    df = df.set_index("new_index", drop=True)
    df.index.name = "parcel_id"
    return df


# zoning for use in the "baseline" scenario
# comes in the hdf5
@sim.table('zoning_baseline', cache=True)
def zoning_baseline(parcels, zoning_lookup):
    df = pd.read_csv(os.path.join(misc.data_dir(), "zoning_parcels.csv"),
                     index_col="geom_id")

    df = pd.merge(df, zoning_lookup.to_frame(), left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    d = {
        "HS": "type1",
        "HT": "type2",
        "HM": "type3",
        "OF": "type4",
        "HO": "type5",
        "IL": "type7",
        "IW": "type8",
        "IH": "type9",
        "RS": "type10",
        "RB": "type11",
        "MR": "type12",
        "MT": "type13",
        "ME": "type14"
    }
    df.columns = [d.get(x, x) for x in df.columns]

    return df


@sim.table('zoning_np', cache=True)
def zoning_np(parcels):
    parcels_to_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                                 'parcel_tpp_pda_juris_expansion.csv'),
                                    low_memory=False)
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                                 'zoning_mods_np.csv'))
    df = pd.merge(parcels_to_zoning,
                  scenario_zoning,
                  on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                  how='left')
    df = df.set_index("geom_id")

    return geom_id_to_parcel_id(df, parcels)


# this is really bizarre, but the parcel table I have right now has empty
# zone_ids for a few parcels.  Not enough to worry about so just filling with
# the mode
@sim.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    df["zone_id"] = df.zone_id.replace(0, 1)

    net = sim.get_injectable('net')
    df['_node_id'] = net.get_node_ids(df['x'], df['y'])
    
    cfg = {
        "fill_nas": {
            "zone_id": {
                "how": "mode",
                "type": "int"
            },
            "_node_id": {
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

    return df


@sim.column('parcels', cache=True)
def pda(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), "pdas_parcels.csv"))
    # we have duplicate geom_ids, we think because a parcel can occur
    # in multiple pdas
    s = df.drop_duplicates(subset=["geom_id"]).set_index("geom_id").pda
    return misc.reindex(s, parcels.geom_id).reindex(parcels.index)


@sim.table('parcels_geography', cache=True)
def parcels_geography():
    return pd.read_csv(os.path.join(misc.data_dir(), "parcels_geography.csv"),
                       index_col="parcel_id")


@sim.table(cache=True)
def development_projects(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))

    for fld in ['residential_sqft', 'residential_price', 'non_residential_price']:
        df[fld] = np.nan
    df["redfin_sale_year"] = 2012 # hedonic doesn't tolerate nans
    df["stories"] = df.stories.fillna(1)
    df["building_sqft"] = df.building_sqft.fillna(0)
    df["non_residential_sqft"] = df.non_residential_sqft.fillna(0)

    df = df.dropna(subset=["geom_id"]) # need a geom_id to link to parcel_id

    df = df.dropna(subset=["year_built"]) # need a year built to get built

    df = df.set_index("geom_id")
    df = geom_id_to_parcel_id(df, parcels).reset_index() # use parcel id

    print "Describe of development projects"
    print df[sim.get_table('buildings').local_columns].describe()
    
    return df


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

import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca
from utils import geom_id_to_parcel_id, parcel_id_to_geom_id
from utils import nearest_neighbor


#####################
# TABLES AND INJECTABLES
#####################


@orca.injectable('year')
def year():
    try:
        return orca.get_injectable("iter_var")
    except:
        pass
    # if we're not running simulation, return base year
    return 2014


@orca.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@orca.table('locations', cache=True)
def locations():
    return pd.read_csv(os.path.join(misc.data_dir(), 'locations.csv'),
                       index_col="name")


@orca.table('jobs', cache=True)
def jobs(store):
    df = store['jobs']
    return df


# the estimation data is not in the buildings table - they are the same
@orca.table('homesales', cache=True)
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


# non-residential rent data
@orca.table('costar', cache=True)
def costar(store, parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), '2015_08_29_costar.csv'))
    df["PropertyType"] = df.PropertyType.replace("General Retail", "Retail")
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    df["costar_rent"] = df["Average Weighted Rent"].astype('float')
    df["year_built"] = df["Year Built"].fillna(1980)
    df = df.dropna(subset=["costar_rent", "Latitude", "Longitude"])

    # now assign parcel id
    df["parcel_id"] = nearest_neighbor(
        parcels.to_frame(['x', 'y']).dropna(subset=['x', 'y']),
        df[['Longitude', 'Latitude']]
    )

    return df


@orca.table(cache=True)
def zoning_lookup():
    return pd.read_csv(os.path.join(misc.data_dir(), "zoning_lookup.csv"),
                       index_col="id")


@orca.table('zcsv', cache=True)
def zcsv():
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2015_12_11_zoning_parcels.csv"),
                     index_col="geom_id")
    return df

@orca.table('zoning_table_city_lookup', cache=True)
def zoning_table_city_lookup():
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "zoning_table_city_lookup.csv"),
                     index_col="juris")
    return df

# zoning for use in the "baseline" scenario
# comes in the hdf5
@orca.table('zoning_baseline', cache=True)
def zoning_baseline(parcels, zoning_lookup):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2015_12_11_zoning_parcels.csv"),
                     index_col="geom_id")

    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    d = {
        "HS": "type1",
        "HT": "type2",
        "HM": "type3",
        "OF": "type4",
        "HO": "type5",
        "SC": "type6",
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


@orca.table('zoning_np', cache=True)
def zoning_np(parcels_geography):
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                               'zoning_mods_np.csv'),
                                  dtype={'jurisdiction': 'str'})
    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                    how='left').set_index('parcel_id')


@orca.table('zoning_th', cache=True)
def zoning_th(parcels_geography):
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                               'zoning_mods_th.csv'),
                                  dtype={'jurisdiction': 'str'})
    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                    how='left').set_index('parcel_id')


@orca.table('zoning_au', cache=True)
def zoning_au(parcels_geography):
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                               'zoning_mods_au.csv'),
                                  dtype={'jurisdiction': 'str'})
    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                    how='left').set_index('parcel_id')


@orca.table('zoning_pr', cache=True)
def zoning_pr(parcels_geography):
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                               'zoning_mods_pr.csv'),
                                  dtype={'jurisdiction': 'str'})
    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                    how='left').set_index('parcel_id')


# this is really bizarre, but the parcel table I have right now has empty
# zone_ids for a few parcels.  Not enough to worry about so just filling with
# the mode
@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    df["zone_id"] = df.zone_id.replace(0, 1)

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

    # have to do it this way because otherwise it's a circular reference
    sdem = pd.read_csv(os.path.join(misc.data_dir(),
                                    "development_projects.csv"))
    # mark parcels that are going to be developed by the sdem
    df["sdem"] = df.geom_id.isin(sdem.geom_id).astype('int')

    return df


@orca.table('parcels_zoning_calculations', cache=True)
def parcels_zoning_calculations(parcels):
    return pd.DataFrame(data=parcels.to_frame(
                        columns=['geom_id',
                                 'total_residential_units']),
                        index=parcels.index)


@orca.table('taz')
def taz(zones):
    return zones


@orca.table(cache=True)
def parcel_rejections():
    url = "https://forecast-feedback.firebaseio.com/parcelResults.json"
    return pd.read_json(url, orient="index").set_index("geomId")


@orca.table(cache=True)
def parcels_geography(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                                  "2015_10_07_2_parcels_geography.csv"),
                     index_col="geom_id", dtype={'jurisdiction': 'str'})
    return geom_id_to_parcel_id(df, parcels)


@orca.table(cache=True)
def development_events(parcels, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))
    df = df.query("action != 'build'")
    return df


@orca.table(cache=True)
def development_projects(parcels, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))

    df = df.query("action == 'build'")
    df = df.dropna(subset=['geom_id'])

    for fld in ['residential_sqft', 'residential_price',
                'non_residential_price']:
        df[fld] = 0
    df["redfin_sale_year"] = 2012  # hedonic doesn't tolerate nans
    df["stories"] = df.stories.fillna(1)
    df["building_sqft"] = df.building_sqft.fillna(0)
    df["non_residential_sqft"] = df.non_residential_sqft.fillna(0)

    df["building_type"] = df.building_type.replace("HP", "OF")
    df["building_type"] = df.building_type.replace("GV", "OF")
    df["building_type"] = df.building_type.replace("SC", "OF")
    df["building_type_id"] = \
        df.building_type.map(settings["building_type_map2"])

    df = df.dropna(subset=["geom_id"])  # need a geom_id to link to parcel_id

    df = df.dropna(subset=["year_built"])  # need a year built to get built

    df["geom_id"] = df.geom_id.astype("int")
    df = df.query('residential_units != "rent"')
    df["residential_units"] = df.residential_units.fillna(0).astype("int")
    geom_id = df.geom_id
    df = df.set_index("geom_id")
    df = geom_id_to_parcel_id(df, parcels).reset_index()  # use parcel id
    df["geom_id"] = geom_id  # add it back again cause it goes away above

    # we don't predict prices for schools and hotels right now
    df = df.query("building_type_id <= 4 or building_type_id >= 7")

    print "Describe of development projects"
    print df[orca.get_table('buildings').local_columns].describe()

    return df


@orca.table('households', cache=True)
def households(store, settings):
    # start with households from urbansim_defaults
    df = datasources.households(store, settings)

    # need to keep track of base year income quartiles for use in the
    # transition model - even caching doesn't work because when you add
    # rows via the transitioning, you automatically clear the cache!
    # this is pretty nasty and unfortunate
    df["base_income_quartile"] = pd.Series(pd.qcut(df.income, 4, labels=False),
                                           index=df.index).add(1)
    df["base_income_octile"] = pd.Series(pd.qcut(df.income, 8, labels=False),
                                         index=df.index).add(1)
    return df


@orca.table('buildings', cache=True)
def buildings(store, households, jobs, building_sqft_per_job, settings):
    # start with buildings from urbansim_defaults
    df = datasources.buildings(store, households, jobs,
                               building_sqft_per_job, settings)

    df = df.drop(['development_type_id', 'improvement_value',
                  'sqft_per_unit', 'nonres_rent_per_sqft',
                  'res_price_per_sqft', 'redfin_sale_price',
                  'redfin_home_type', 'costar_property_type',
                  'costar_rent'], axis=1)

    # set the vacancy rate in each building to 5% for testing purposes
    df["residential_units"] = df.residential_units.fillna(0)

    # for some reason nonres can be more than total sqft
    df["building_sqft"] = pd.DataFrame({
        "one": df.building_sqft,
        "two": df.residential_sqft + df.non_residential_sqft}).max(axis=1)

    # keeps parking lots from getting redeveloped
    df["building_sqft"][df.building_type_id.isin([15, 16])] = 0
    df["non_residential_sqft"][df.building_type_id.isin([15, 16])] = 0

    # don't know what a 0 building type id, set to office
    df["building_type_id"] = df.building_type_id.replace(0, 4)

    # we should only be using the "buildings" table during simulation, and in
    # simulation we want to normalize the prices to 2012 style prices
    df["redfin_sale_year"] = 2012

    return df


@orca.table('household_controls_unstacked', cache=True)
def household_controls_unstacked():
    df = pd.read_csv(os.path.join(misc.data_dir(), "household_controls.csv"))
    return df.set_index('year')


# the following overrides household_controls table defined in urbansim_defaults
@orca.table('household_controls', cache=True)
def household_controls(household_controls_unstacked):
    df = household_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['base_income_quartile', 'total_number_of_households']
    return df


@orca.table('employment_controls_unstacked', cache=True)
def employment_controls_unstacked():
    df = pd.read_csv(os.path.join(misc.data_dir(), "employment_controls.csv"))
    return df.set_index('year')


# the following overrides employment_controls
# table defined in urbansim_defaults
@orca.table('employment_controls', cache=True)
def employment_controls(employment_controls_unstacked):
    df = employment_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4, 5, 6]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['empsix_id', 'number_of_jobs']
    return df


@orca.table('zone_forecast_inputs', cache=True)
def zone_forecast_inputs():
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    "zone_forecast_inputs.csv"),
                       index_col="zone_id")


# this is the set of categories by zone of sending and receiving zones
# in terms of vmt fees
@orca.table("vmt_fee_categories", cache=True)
def vmt_fee_categories():
    return pd.read_csv(os.path.join(misc.data_dir(), "vmt_fee_zonecats.csv"),
                       index_col="taz")


@orca.table('taz_geography', cache=True)
def taz_geography():
    tg = pd.read_csv(os.path.join(misc.data_dir(),
                     "taz_geography.csv"), index_col="zone")
    sr = pd.read_csv(os.path.join(misc.data_dir(),
                     "subregions.csv"), index_col="sd")
    tg["subregion_id"] = sr.subregion.loc[tg.superdistrict].values
    tg["subregion"] = tg.subregion_id.map({
        1: "Core",
        2: "Urban",
        3: "Suburban",
        4: "Rural"
    })
    return tg


# these are shapes - "zones" in the bay area


@orca.table('zones', cache=True)
def zones(store):
    df = store['zones']
    df = df.sort_index()
    return df


# this specifies the relationships between tables
orca.broadcast('parcels_geography', 'buildings', cast_index=True,
               onto_on='parcel_id')
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('parcels', 'homesales', cast_index=True, onto_on='parcel_id')
orca.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
orca.broadcast('tmnodes', 'homesales', cast_index=True, onto_on='tmnode_id')
orca.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
orca.broadcast('tmnodes', 'costar', cast_index=True, onto_on='tmnode_id')
orca.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
orca.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True,
               onto_on='zone_id')

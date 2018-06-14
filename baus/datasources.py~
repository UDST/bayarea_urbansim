import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca
import preprocessing
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


@orca.injectable()
def initial_year():
    return 2010


@orca.injectable()
def final_year():
    return 2040


@orca.injectable(cache=True)
def store(settings):
    return pd.HDFStore(os.path.join(misc.data_dir(), settings["store"]))


@orca.injectable(cache=True)
def limits_settings(settings, scenario):
    # for limits, we inherit from the default
    # limits set the max number of job spaces or res units that may be
    # built per juris for each scenario - usually these represent actual
    # policies in place in each city which limit development

    d = settings['development_limits']

    if scenario in d.keys():
        print "Using limits for scenario: %s" % scenario
        assert "default" in d

        d_scen = d[scenario]
        d = d["default"]
        for key, value in d_scen.iteritems():
            d.setdefault(key, {})
            d[key].update(value)

        return d

    print "Using default limits"
    return d["default"]


@orca.injectable(cache=True)
def inclusionary_housing_settings(settings, scenario):
    # for inclustionary housing, each scenario is different
    # there is no inheritance

    s = settings['inclusionary_housing_settings']

    if scenario in s.keys():
        print "Using inclusionary settings for scenario: %s" % scenario
        s = s[scenario]

    elif "default" in s.keys():
        print "Using default inclusionary settings"
        s = s["default"]

    d = {}
    for item in s:
        # this is a list of cities with an inclusionary rate that is the
        # same for all the cities in the list
        print "Setting inclusionary rates for %d cities to %.2f" %\
            (len(item["values"]), item["amount"])
        # this is a list of inclusionary rates and the cities they apply
        # to - need tro turn it in a map of city names to rates
        for juris in item["values"]:
            d[juris] = item["amount"]

    return d


@orca.injectable(cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@orca.step()
def fetch_from_s3(settings):
    import boto
    # fetch files from s3 based on config in settings.yaml
    s3_settings = settings["s3_settings"]

    conn = boto.connect_s3()
    bucket = conn.get_bucket(s3_settings["bucket"], validate=False)

    for file in s3_settings["files"]:
        file = os.path.join("data", file)
        if os.path.exists(file):
            continue
        print "Downloading " + file
        key = bucket.get_key(file, validate=False)
        key.get_contents_to_filename(file)


# key locations in the Bay Area for use as attractions in the models
@orca.table(cache=True)
def landmarks():
    return pd.read_csv(os.path.join(misc.data_dir(), 'landmarks.csv'),
                       index_col="name")


@orca.table(cache=True)
def baseyear_taz_controls():
    return pd.read_csv(os.path.join("data",
                       "baseyear_taz_controls.csv"), index_col="taz1454")


@orca.table(cache=True)
def base_year_summary_taz():
    return pd.read_csv(os.path.join('output',
                       'baseyear_taz_summaries_2010.csv'),
                       index_col="zone_id")


# non-residential rent data
@orca.table(cache=True)
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
                       index_col='id')


# zoning for use in the "baseline" scenario
@orca.table(cache=True)
def zoning_baseline(parcels, zoning_lookup, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2015_12_21_zoning_parcels.csv"),
                     index_col="geom_id")
    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    return df


@orca.table(cache=True)
def new_tpp_id():
    return pd.read_csv(os.path.join(misc.data_dir(), "tpp_id_2016.zip"),
                       index_col="parcel_id")


@orca.table(cache=True)
def zoning_scenario(parcels_geography, scenario, settings):
    scenario_zoning = pd.read_csv(
        os.path.join(misc.data_dir(), 'zoning_mods_%s.csv' % scenario))

    for k in settings["building_type_map"].keys():
        scenario_zoning[k] = np.nan

    def add_drop_helper(col, val):
        for ind, item in scenario_zoning[col].iteritems():
            if not isinstance(item, str):
                continue
            for btype in item.split():
                scenario_zoning.loc[ind, btype] = val

    add_drop_helper("add_bldg", 1)
    add_drop_helper("drop_bldg", 0)

    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['zoningmodcat'],
                    how='left').set_index('parcel_id')


@orca.table(cache=True)
def parcels(store):
    return store['parcels']


@orca.table(cache=True)
def parcels_zoning_calculations(parcels):
    return pd.DataFrame(index=parcels.index)


@orca.table()
def taz(zones):
    return zones


@orca.table(cache=True)
def parcel_rejections():
    url = "https://forecast-feedback.firebaseio.com/parcelResults.json"
    return pd.read_json(url, orient="index").set_index("geomId")


@orca.table(cache=True)
def parcels_geography(parcels):
    df = pd.read_csv(
        os.path.join(misc.data_dir(), "02_01_2016_parcels_geography.csv"),
        index_col="geom_id")
    df = geom_id_to_parcel_id(df, parcels)

    # this will be used to map juris id to name
    juris_name = pd.read_csv(
        os.path.join(misc.data_dir(), "census_id_to_name.csv"),
        index_col="census_id").name10

    df["juris_name"] = df.jurisdiction_id.map(juris_name)

    df.loc[2054504, "juris_name"] = "Marin County"
    df.loc[2054505, "juris_name"] = "Santa Clara County"
    df.loc[2054506, "juris_name"] = "Marin County"
    df.loc[572927, "juris_name"] = "Contra Costa County"
    # assert no empty juris values
    assert True not in df.juris_name.isnull().value_counts()

    df["pda_id"] = df.pda_id.str.lower()

    # danville wasn't supposed to be a pda
    df["pda_id"] = df.pda_id.replace("dan1", np.nan)

    return df


@orca.table(cache=True)
def manual_edits():
    return pd.read_csv(os.path.join(misc.data_dir(), "manual_edits.csv"))


def reprocess_dev_projects(df):
    # if dev projects with the same parcel id have more than one build
    # record, we change the later ones to add records - we don't want to
    # constantly be redeveloping projects, but it's a common error for users
    # to make in their development project configuration
    df = df.sort_values(["geom_id", "year_built"])
    prev_geom_id = None
    for index, rec in df.iterrows():
        if rec.geom_id == prev_geom_id:
            df.loc[index, "action"] = "add"
        prev_geom_id = rec.geom_id

    return df


# shared between demolish and build tables below
def get_dev_projects_table(scenario, parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))
    df = reprocess_dev_projects(df)

    # this filters project by scenario
    if scenario in df:
        # df[scenario] is 1s and 0s indicating whether to include it
        df = df[df[scenario].astype('bool')]

    df = df.dropna(subset=['geom_id'])

    cnts = df.geom_id.isin(parcels.geom_id).value_counts()
    if False in cnts.index:
        print "%d MISSING GEOMIDS!" % cnts.loc[False]

    df = df[df.geom_id.isin(parcels.geom_id)]

    geom_id = df.geom_id  # save for later
    df = df.set_index("geom_id")
    df = geom_id_to_parcel_id(df, parcels).reset_index()  # use parcel id
    df["geom_id"] = geom_id.values  # add it back again cause it goes away

    return df


@orca.table(cache=True)
def demolish_events(parcels, settings, scenario):
    df = get_dev_projects_table(scenario, parcels)

    # keep demolish and build records
    return df[df.action.isin(["demolish", "build"])]


@orca.table(cache=True)
def development_projects(parcels, settings, scenario):
    df = get_dev_projects_table(scenario, parcels)

    for col in [
            'residential_sqft', 'residential_price', 'non_residential_rent']:
        df[col] = 0
    df["redfin_sale_year"] = 2012  # default base year
    df["redfin_sale_price"] = np.nan  # null sales price
    df["stories"] = df.stories.fillna(1)
    df["building_sqft"] = df.building_sqft.fillna(0)
    df["non_residential_sqft"] = df.non_residential_sqft.fillna(0)
    df["residential_units"] = df.residential_units.fillna(0).astype("int")
    df["deed_restricted_units"] = 0

    df["building_type"] = df.building_type.replace("HP", "OF")
    df["building_type"] = df.building_type.replace("GV", "OF")
    df["building_type"] = df.building_type.replace("SC", "OF")

    building_types = settings["building_type_map"].keys()
    # only deal with building types we recorgnize
    # otherwise hedonics break
    df = df[df.building_type.isin(building_types)]

    # we don't predict prices for schools and hotels right now
    df = df[~df.building_type.isin(["SC", "HO"])]

    # need a year built to get built
    df = df.dropna(subset=["year_built"])
    df = df[df.action.isin(["add", "build"])]

    print "Describe of development projects"
    # this makes sure dev projects has all the same columns as buildings
    # which is the point of this method
    print df[orca.get_table('buildings').local_columns].describe()

    return df


def print_error_if_not_available(store, table):
    if table not in store:
        raise Exception(
            "%s not found in store - you need to preprocess" % table +
            " the data with:\n  python baus.py --mode preprocessing -c")
    return store[table]


@orca.table(cache=True)
def jobs(store):
    return print_error_if_not_available(store, 'jobs_preproc')


@orca.table(cache=True)
def households(store):
    return print_error_if_not_available(store, 'households_preproc')


@orca.table(cache=True)
def buildings(store):
    return print_error_if_not_available(store, 'buildings_preproc')


@orca.table(cache=True)
def residential_units(store):
    return print_error_if_not_available(store, 'residential_units_preproc')


@orca.table(cache=True)
def household_controls_unstacked():
    return pd.read_csv(os.path.join(misc.data_dir(), "household_controls.csv"),
                       index_col='year')


# the following overrides household_controls
# table defined in urbansim_defaults
@orca.table(cache=True)
def household_controls(household_controls_unstacked):
    df = household_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['base_income_quartile', 'total_number_of_households']
    return df


@orca.table(cache=True)
def employment_controls_unstacked():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "employment_controls.csv"),
        index_col='year')


# the following overrides employment_controls
# table defined in urbansim_defaults
@orca.table(cache=True)
def employment_controls(employment_controls_unstacked):
    df = employment_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4, 5, 6]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['empsix_id', 'number_of_jobs']
    return df


@orca.table(cache=True)
def zone_forecast_inputs():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "zone_forecast_inputs.csv"),
        index_col="zone_id")


# this is the set of categories by zone of sending and receiving zones
# in terms of vmt fees
@orca.table(cache=True)
def vmt_fee_categories():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "vmt_fee_zonecats.csv"),
        index_col="taz")


@orca.table(cache=True)
def superdistricts():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "superdistricts.csv"),
        index_col="number")


@orca.table(cache=True)
def abag_targets():
    return pd.read_csv(os.path.join(misc.data_dir(), "abag_targets.csv"))


@orca.table(cache=True)
def taz_geography(superdistricts):
    tg = pd.read_csv(
        os.path.join(misc.data_dir(), "taz_geography.csv"),
        index_col="zone")

    # we want "subregion" geography on the taz_geography table
    # we have to go get it from the superdistricts table and join
    # using the superdistrcit id
    tg["subregion_id"] = \
        superdistricts.subregion.loc[tg.superdistrict].values
    tg["subregion"] = tg.subregion_id.map({
        1: "Core",
        2: "Urban",
        3: "Suburban",
        4: "Rural"
    })
    return tg


# these are shapes - "zones" in the bay area
@orca.table(cache=True)
def zones(store):
    # sort index so it prints out nicely when we want it to
    return store['zones'].sort_index()


# this specifies the relationships between tables
orca.broadcast('buildings', 'residential_units', cast_index=True,
               onto_on='building_id')
orca.broadcast('residential_units', 'households', cast_index=True,
               onto_on='unit_id')
orca.broadcast('parcels_geography', 'buildings', cast_index=True,
               onto_on='parcel_id')
# not defined in urbansim_Defaults
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True,
               onto_on='zone_id')

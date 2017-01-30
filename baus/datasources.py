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
def recompute_cached_tables():
    # we cache the four major tables - parcels, buildngs, jobs, and households
    # in order to save computation time on reading tables - set this to true
    # in order to recompute all the cached tables
    return False


@orca.injectable()
def initial_year():
    return 2010


@orca.injectable()
def final_year():
    return 2040


@orca.injectable(cache=True)
def store(settings):
    return pd.HDFStore(os.path.join(misc.data_dir(), settings["store"]))


# this is the income split for the low income and regular HLCMs
@orca.injectable()
def low_income(settings):
    return int(settings["low_income_for_hlcm"])


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
        # to - need to turn it in a map of city names to rates
        for juris in item["values"]:
            d[juris] = item["amount"]

    return d


@orca.injectable(cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


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
    df = pd.read_csv(os.path.join(misc.data_dir(), "zoning_lookup.csv"),
                     index_col='id')


# zoning for use in the "baseline" scenario
# comes in the hdf5
@orca.table(cache=True)
def zoning_baseline(parcels, zoning_lookup, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2015_12_21_zoning_parcels.csv"),
                     index_col="geom_id")
    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    # change column names
    d = {k: "type%d" % v for k, v in settings["building_type_map2"].items()}
    df.columns = [d.get(x, x) for x in df.columns]

    return df


@orca.table(cache=True)
def new_tpp_id():
    return pd.read_csv(os.path.join(misc.data_dir(), "tpp_id_2016.zip"),
                       index_col="parcel_id")


@orca.table(cache=True)
def zoning_scenario(parcels_geography, scenario, settings):

    scenario_zoning = pd.read_csv(
        os.path.join(misc.data_dir(),
                     'zoning_mods_%s.csv' % scenario),
        dtype={'jurisdiction': 'str'})

    d = {k: "type%d" % v for k, v in settings["building_type_map2"].items()}

    for k, v in d.items():
        scenario_zoning['add-'+v] = scenario_zoning.add_bldg.str.contains(k)

    for k, v in d.items():
        scenario_zoning['drop-'+v] = scenario_zoning.drop_bldg.\
            astype(str).str.contains(k)

    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=['zoningmodcat'],
                    how='left').set_index('parcel_id')


@orca.table(cache=True)
def parcels(store):
    return store['parcels']


@orca.table(cache=True)
def parcels_zoning_calculations(parcels):
    return pd.DataFrame(data=parcels.to_frame(
                        columns=['geom_id',
                                 'total_residential_units']),
                        index=parcels.index)


@orca.table()
def taz(zones):
    return zones


@orca.table(cache=True)
def parcel_rejections():
    url = "https://forecast-feedback.firebaseio.com/parcelResults.json"
    return pd.read_json(url, orient="index").set_index("geomId")


@orca.table(cache=True)
def parcels_geography(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                                  "02_01_2016_parcels_geography.csv"),
                     index_col="geom_id", dtype={'jurisdiction': 'str'})
    df = geom_id_to_parcel_id(df, parcels)

    juris_name = pd.read_csv(os.path.join(misc.data_dir(),
                                          "census_id_to_name.csv"),
                             index_col="census_id").name10

    df["juris_name"] = df.jurisdiction_id.map(juris_name)

    df["pda_id"] = df.pda_id.str.lower()

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
    df = df.sort(["geom_id", "year_built"])
    prev_geom_id = None
    for index, rec in df.iterrows():
        if rec.geom_id == prev_geom_id:
            df.loc[index, "action"] = "add"
        prev_geom_id = rec.geom_id

    return df


@orca.table(cache=True)
def demolish_events(parcels, settings, scenario):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))
    df = reprocess_dev_projects(df)

    # this filters project by scenario
    if scenario in df:
        # df[scenario] is 1s and 0s indicating whether to include it
        df = df[df[scenario].astype('bool')]

    # keep demolish and build records
    df = df[df.action.isin(["demolish", "build"])]

    df = df.dropna(subset=['geom_id'])
    df = df.set_index("geom_id")
    df = geom_id_to_parcel_id(df, parcels).reset_index()  # use parcel id

    return df


@orca.table(cache=True)
def development_projects(parcels, settings, scenario):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))
    df = reprocess_dev_projects(df)

    df = df[df.action.isin(["add", "build"])]

    # this filters project by scenario
    colname = "scen%s" % scenario
    # df[colname] is 1s and 0s indicating whether to include it
    # this used to be an optional filter but now I'm going to require it so
    # that we don't accidentally include all the development projects since
    # we've started using scenario-based dev projects pretty extensively
    df = df[df[colname].astype('bool')]

    df = df.dropna(subset=['geom_id'])

    for fld in ['residential_sqft', 'residential_price',
                'non_residential_price']:
        df[fld] = 0
    df["redfin_sale_year"] = 2012  # default base year
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
    df["geom_id"] = geom_id.values  # add it back again cause it goes away

    # we don't predict prices for schools and hotels right now
    df = df.query("building_type_id <= 4 or building_type_id >= 7")

    df["deed_restricted_units"] = 0

    print "Describe of development projects"
    print df[orca.get_table('buildings').local_columns].describe()

    return df


@orca.table(cache=True)
def jobs(store):
    return store['jobs_preproc']


@orca.table(cache=True)
def households(store):
    return store['households_preproc']


@orca.table(cache=True)
def buildings(store):
    return store['buildings_preproc']


@orca.table(cache=True)
def household_controls_unstacked():
    df = pd.read_csv(os.path.join(misc.data_dir(), "household_controls.csv"))
    return df.set_index('year')


# the following overrides household_controls table defined in urbansim_defaults
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
    df = pd.read_csv(os.path.join(misc.data_dir(), "employment_controls.csv"))
    return df.set_index('year')


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
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    "zone_forecast_inputs.csv"),
                       index_col="zone_id")


# this is the set of categories by zone of sending and receiving zones
# in terms of vmt fees
@orca.table(cache=True)
def vmt_fee_categories():
    return pd.read_csv(os.path.join(misc.data_dir(), "vmt_fee_zonecats.csv"),
                       index_col="taz")


@orca.table(cache=True)
def superdistricts():
    return pd.read_csv(os.path.join(misc.data_dir(),
                       "superdistricts.csv"), index_col="number")


@orca.table(cache=True)
def abag_targets():
    return pd.read_csv(os.path.join(misc.data_dir(), "abag_targets.csv"))


@orca.table(cache=True)
def taz_geography(superdistricts):
    tg = pd.read_csv(os.path.join(misc.data_dir(),
                     "taz_geography.csv"), index_col="zone")
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
    return store['zones'].sort_index()


# this specifies the relationships between tables
orca.broadcast('parcels_geography', 'buildings', cast_index=True,
               onto_on='parcel_id')
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
orca.broadcast('tmnodes', 'costar', cast_index=True, onto_on='tmnode_id')
orca.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True,
               onto_on='zone_id')

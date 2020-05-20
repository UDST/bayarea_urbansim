from __future__ import print_function

import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca
from baus import preprocessing
from baus.utils import geom_id_to_parcel_id, parcel_id_to_geom_id
from baus.utils import nearest_neighbor
import yaml


#####################
# TABLES AND INJECTABLES
#####################


# define new settings files- these have been subdivided from the
# general settings file
# this is similar to the code for settings in urbansim_defaults
@orca.injectable('hazards', cache=True)
def hazards():
    with open(os.path.join(misc.configs_dir(), "hazards.yaml")) as f:
        return yaml.load(f)


@orca.injectable('policy', cache=True)
def policy():
    with open(os.path.join(misc.configs_dir(), "policy.yaml")) as f:
        return yaml.load(f)


@orca.injectable('inputs', cache=True)
def inputs():

    with open(os.path.join(misc.configs_dir(), "inputs.yaml")) as f:
        return yaml.load(f)


@orca.injectable('mapping', cache=True)
def mapping():
    with open(os.path.join(misc.configs_dir(), "mapping.yaml")) as f:
        return yaml.load(f)


# now that there are new settings files, override the locations of certain
# settings already defined in urbansim_defaults
@orca.injectable("building_type_map")
def building_type_map(mapping):
    return mapping["building_type_map"]


@orca.injectable('year')
def year():
    try:
        return orca.get_injectable("iter_var")
    except Exception as e:
        pass
    # if we're not running simulation, return base year
    return 2014


@orca.injectable()
def initial_year():
    return 2010


@orca.injectable()
def final_year():
    return 2050


@orca.injectable(cache=True)
def store(settings):
    return pd.HDFStore(os.path.join(misc.data_dir(), settings["store"]))


@orca.injectable(cache=True)
def limits_settings(policy, scenario):
    # for limits, we inherit from the default
    # limits set the max number of job spaces or res units that may be
    # built per juris for each scenario - usually these represent actual
    # policies in place in each city which limit development

    # set up so that fr2 limits can be turned off as needed
    # instead of looking for fr2 limits, the fr1 scenario is used
    if (scenario in ["11", "12", "15"]) and\
       (scenario not in policy["office_caps_fr2_enable"]):
        scenario = str(int(scenario) - 10)

    d = policy['development_limits']

    if scenario in d.keys():
        print("Using limits for scenario: %s" % scenario)
        assert "default" in d

        d_scen = d[scenario]
        d = d["default"]
        for key, value in d_scen.items():
            d.setdefault(key, {})
            d[key].update(value)

        return d

    print("Using default limits")
    return d["default"]


@orca.injectable(cache=True)
def inclusionary_housing_settings(policy, scenario):
    # for inclustionary housing, each scenario is different
    # there is no inheritance

    s = policy['inclusionary_housing_settings']

    if (scenario in ["11", "12", "15"]) and\
       (scenario not in policy["inclusionary_fr2_enable"]):
        print("Using Futures Round 1 (PBA40) inclusionary settings")
        fr1 = str(int(scenario) - 10)
        s = s[fr1]

    elif scenario in s.keys():
        print("Using inclusionary settings for scenario: %s" % scenario)
        s = s[scenario]

    elif "default" in s.keys():
        print("Using default inclusionary settings")
        s = s["default"]

    d = {}
    for item in s:
        # this is a list of cities with an inclusionary rate that is the
        # same for all the cities in the list
        print("Setting inclusionary rates for %d cities to %.2f" %
              (len(item["values"]), item["amount"]))
        # this is a list of inclusionary rates and the cities they apply
        # to - need tro turn it in a map of city names to rates
        for juris in item["values"]:
            d[juris] = item["amount"]

    return d


@orca.injectable(cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@orca.injectable(cache=True)
def elcm_config():
    return get_config_file('elcm')


@orca.injectable(cache=True)
def hlcm_owner_config():
    return get_config_file('hlcm_owner')


@orca.injectable(cache=True)
def hlcm_owner_no_unplaced_config():
    return get_config_file('hlcm_owner_no_unplaced')


@orca.injectable(cache=True)
def hlcm_owner_lowincome_config():
    return get_config_file('hlcm_owner_lowincome')


@orca.injectable(cache=True)
def hlcm_renter_config():
    return get_config_file('hlcm_renter')


@orca.injectable(cache=True)
def hlcm_renter_no_unplaced_config():
    return get_config_file('hlcm_renter_no_unplaced')


@orca.injectable(cache=True)
def hlcm_renter_lowincome_config():
    return get_config_file('hlcm_renter_lowincome')


@orca.injectable(cache=True)
def rsh_config():
    fname = get_config_file('rsh')
    orca.add_injectable("rsh_file", fname)
    return get_config_file('rsh')


@orca.injectable(cache=True)
def rrh_config():
    return get_config_file('rrh')


@orca.injectable(cache=True)
def nrh_config():
    return get_config_file('nrh')


def get_config_file(type):
    configs = orca.get_injectable('inputs')['model_configs'][type.
                                                             split('_')[0]]
    sc = orca.get_injectable('scenario')
    sc_cfg = 's{}_{}_config'.format(sc, type)
    gen_cfg = '{}_config'.format(type)
    if sc_cfg in configs:
        return configs[sc_cfg]
    elif gen_cfg in configs:
        return configs[gen_cfg]
    else:
        return '{}.yaml'.format(type)


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
        print("Downloading " + file)
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
                     "2020_04_15_zoning_parcels.csv"),
                     index_col="geom_id")
    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    return df


@orca.table(cache=True)
def new_tpp_id():
    return pd.read_csv(os.path.join(misc.data_dir(), "tpp_id_2016.csv"),
                       index_col="parcel_id")


@orca.table(cache=True)
def maz():
    maz = pd.read_csv(os.path.join(misc.data_dir(), "maz_geography.csv"))
    maz = maz.drop_duplicates('MAZ').set_index('MAZ')
    taz1454 = pd.read_csv(os.path.join(misc.data_dir(), "maz22_taz1454.csv"),
                          index_col='maz')
    maz['taz1454'] = taz1454.TAZ1454
    return maz


@orca.table(cache=True)
def parcel_to_maz():
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    "2018_05_23_parcel_to_maz22.csv"),
                       index_col="PARCEL_ID")


@orca.table(cache=True)
def county_forecast_inputs():
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    "county_forecast_inputs.csv"),
                       index_col="COUNTY")


@orca.table(cache=True)
def county_employment_forecast():
    return pd.read_csv(os.path.join(misc.data_dir(),
                       "county_employment_forecast.csv"))


@orca.table(cache=True)
def taz2_forecast_inputs(regional_demographic_forecast):
    t2fi = pd.read_csv(os.path.join(misc.data_dir(),
                                    "taz2_forecast_inputs.csv"),
                       index_col='TAZ').replace('#DIV/0!', np.nan)

    rdf = regional_demographic_forecast.to_frame()
    # apply regional share of hh by size to MAZs with no households in 2010
    t2fi.loc[t2fi.shrw0_2010.isnull(),
             'shrw0_2010'] = rdf.loc[rdf.year == 2010, 'shrw0'].values[0]
    t2fi.loc[t2fi.shrw1_2010.isnull(),
             'shrw1_2010'] = rdf.loc[rdf.year == 2010, 'shrw1'].values[0]
    t2fi.loc[t2fi.shrw2_2010.isnull(),
             'shrw2_2010'] = rdf.loc[rdf.year == 2010, 'shrw2'].values[0]
    t2fi.loc[t2fi.shrw3_2010.isnull(),
             'shrw3_2010'] = rdf.loc[rdf.year == 2010, 'shrw3'].values[0]

    # apply regional share of persons by age category
    t2fi.loc[t2fi.shra1_2010.isnull(),
             'shra1_2010'] = rdf.loc[rdf.year == 2010, 'shra1'].values[0]
    t2fi.loc[t2fi.shra2_2010.isnull(),
             'shra2_2010'] = rdf.loc[rdf.year == 2010, 'shra2'].values[0]
    t2fi.loc[t2fi.shra3_2010.isnull(),
             'shra3_2010'] = rdf.loc[rdf.year == 2010, 'shra3'].values[0]
    t2fi.loc[t2fi.shra4_2010.isnull(),
             'shra4_2010'] = rdf.loc[rdf.year == 2010, 'shra4'].values[0]

    # apply regional share of hh by presence of children
    t2fi.loc[t2fi.shrn_2010.isnull(),
             'shrn_2010'] = rdf.loc[rdf.year == 2010, 'shrn'].values[0]
    t2fi.loc[t2fi.shry_2010.isnull(),
             'shry_2010'] = rdf.loc[rdf.year == 2010, 'shry'].values[0]

    t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010', 'shrw3_2010',
          'shra1_2010', 'shra2_2010', 'shra3_2010', 'shra4_2010', 'shrn_2010',
          'shry_2010']] = t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010',
                                'shrw3_2010', 'shra1_2010', 'shra2_2010',
                                'shra3_2010', 'shra4_2010', 'shrn_2010',
                                'shry_2010']].astype('float')
    return t2fi


@orca.table(cache=True)
def empsh_to_empsix():
    return pd.read_csv(os.path.join(misc.data_dir(), "empsh_to_empsix.csv"))


@orca.table(cache=True)
def maz_forecast_inputs(regional_demographic_forecast):
    rdf = regional_demographic_forecast.to_frame()
    mfi = pd.read_csv(os.path.join(misc.data_dir(),
                                   "maz_forecast_inputs.csv"),
                      index_col='MAZ').replace('#DIV/0!', np.nan)

    # apply regional share of hh by size to MAZs with no households in 2010
    mfi.loc[mfi.shrs1_2010.isnull(),
            'shrs1_2010'] = rdf.loc[rdf.year == 2010, 'shrs1'].values[0]
    mfi.loc[mfi.shrs2_2010.isnull(),
            'shrs2_2010'] = rdf.loc[rdf.year == 2010, 'shrs2'].values[0]
    mfi.loc[mfi.shrs3_2010.isnull(),
            'shrs3_2010'] = rdf.loc[rdf.year == 2010, 'shrs3'].values[0]
    # the fourth category here is missing the 'r' in the csv
    mfi.loc[mfi.shs4_2010.isnull(),
            'shs4_2010'] = rdf.loc[rdf.year == 2010, 'shrs4'].values[0]
    mfi[['shrs1_2010', 'shrs2_2010', 'shrs3_2010',
         'shs4_2010']] = mfi[['shrs1_2010', 'shrs2_2010',
                              'shrs3_2010', 'shs4_2010']].astype('float')
    return mfi


@orca.table(cache=True)
def zoning_scenario(parcels_geography, scenario, policy, mapping):

    if (scenario in ["11", "12", "15"]) and\
       (scenario not in policy["geographies_fr2_enable"]):
        scenario = str(int(scenario) - 10)

    scenario_zoning = pd.read_csv(
        os.path.join(misc.data_dir(), 'zoning_mods_%s.csv' % scenario))

    for k in mapping["building_type_map"].keys():
        scenario_zoning[k] = np.nan

    def add_drop_helper(col, val):
        for ind, item in scenario_zoning[col].items():
            if not isinstance(item, str):
                continue
            for btype in item.split():
                scenario_zoning.loc[ind, btype] = val

    add_drop_helper("add_bldg", 1)
    add_drop_helper("drop_bldg", 0)

    if 'pba50zoningmodcat' in scenario_zoning.columns:
        join_col = 'pba50zoningmodcat'
    elif 'zoninghzcat' in scenario_zoning.columns:
        join_col = 'zoninghzcat'
    else:
        join_col = 'zoningmodcat'

    return pd.merge(parcels_geography.to_frame().reset_index(),
                    scenario_zoning,
                    on=join_col,
                    how='left').set_index('parcel_id')


@orca.table(cache=True)
def parcels(store):
    df = store['parcels']
    return df.loc[df.x.notnull()]


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
def parcels_geography(parcels, scenario, settings):
    df = pd.read_csv(
        os.path.join(misc.data_dir(), "2020_04_17_parcels_geography.csv"),
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

    df['juris_trich'] = df.juris_id + df.trich_id

    df["pda_id"] = df.pda_id.str.lower()

    # danville wasn't supposed to be a pda
    df["pda_id"] = df.pda_id.replace("dan1", np.nan)

    return df


@orca.table(cache=True)
def parcels_subzone():
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    '2018_10_17_parcel_to_taz1454sub.csv'),
                       usecols=['taz_sub', 'PARCEL_ID'],
                       index_col='PARCEL_ID')


@orca.table(cache=False)
def mandatory_accessibility():
    fname = get_logsum_file('mandatory')
    orca.add_injectable("mand_acc_file_2010", fname)
    df = pd.read_csv(os.path.join(
        misc.data_dir(), fname))
    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone
    return df.set_index('taz_sub')


@orca.table(cache=False)
def non_mandatory_accessibility():
    fname = get_logsum_file('non_mandatory')
    orca.add_injectable("nonmand_acc_file_2010", fname)
    df = pd.read_csv(os.path.join(
        misc.data_dir(), fname))
    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone
    return df.set_index('taz_sub')


@orca.table(cache=False)
def accessibilities_segmentation():
    fname = get_logsum_file('segmentation')
    orca.add_injectable("acc_seg_file_2010", fname)
    df = pd.read_csv(os.path.join(
        misc.data_dir(), fname))
    df['AV'] = df['hasAV'].apply(lambda x: 'AV' if x == 1 else 'noAV')
    df['label'] = (df['incQ_label'] + '_' + df['autoSuff_label'] +
                   '_' + df['AV'])
    df = df.groupby('label').sum()
    df['prop'] = df['num_persons'] / df['num_persons'].sum()
    df = df[['prop']].transpose().reset_index(drop=True)
    return df


def get_logsum_file(type='mandatory'):
    logsums = orca.get_injectable('inputs')['logsums'][type]
    sc = orca.get_injectable('scenario')
    yr = orca.get_injectable('year')
    try:
        prev_type = orca.get_injectable('previous_{}_logsum_type'.format(type))
        if prev_type == 'generic':
            return orca.get_injectable('previous_{}_logsum_file'.format(type))
        elif prev_type == 'year':
            if 'logsum_{}'.format(yr) in logsums:
                ls = logsums['logsum_{}'.format(yr)]
                orca.add_injectable('previous_{}_logsum_file'.format(type), ls)
                return ls
            else:
                return orca.get_injectable('previous_{}_logsum_file'
                                           .format(type))
        elif prev_type == 'scenario':
            if 'logsum_s{}'.format(sc) in logsums:
                ls = logsums['logsum_s{}'.format(sc)]
                orca.add_injectable('previous_{}_logsum_file'
                                    .format(type), ls)
                return ls
            else:
                return orca.get_injectable('previous_{}_logsum_file'
                                           .format(type))
        else:
            if 'logsum_{}_s{}'.format(yr, sc) in logsums:
                ls = logsums['logsum_{}_s{}'.format(yr, sc)]
                orca.add_injectable('previous_{}_logsum_file'
                                    .format(type), ls)
                return ls
            else:
                return orca.get_injectable('previous_{}_logsum_file'
                                           .format(type))
    except Exception as e:
        if 'logsum' in logsums:
            ls = logsums['logsum']
            ls_type = 'generic'
        if 'logsum_{}'.format(yr) in logsums:
            ls = logsums['logsum_{}'.format(yr)]
            ls_type = 'year'
        if 'logsum_s{}'.format(sc) in logsums:
            ls = logsums['logsum_s{}'.format(sc)]
            ls_type = 'scenario'
        if 'logsum_{}_s{}'.format(yr, sc) in logsums:
            ls = logsums['logsum_{}_s{}'.format(yr, sc)]
            ls_type = 'year_scenario'
        orca.add_injectable('previous_{}_logsum_type'.format(type),
                            ls_type)
        orca.add_injectable('previous_{}_logsum_file'.format(type),
                            ls)
        return ls


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
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2020_05_20_development_projects.csv"))
    df = reprocess_dev_projects(df)

    # this filters project by scenario
    scen = 'scen' + str(scenario)
    if scen in df:
        # df[scenario] is 1s and 0s indicating whether to include it
        df = df[df[scen].astype('bool')]

    df = df.dropna(subset=['geom_id'])

    cnts = df.geom_id.isin(parcels.geom_id).value_counts()
    if False in cnts.index:
        print("%d MISSING GEOMIDS!" % cnts.loc[False])

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
def development_projects(parcels, mapping, scenario):
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

    building_types = mapping["building_type_map"].keys()
    # only deal with building types we recorgnize
    # otherwise hedonics break
    df = df[df.building_type.isin(building_types)]

    # we don't predict prices for schools and hotels right now
    df = df[~df.building_type.isin(["SC", "HO"])]

    # need a year built to get built
    df = df.dropna(subset=["year_built"])
    df = df[df.action.isin(["add", "build"])]

    print("Describe of development projects")
    # this makes sure dev projects has all the same columns as buildings
    # which is the point of this method
    print(df[orca.get_table('buildings').local_columns].describe())

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
    fname = get_control_file(type='household')
    orca.add_injectable("household_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname),
                       index_col='year')


@orca.table(cache=True)
def regional_demographic_forecast():
    fname = get_control_file(type='demographic_forecast')
    orca.add_injectable("reg_dem_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname))


def get_control_file(type):
    controls = orca.get_injectable('inputs')['control_tables'][type]
    sc = orca.get_injectable('scenario')
    sc_file = 's{}_{}_controls_input_file'.format(sc, type)
    gen_file = '{}_controls_input_file'.format(type)
    if sc_file in controls:
        fname = controls[sc_file]
    elif gen_file in controls:
        fname = controls[gen_file]
    else:
        fname = '{}_controls.csv'.format(type)
    return fname


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
    fname = get_control_file(type='employment')
    orca.add_injectable("employment_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname), index_col='year')


@orca.table(cache=True)
def regional_controls():
    fname = get_control_file(type='regional')
    orca.add_injectable("reg_control_file", fname)
    return pd.read_csv(os.path.join('data', fname), index_col="year")


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


@orca.table(cache=True)
def taz_forecast_inputs():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "taz_forecast_inputs.csv"),
        index_col="TAZ1454")


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


@orca.table(cache=True)
def taz2_price_shifters():
    return pd.read_csv(os.path.join(misc.data_dir(),
                                    "taz2_price_shifters.csv"),
                       index_col="TAZ")


# these are shapes - "zones" in the bay area
@orca.table(cache=True)
def zones(store):
    # sort index so it prints out nicely when we want it to
    return store['zones'].sort_index()


# SLR inundation levels for parcels, with full, partial, or no mitigation
@orca.table(cache=True)
def slr_parcel_inundation():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_parcel_inundation.csv"),
        index_col='parcel_id')


@orca.table(cache=True)
def slr_parcel_inundation_mf():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_parcel_inundation_mf.csv"),
        index_col='parcel_id')


@orca.table(cache=True)
def slr_parcel_inundation_mp():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_parcel_inundation_mp.csv"),
        index_col='parcel_id')


# SLR progression by year, for "futures" C, B, R
@orca.table(cache=True)
def slr_progression_C():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_progression_C.csv"))


@orca.table(cache=True)
def slr_progression_B():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_progression_B.csv"))


@orca.table(cache=True)
def slr_progression_R():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_progression_R.csv"))


# census tracts for parcels, to assign earthquake probabilities
@orca.table(cache=True)
def parcels_tract():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "parcel_tract_xwalk.csv"),
        index_col='parcel_id')


# earthquake and fire damage probabilities for census tracts
@orca.table(cache=True)
def tracts_earthquake():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "tract_damage_earthquake.csv"))


# this specifies the relationships between tables
orca.broadcast('buildings', 'residential_units', cast_index=True,
               onto_on='building_id')
orca.broadcast('residential_units', 'households', cast_index=True,
               onto_on='unit_id')
orca.broadcast('parcels_geography', 'buildings', cast_index=True,
               onto_on='parcel_id')
orca.broadcast('parcels', 'buildings', cast_index=True,
               onto_on='parcel_id')
# not defined in urbansim_Defaults
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True,
               onto_on='zone_id')

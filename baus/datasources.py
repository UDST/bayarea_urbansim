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


@orca.injectable()
def initial_year():
    return 2010


@orca.injectable()
def final_year():
    return 2040


@orca.injectable('store', cache=True)
def hdfstore(settings):
    return pd.HDFStore(
        os.path.join(misc.data_dir(), settings["store"]))


@orca.injectable(cache=True)
def low_income(settings):
    return int(settings["low_income_for_hlcm"])


@orca.injectable("limits_settings", cache=True)
def limits_settings(settings, scenario):

    d = settings['development_limits']

    if scenario in d.keys():
        print "Using limits for scenario: %s" % scenario
        return d[scenario]

    if "default" in d.keys():
        print "Using default limits"
        return d["default"]

    # assume there's no scenario-based limits and the dict is the limits
    return d


@orca.injectable(cache=True)
def inclusionary_housing_settings(settings, scenario):

    s = settings['inclusionary_housing_settings']

    if scenario in s.keys():
        print "Using inclusionary settings for scenario: %s" % scenario
        s = s[scenario]

    elif "default" in s.keys():
        print "Using default inclusionary settings"
        s = s["default"]

    d = {}
    for item in s:
        print "Setting inclusionary rates for %d cities to %.2f" %\
            (len(item["values"]), item["amount"])
        # this is a list of inclusionary rates and the cities they apply
        # to - need to turn it in a map of city names to rates
        for juris in item["values"]:
            d[juris] = item["amount"]

    return d


@orca.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


# key locations in the Bay Area for use as attractions in the models
@orca.table('landmarks', cache=True)
def landmarks():
    return pd.read_csv(os.path.join(misc.data_dir(), 'landmarks.csv'),
                       index_col="name")


@orca.table('jobs', cache=True)
def jobs(store):

    if 'jobs_urbansim_allocated' not in store:
        # if jobs allocation hasn't been done, then do it
        # (this should only happen once)
        orca.run(["allocate_jobs"])

    return store['jobs_urbansim_allocated']


# the way this works is there is an orca step to do jobs allocation, which
# reads base year totals and creates jobs and allocates them to buildings,
# and writes it back to the h5.  then the actual jobs table above just reads
# the auto-allocated version from the h5.  was hoping to just do allocation
# on the fly but it takes about 4 minutes so way to long to do on the fly
@orca.step('allocate_jobs')
def jobs(store, baseyear_taz_controls, settings, parcels):

    # this isn't pretty, but can't use orca table because there would
    # be a circular dependenct - I mean jobs dependent on buildings and
    # buildings on jobs, so we have to grab from the store directly
    buildings = store['buildings']
    buildings["non_residential_sqft"][
        buildings.building_type_id.isin([15, 16])] = 0
    buildings["building_sqft"][buildings.building_type_id.isin([15, 16])] = 0
    buildings["zone_id"] = misc.reindex(parcels.zone_id, buildings.parcel_id)

    # we need to do a new assignment from the controls to the buildings

    # first disaggregate the job totals
    sector_map = settings["naics_to_empsix"]
    jobs = []
    for taz, row in baseyear_taz_controls.local.iterrows():
        for sector_col, num in row.iteritems():

            # not a sector total
            if not sector_col.startswith("emp_sec"):
                continue

            # get integer sector id
            sector_id = int(''.join(c for c in sector_col if c.isdigit()))
            sector_name = sector_map[sector_id]

            jobs += [[sector_id, sector_name, taz, -1]] * int(num)

    # df is now the
    df = pd.DataFrame(jobs, columns=[
        'sector_id', 'empsix', 'taz', 'building_id'])

    # just do random assignment weighted by job spaces - we'll then
    # fill in the job_spaces if overfilled in the next step (code
    # has existed in urbansim for a while)
    for taz, cnt in df.groupby('taz').size().iteritems():

        potential_add_locations = buildings.non_residential_sqft[
            (buildings.zone_id == taz) &
            (buildings.non_residential_sqft > 0)]

        if len(potential_add_locations) == 0:
            # if no non-res buildings, put jobs in res buildings
            potential_add_locations = buildings.building_sqft[
                buildings.zone_id == taz]

        weights = potential_add_locations / potential_add_locations.sum()

        print taz, len(potential_add_locations),\
            potential_add_locations.sum(), cnt

        buildings_ids = potential_add_locations.sample(
            cnt, replace=True, weights=weights)

        df["building_id"][df.taz == taz] = buildings_ids.index.values

    s = buildings.zone_id.loc[df.building_id].value_counts()
    t = baseyear_taz_controls.emp_tot - s
    # assert we matched the totals exactly
    assert t.sum() == 0

    # this is some exploratory diagnostics comparing the job controls to
    # the buildings table - in other words, comparing non-residential space
    # to the number of jobs
    '''
    old_jobs = store['jobs']
    old_jobs_cnt = old_jobs.groupby('taz').si ze()

    emp_tot = baseyear_taz_controls.emp_tot
    print buildings.job_spaces.groupby(buildings.building_type_id).sum()
    supply = buildings.job_spaces.groupby(buildings.zone_id).sum()
    non_residential_sqft = buildings.non_residential_sqft.\
        groupby(buildings.zone_id).sum()
    s = (supply-emp_tot).order()
    df = pd.DataFrame({
        "job_spaces": supply,
        "jobs": emp_tot,
        "non_residential_sqft": non_residential_sqft
    }, index=s.index)
    df["vacant_spaces"] = supply-emp_tot
    df["vacancy_rate"] = df.vacant_spaces/supply.astype('float')
    df["old_jobs"] = old_jobs_cnt
    df["old_vacant_spaces"] = supply-old_jobs_cnt
    df["old_vacancy_rate"] = df.old_vacant_spaces/supply.astype('float')
    df["sqft_per_job"] = df.non_residential_sqft / df.jobs
    df["old_sqft_per_job"] = df.non_residential_sqft / df.old_jobs
    df.index.name = "zone_id"
    print df[["jobs", "old_jobs", "job_spaces", "non_residential_sqft"]].corr()
    df.sort("sqft_per_job").to_csv("job_demand.csv")
    '''

    store['jobs_urbansim_allocated'] = df


@orca.table(cache=True)
def baseyear_taz_controls():
    return pd.read_csv(os.path.join("data",
                       "baseyear_taz_controls.csv"), index_col="taz1454")


@orca.table(cache=True)
def base_year_summary_taz():
    return pd.read_csv(os.path.join('output',
                       'baseyear_taz_summaries_2010.csv'),
                       index_col="zone_id")


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
    df = pd.read_csv(os.path.join(misc.data_dir(), "zoning_lookup.csv"))
    # this part is a bit strange - we do string matching on the names of zoning
    # in order ot link parcels and zoning and some of the strings have small
    # differences, so we copy the row and have different strings for the same
    # lookup row.  for now we drop duplicates of the id field in order to run
    # in urbansim (all the attributes of rows that share an id are the same -
    # only the name is different)
    df = df.drop_duplicates(subset='id').set_index('id')
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
def zoning_baseline(parcels, zoning_lookup, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(),
                     "2015_12_21_zoning_parcels.csv"),
                     index_col="geom_id")
    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    d = {k: "type%d" % v for k, v in settings["building_type_map2"].items()}

    df.columns = [d.get(x, x) for x in df.columns]

    return df


@orca.table('zoning_scenario', cache=True)
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
                                  "02_01_2016_parcels_geography.csv"),
                     index_col="geom_id", dtype={'jurisdiction': 'str'})
    df = geom_id_to_parcel_id(df, parcels)

    juris_name = pd.read_csv(os.path.join(misc.data_dir(),
                                          "census_id_to_name.csv"),
                             index_col="census_id").name10

    df["juris_name"] = df.jurisdiction_id.map(juris_name)

    df["pda_id"] = df.pda_id.str.lower()

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
    df["geom_id"] = geom_id.values  # add it back again cause it goes away

    # we don't predict prices for schools and hotels right now
    df = df.query("building_type_id <= 4 or building_type_id >= 7")

    df["deed_restricted_units"] = 0

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
def buildings(store, parcels, households, jobs, building_sqft_per_job,
              settings, manual_edits):

    # start with buildings from urbansim_defaults
    df = datasources.buildings(store, households, jobs,
                               building_sqft_per_job, settings)

    df = df.drop(['development_type_id', 'improvement_value',
                  'sqft_per_unit', 'nonres_rent_per_sqft',
                  'res_price_per_sqft', 'redfin_sale_price',
                  'redfin_home_type', 'costar_property_type',
                  'costar_rent'], axis=1)

    edits = manual_edits.local
    edits = edits[edits.table == 'buildings']
    for index, row, col, val in \
            edits[["id", "attribute", "new_value"]].itertuples():
        df.set_value(row, col, val)

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

    # hope we get more data on this soon
    df["deed_restricted_units"] = 0

    if "skip_deed_restricted_units" in settings:
        return df

    zone_ids = misc.reindex(parcels.zone_id, df.parcel_id).\
        reindex(df.index).fillna(-1)

    # sample deed restricted units to match current deed restricted unit
    # zone totals
    for taz, row in pd.read_csv('data/deed_restricted_zone_totals.csv',
                                index_col='taz_key').iterrows():

        cnt = row["units"]

        if cnt <= 0:
            continue

        potential_add_locations = df.residential_units[
            (zone_ids == taz) &
            (df.residential_units > 0)]

        assert len(potential_add_locations) > 0

        weights = potential_add_locations / potential_add_locations.sum()

        buildings_ids = potential_add_locations.sample(
            cnt, replace=True, weights=weights)

        units = pd.Series(buildings_ids.index.values).value_counts()
        df.loc[units.index, "deed_restricted_units"] += units.values

    print "Total deed restricted units after random selection: %d" % \
        df.deed_restricted_units.sum()

    df["deed_restricted_units"] = \
        df[["deed_restricted_units", "residential_units"]].min(axis=1)

    print "Total deed restricted units after truncating to res units: %d" % \
        df.deed_restricted_units.sum()

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


@orca.table(cache=True)
def superdistricts():
    return pd.read_csv(os.path.join(misc.data_dir(),
                       "superdistricts.csv"), index_col="number")


@orca.table(cache=True)
def abag_targets():
    return pd.read_csv(os.path.join(misc.data_dir(), "abag_targets.csv"))


@orca.table('taz_geography', cache=True)
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

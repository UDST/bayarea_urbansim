import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca


@orca.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@orca.table('jobs', cache=True)
def jobs(store):
    # nets = store['nets']
    ###go from establishments to jobs
    # df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)]\
        # .reset_index()
    # df.index.name = 'job_id'
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


@orca.column('homesales', 'node_id', cache=True)
def node_id(homesales, parcels):
    return misc.reindex(parcels.node_id, homesales.parcel_id)


@orca.column('homesales', 'zone_id', cache=True)
def zone_id(homesales, parcels):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


@orca.column('homesales', cache=True)
def base_price_per_sqft(homesales):
    s = homesales.price_per_sqft.groupby(homesales.zone_id).quantile()
    return misc.reindex(s, homesales.zone_id)


@orca.column('buildings', cache=True)
def base_price_per_sqft(homesales, buildings):
    s = homesales.price_per_sqft.groupby(homesales.zone_id).quantile()
    return misc.reindex(s, buildings.zone_id).reindex(buildings.index).fillna(s.quantile())


# non-residential rent data
@orca.table('costar', cache=True)
def costar(store):
    df = store['costar']
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
    return df


@orca.table(cache=True)
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
@orca.table('zoning_baseline', cache=True)
def zoning_baseline(parcels, zoning_lookup):
    df = pd.read_csv(os.path.join(misc.data_dir(), "2015_08_13_zoning_parcels.csv"),
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


@orca.table('zoning_np', cache=True)
def zoning_np(parcels_geography):
    scenario_zoning = pd.read_csv(os.path.join(misc.data_dir(),
                                                 'zoning_mods_np.csv'))
    return pd.merge(parcels_geography.to_frame(),
                    scenario_zoning,
                    on=['jurisdiction', 'pda_id', 'tpp_id', 'exp_id'],
                    how='left')


# this is really bizarre, but the parcel table I have right now has empty
# zone_ids for a few parcels.  Not enough to worry about so just filling with
# the mode
@orca.table('parcels', cache=True)
def parcels(store, net):
    df = store['parcels']
    df["zone_id"] = df.zone_id.replace(0, 1)

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


@orca.column('parcels', cache=True)
def pda(parcels, parcels_geography):
    return parcels_geography.pda_id.reindex(parcels.index)


@orca.table(cache=True)
def parcels_geography(parcels):
    df = pd.read_csv(os.path.join(misc.data_dir(), "2015_08_13_parcels_geography.csv"),
                     index_col="geom_id")
    return geom_id_to_parcel_id(df, parcels)


@orca.table(cache=True)
def development_projects(parcels, settings):
    df = pd.read_csv(os.path.join(misc.data_dir(), "development_projects.csv"))

    for fld in ['residential_sqft', 'residential_price', 'non_residential_price']:
        df[fld] = 0
    df["redfin_sale_year"] = 2012 # hedonic doesn't tolerate nans
    df["stories"] = df.stories.fillna(1)
    df["building_sqft"] = df.building_sqft.fillna(0)
    df["non_residential_sqft"] = df.non_residential_sqft.fillna(0)
    df["building_type_id"] = df.building_type.map(settings["building_type_map2"])

    df = df.dropna(subset=["geom_id"]) # need a geom_id to link to parcel_id

    df = df.dropna(subset=["year_built"]) # need a year built to get built

    df["geom_id"] = df.geom_id.astype("int")
    df = df.query('residential_units != "rent"')
    df["residential_units"] = df.residential_units.astype("int")
    df = df.set_index("geom_id")
    df = geom_id_to_parcel_id(df, parcels).reset_index() # use parcel id

    # we don't predict prices for schools and hotels right now
    df = df.query("building_type_id <= 4 or building_type_id >= 7")

    print "Describe of development projects"
    print df[orca.get_table('buildings').local_columns].describe()
    
    return df


@orca.table('buildings', cache=True)
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
    
    # BRUTE FORCE INCREASE THE CAPACITY FOR MORE JOBS
    print "WARNING: this has the hard-coded version which unrealistically increases non-residential square footage to house all the base year jobs" 
    df["non_residential_sqft"] = (df.non_residential_sqft * 1.33).astype('int')

    # we should only be using the "buildings" table during simulation, and in
    # simulation we want to normalize the prices to 2012 style prices
    df["redfin_sale_year"] = 2012
    return df


def households_building_id(residential_units, households):
    df = misc.reindex(residential_units.building_id, households.unit_id)
    return df.fillna(-1)


@orca.table('residential_units', cache=True)
def residential_units(buildings, households):
    # in lieu of having a real units table in the base year, we're going to
    # build one from the buildings table.
    df = pd.DataFrame({
        "unit_residential_price": 0,
        "unit_residential_rent": 0,
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
    orca.add_table("households", households)

    # now that building_id is dropped from households, we can add the
    # function to compute it from the relationship between households
    # and residential_units
    orca.add_column("households", "building_id", households_building_id)
    
    # ASSIGN INITIAL UNIT TENURE BASED ON HOUSEHOLDS TABLE
    # 0= owner occupied, 1= rented
    # cf households -> hownrent, where 1= owns, 2= rents
    df["unit_tenure"] = np.nan
    ownership_mask = (households.hownrent == 1) & (households.unit_id != -1)
    rental_mask = (households.hownrent == 2) & (households.unit_id != -1)
    
    df.loc[households[ownership_mask].unit_id.values, "unit_tenure"] = 0
    df.loc[households[rental_mask].unit_id.values, "unit_tenure"] = 1
    
    print "Initial unit tenure assignment: %d%% owner occupied, %d%% unfilled" % \
    		(round(len(df[df.unit_tenure == 0])*100/len(df[df.unit_tenure.notnull()])), \
    		 round(len(df[df.unit_tenure.isnull()])*100/len(df)))

	# fill remaining units with random tenure assignment
    unfilled = df[df.unit_tenure.isnull()].index
    df.loc[unfilled, "unit_tenure"] = np.random.randint(0, 2, len(unfilled))
    
    return df


@orca.column('residential_units', 'vacant_units')
def vacant_units(residential_units, households):
    return residential_units.num_units.sub(
        households.unit_id[households.unit_id != -1].value_counts(),
        fill_value=0)


@orca.column('residential_units', 'node_id')
def node_id(residential_units, buildings):
    return misc.reindex(buildings.node_id, residential_units.building_id)


@orca.column('residential_units', 'zone_id')
def zone_id(residential_units, buildings):
    return misc.reindex(buildings.zone_id, residential_units.building_id)


@orca.column('residential_units', 'submarket_id')
def submarket_id(residential_units, buildings):
    return misc.reindex(buildings.zone_id, residential_units.building_id)


# setting up a separate aggregations list for unit-based models
@orca.injectable("unit_aggregations")
def unit_aggregations(settings):
    if "unit_aggregation_tables" not in settings or \
    	settings["unit_aggregation_tables"] is None:
    	return []
    return [orca.get_table(tbl) for tbl in settings["unit_aggregation_tables"]]


@orca.table('craigslist', cache=True)
def craigslist():
	df = pd.read_csv(os.path.join(misc.data_dir(), "sfbay_craigslist.csv"))
	net = orca.get_injectable('net')
	df['node_id'] = net.get_node_ids(df['longitude'], df['latitude'])
	# fill nans -- missing bedrooms are mostly studio apts
	df['bedrooms'] = df.bedrooms.replace(np.nan, 1)
	df['neighborhood'] = df.neighborhood.replace(np.nan, '')
	return df


@orca.column('craigslist', 'zone_id', cache=True)
def zone_id(craigslist, parcels):
    return misc.reindex(parcels.zone_id, craigslist.node_id)


#######################
# EXTRA PUMS VARIABLES
#######################

@orca.table('household_extras', cache=True)
def household_extras():
	df = pd.read_csv(os.path.join(misc.data_dir(), "household_extras.csv"))
	df = df.set_index('serialno')
	return df


@orca.column('households', 'white')
def white(households, household_extras):
    return misc.reindex(household_extras.white, households.serialno).fillna(0)


@orca.column('households', 'black')
def black(households, household_extras):
    return misc.reindex(household_extras.black, households.serialno).fillna(0)


@orca.column('households', 'asian')
def asian(households, household_extras):
    return misc.reindex(household_extras.asian, households.serialno).fillna(0)


@orca.column('households', 'hisp')
def hisp(households, household_extras):
    return misc.reindex(household_extras.hisp, households.serialno).fillna(0)


# this specifies the relationships between tables
orca.broadcast('parcels_geography', 'buildings', cast_index=True, onto_on='parcel_id')

orca.broadcast('nodes', 'homesales', cast_index=True, onto_on='node_id')
orca.broadcast('nodes', 'costar', cast_index=True, onto_on='node_id')
orca.broadcast('nodes', 'craigslist', cast_index=True, onto_on='node_id')

orca.broadcast('logsums', 'homesales', cast_index=True, onto_on='zone_id')
orca.broadcast('logsums', 'costar', cast_index=True, onto_on='zone_id')
orca.broadcast('logsums', 'craigslist', cast_index=True, onto_on='zone_id')

orca.broadcast('buildings', 'residential_units', cast_index=True, onto_on='building_id')

orca.broadcast('households', 'household_extras', cast_index=True, onto_on='serialno')

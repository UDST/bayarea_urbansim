from urbansim.utils import misc
import os
import sys
import orca
import datasources
import variables
from utils import parcel_id_to_geom_id, add_buildings
from urbansim.utils import networks
import pandana.network as pdna
from urbansim_defaults import models
from urbansim_defaults import utils
from urbansim.developer import sqftproforma, developer
from urbansim.developer.developer import Developer as dev
import subsidies
import summaries
import numpy as np
import pandas as pd


@orca.step('rsh_simulate')
def rsh_simulate(buildings, aggregations, settings):
    utils.hedonic_simulate("rsh.yaml", buildings, aggregations,
                           "residential_price")
    if "rsh_simulate" in settings:
        low = float(settings["rsh_simulate"]["low"])
        high = float(settings["rsh_simulate"]["high"])
        buildings.update_col("residential_price",
                             buildings.residential_price.clip(low, high))
        print "Clipped rsh_simulate produces\n", \
            buildings.residential_price.describe()


@orca.step('households_transition')
def households_transition(households, household_controls, year, settings):
    s = orca.get_table('households').base_income_quartile.value_counts()
    print "Distribution by income before:\n", (s/s.sum())
    ret = utils.full_transition(households,
                                household_controls,
                                year,
                                settings['households_transition'],
                                "building_id")
    s = orca.get_table('households').base_income_quartile.value_counts()
    print "Distribution by income after:\n", (s/s.sum())
    return ret


@orca.step('households_relocation')
def households_relocation(households, settings, years_per_iter):
    rate = settings['rates']['households_relocation']
    rate = min(rate * years_per_iter, 1.0)
    return utils.simple_relocation(households, rate, "building_id")


@orca.step('jobs_relocation')
def jobs_relocation(jobs, settings, years_per_iter):
    rate = settings['rates']['jobs_relocation']
    rate = min(rate * years_per_iter, 1.0)
    return utils.simple_relocation(jobs, rate, "building_id")


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time
@orca.step("scheduled_development_events")
def scheduled_development_events(buildings, development_projects,
                                 demolish_events, summary, year, parcels,
                                 settings, years_per_iter,
                                 building_sqft_per_job, vmt_fee_categories):

    # first demolish
    demolish = demolish_events.to_frame().\
        query("%d <= year_built < %d" % (year, year + years_per_iter))
    print "Demolishing/building %d buildings" % len(demolish)
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(
        buildings.to_frame(buildings.local_columns),
        demolish,
        unplace_agents=["households", "jobs"])
    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print "Demolished %d buildings" % (l1 - len(buildings))
    print "    (this number is smaller when parcel has no existing buildings)"

    # then build
    dps = development_projects.to_frame().\
        query("%d <= year_built < %d" % (year, year + years_per_iter))

    if len(dps) == 0:
        return

    new_buildings = utils.scheduled_development_events(
        buildings, dps,
        remove_developed_buildings=False,
        unplace_agents=['households', 'jobs'])
    new_buildings["form"] = new_buildings.building_type_id.map(
        settings['building_type_map']).str.lower()
    new_buildings["job_spaces"] = new_buildings.building_sqft / \
        new_buildings.building_type_id.fillna(-1).map(building_sqft_per_job)
    new_buildings["job_spaces"] = new_buildings.job_spaces.astype('int')
    new_buildings["geom_id"] = parcel_id_to_geom_id(new_buildings.parcel_id)
    new_buildings["SDEM"] = True
    new_buildings["subsidized"] = False

    new_buildings["zone_id"] = misc.reindex(
        parcels.zone_id, new_buildings.parcel_id)
    new_buildings["vmt_res_cat"] = misc.reindex(
        vmt_fee_categories.res_cat, new_buildings.zone_id)
    del new_buildings["zone_id"]

    summary.add_parcel_output(new_buildings)


@orca.injectable("supply_and_demand_multiplier_func", autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
    s = demand / supply
    settings = orca.get_injectable('settings')
    print "Number of submarkets where demand exceeds supply:", len(s[s > 1.0])
    # print "Raw relationship of supply and demand\n", s.describe()
    supply_correction = settings["enable_supply_correction"]
    clip_change_high = supply_correction["kwargs"]["clip_change_high"]
    t = s
    t -= 1.0
    t = t / t.max() * (clip_change_high-1)
    t += 1.0
    s.loc[s > 1.0] = t.loc[s > 1.0]
    # print "Shifters for current iteration\n", s.describe()
    return s, (s <= 1.0).all()


# this if the function for mapping a specific building that we build to a
# specific building type
@orca.injectable("form_to_btype_func", autocall=False)
def form_to_btype_func(building):
    settings = orca.get_injectable('settings')
    form = building.form
    dua = building.residential_units / (building.parcel_size / 43560.0)
    # precise mapping of form to building type for residential
    if form is None or form == "residential":
        if dua < 16:
            return 1
        elif dua < 32:
            return 2
        return 3
    return settings["form_to_btype"][form][0]


@orca.injectable("add_extra_columns_func", autocall=False)
def add_extra_columns(df):
    for col in ["residential_price", "non_residential_price"]:
        df[col] = 0

    df["redfin_sale_year"] = 2012

    if "residential_units" not in df:
        df["residential_units"] = 0

    if "parcel_size" not in df:
        df["parcel_size"] = \
            orca.get_table("parcels").parcel_size.loc[df.parcel_id]

    if "year" in orca.orca._INJECTABLES and "year_built" not in df:
        df["year_built"] = orca.get_injectable("year")

    if "form_to_btype_func" in orca.orca._INJECTABLES and \
        "building_type_id" not in df:
        form_to_btype_func = orca.get_injectable("form_to_btype_func")
        df["building_type_id"] = df.apply(form_to_btype_func, axis=1)

    return df


@orca.step('alt_feasibility')
def alt_feasibility(parcels, settings,
                    parcel_sales_price_sqft_func,
                    parcel_is_allowed_func):
    kwargs = settings['feasibility']
    config = sqftproforma.SqFtProFormaConfig()
    config.parking_rates["office"] = 1.5
    config.parking_rates["retail"] = 1.5

    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=config,
                          **kwargs)

    f = subsidies.add_fees_to_feasibility(
        orca.get_table('feasibility').to_frame(),
        parcels, drop_unprofitable=True)

    orca.add_table("feasibility", f)


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func, parcels_geography,
                          limits_settings):

    kwargs = settings['residential_developer']

    num_units = dev.compute_units_to_build(
        len(households),
        buildings["residential_units"].sum(),
        kwargs['target_vacancy'])

    targets = []
    typ = "Residential"
    # now apply limits - limits are assumed to be yearly, apply to an
    # entire jurisdiction and be in terms of residential_units or job_spaces
    if typ in limits_settings:

        juris_name = parcels_geography.juris_name.\
            reindex(parcels.index).fillna('Other')

        juris_list = limits_settings[typ].keys()
        for juris, limit in limits_settings[typ].items():

            # the actual target is the limit times the number of years run
            # so far in the simulation (plus this year), minus the amount
            # built in previous years - in other words, you get rollover
            # and development is lumpy

            current_total = parcels.total_residential_units[
                (juris_name == juris) & (parcels.newest_building >= 2010)]\
                .sum()

            target = (year - 2010 + 1) * limit - current_total

            if target <= 0:
                    continue

            targets.append((juris_name == juris, target, juris))
            num_units -= target

        # other cities not in the targets get the remaining target
        targets.append((~juris_name.isin(juris_list), num_units, "none"))

    else:
        # otherwise use all parcels with total number of units
        targets.append((parcels.index == parcels.index, num_units, "none"))

    for parcel_mask, target, juris in targets:

        print "Running developer for %s with target of %d" % \
            (str(juris), target)

        # this was a fairly heinous bug - have to get the building wrapper
        # again because the buildings df gets modified by the run_developer
        # method below
        buildings = orca.get_table('buildings')

        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size[parcel_mask],
            parcels.ave_sqft_per_unit[parcel_mask],
            parcels.total_residential_units[parcel_mask],
            feasibility,
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            num_units_to_build=target,
            **kwargs)

        if new_buildings is not None:
            new_buildings["subsidized"] = False

        summary.add_parcel_output(new_buildings)


@orca.step()
def retail_developer(jobs, buildings, parcels, nodes, feasibility,
                     settings, summary, add_extra_columns_func, net):

    dev_settings = settings['non_residential_developer']

    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    target = all_units * float(dev_settings['type_splits']["retail"])

    nodes = nodes.to_frame()

    # then compute the ratio of income to retail sqft - a high number here
    # indicates an underserved market
    nodes["retail_ratio"] = nodes.sum_income / nodes.retail_sqft.clip(lower=1)

    feasibility = feasibility.to_frame().loc[:,"retail"]

    # add node_id to feasibility frame
    feasibility["node_id"] = misc.reindex(parcels.node_id,
                                          feasibility.parcel_id)

    # add retail_ratio to feasibility frame using node_id
    feasibility["retail_ratio"] = misc.reindex(nodes.retail_ratio,
                                               feasibility.node_id)

    # create features
    f1 = feasibility.retail_ratio / feasibility.retail_ratio.max()
    f2 = feasibility.max_profit / feasibility.max_profit.max()

    # combine features in probability function - it's like combining expense
    # of building the building with the market in the neighborhood
    p = f1 * 1.5 + f2

    import time
    print time.ctime()
    print "Attempting to build {:,} retail sqft".format(target)

    # order by weighted random sample
    feasibility = feasibility.sample(frac=1.0, weights=p)

    bldgs = buildings.to_frame(buildings.local_columns + ["general_type"])

    devs = []

    for dev_id, dev in feasibility.iterrows():

        # any special logic to filter these devs?

        # remove new dev sqft from target
        target -= dev.non_residential_sqft

        # add redeveloped sqft to target
        filt = "general_type == 'Retail' and parcel_id == %d" % dev["parcel_id"]
        target += bldgs.query(filt).non_residential_sqft.sum()

        devs.append(dev)

        if target < 0:
            break

    print time.ctime()

    # record keeping - add extra columns to match building dataframe
    # add the buidings and demolish old buildings, and add to debug output
    devs = pd.DataFrame(devs, columns=feasibility.columns)

    print "Building {:,} retail sqft in {:,} projects".format(
        devs.non_residential_sqft.sum(), len(devs))
    if target > 0:
        print "   WARNING: retail target not met"

    devs["form"] = "retail"
    devs = add_extra_columns_func(devs)

    add_buildings(buildings, devs)
    print time.ctime()

    summary.add_parcel_output(devs)


@orca.step()
def office_developer(feasibility, jobs, buildings, parcels, year,
                     settings, summary, form_to_btype_func, scenario,
                     add_extra_columns_func, parcels_geography,
                     limits_settings):

    dev_settings = settings['non_residential_developer']

    # I'm going to try a new way of computing this because the math the other
    # way is simply too hard.  Basically we used to try and apportion sectors
    # into the demand for office, retail, and industrial, but there's just so
    # much dirtyness to the data, for instance 15% of jobs are in residential
    # buildings, and 15% in other buildings, it's just hard to know how much
    # to build, we I think the right thing to do is to compute the number of
    # job spaces that are required overall, and then to apportion that new dev
    # into the three non-res types with a single set of coefficients
    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    print "Total units to build = %d" % all_units
    if all_units <= 0:
        return

    for typ in ["Office"]:

        print "\nRunning for type: ", typ

        num_units = all_units * float(dev_settings['type_splits'][typ])

        targets = []
        # now apply limits - limits are assumed to be yearly, apply to an
        # entire jurisdiction and be in terms of residential_units or
        # job_spaces
        if year > 2015 and typ in limits_settings:

            juris_name = parcels_geography.juris_name.\
                reindex(parcels.index).fillna('Other')

            juris_list = limits_settings[typ].keys()
            for juris, limit in limits_settings[typ].items():

                # the actual target is the limit times the number of years run
                # so far in the simulation (plus this year), minus the amount
                # built in previous years - in other words, you get rollover
                # and development is lumpy

                current_total = parcels.total_job_spaces[
                    (juris_name == juris) & (parcels.newest_building > 2015)]\
                    .sum()

                target = (year - 2015 + 1) * limit - current_total

                if target <= 0:
                    print "Already met target for juris = %s" % juris
                    print "    target = %d, current_total = %d" %\
                        (target, current_total)
                    continue

                targets.append((juris_name == juris, target, juris))
                num_units -= target

            # other cities not in the targets get the remaining target
            targets.append((~juris_name.isin(juris_list), num_units, "none"))

        else:
            # otherwise use all parcels with total number of units
            targets.append((parcels.index == parcels.index, num_units, "none"))

        for parcel_mask, target, juris in targets:

            print "Running developer for %s with target of %d" % \
                (str(juris), target)
            print "Parcels in play:\n", pd.Series(parcel_mask).value_counts()

            # this was a fairly heinous bug - have to get the building wrapper
            # again because the buildings df gets modified by the run_developer
            # method below
            buildings = orca.get_table('buildings')

            new_buildings = utils.run_developer(
                typ.lower(),
                jobs,
                buildings,
                "job_spaces",
                parcels.parcel_size[parcel_mask],
                parcels.ave_sqft_per_unit[parcel_mask],
                parcels.total_job_spaces[parcel_mask],
                feasibility,
                year=year,
                form_to_btype_callback=form_to_btype_func,
                add_more_columns_callback=add_extra_columns_func,
                residential=False,
                num_units_to_build=target,
                **dev_settings['kwargs'])

            if new_buildings is not None:
                new_buildings["subsidized"] = False

            summary.add_parcel_output(new_buildings)


@orca.step()
def developer_reprocess(buildings, year, years_per_iter, jobs,
                        parcels, summary, parcel_is_allowed_func):
    # this takes new units that come out of the developer, both subsidized
    # and non-subsidized and reprocesses them as required - please read
    # comments to see what this means in detail

    # 20% of base year buildings which are "residential" have job spaces - I
    # mean, there is a ratio of job spaces to res units in residential
    # buildings of 1 to 5 - this ratio should be kept for future year
    # buildings
    s = buildings.general_type == "Residential"
    res_units = buildings.residential_units[s].sum()
    job_spaces = buildings.job_spaces[s].sum()

    to_add = res_units * .20 - job_spaces
    if to_add > 0:
        print "Adding %d job_spaces" % to_add
        res_units = buildings.residential_units[s]
        # bias selection of places to put job spaces based on res units
        add_indexes = np.random.choice(res_units.index.values, size=to_add,
                                       replace=True,
                                       p=(res_units/res_units.sum()))
        # collect same indexes
        add_indexes = pd.Series(add_indexes).value_counts()
        # this is sqft per job for residential bldgs
        add_sizes = add_indexes * 400
        print "Job spaces in res before adjustment: ", \
            buildings.job_spaces[s].sum()
        buildings.local.loc[add_sizes.index,
                            "non_residential_sqft"] += add_sizes.values
        print "Job spaces in res after adjustment: ",\
            buildings.job_spaces[s].sum()

    # the second step here is to add retail to buildings that are greater than
    # X stories tall - presumably this is a ground floor retail policy
    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = old_buildings.query(
       '%d == year_built and stories >= 4' % year)

    print "Attempting to add ground floor retail to %d devs" % \
        len(new_buildings)
    retail = parcel_is_allowed_func("retail")
    new_buildings = new_buildings[retail.loc[new_buildings.parcel_id].values]
    print "Disallowing dev on these parcels:"
    print "    %d devs left after retail disallowed" % len(new_buildings)

    # this is the key point - make these new buildings' nonres sqft equal
    # to one story of the new buildings
    new_buildings.non_residential_sqft = new_buildings.building_sqft / \
        new_buildings.stories * .8

    new_buildings["residential_units"] = 0
    new_buildings["residential_sqft"] = 0
    new_buildings["building_sqft"] = new_buildings.non_residential_sqft
    new_buildings["stories"] = 1
    new_buildings["building_type_id"] = 10

    print "Adding %d sqft of ground floor retail in %d locations" % \
        (new_buildings.non_residential_sqft.sum(), len(new_buildings))

    all_buildings = dev.merge(old_buildings, new_buildings)
    orca.add_table("buildings", all_buildings)

    new_buildings["form"] = "retail"
    # this is sqft per job for retail use - this is all rather
    # ad-hoc so I'm hard-coding
    new_buildings["job_spaces"] = \
        (new_buildings.non_residential_sqft / 445.0).astype('int')
    new_buildings["net_units"] = new_buildings.job_spaces
    summary.add_parcel_output(new_buildings)

    # got to get the frame again because we just added rows
    buildings = orca.get_table('buildings')
    buildings_df = buildings.to_frame(
        ['year_built', 'building_sqft', 'general_type'])
    sqft_by_gtype = buildings_df.query('year_built >= %d' % year).\
        groupby('general_type').building_sqft.sum()
    print "New square feet by general type in millions:\n",\
        sqft_by_gtype / 1000000.0


def make_network(name, weight_col, max_distance):
    st = pd.HDFStore(os.path.join(misc.data_dir(), name), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[[weight_col]])
    net.precompute(max_distance)
    return net


def make_network_from_settings(settings):
    return make_network(
        settings["name"],
        settings.get("weight_col", "weight"),
        settings['max_distance']
    )


@orca.injectable('net', cache=True)
def build_networks(settings):
    nets = {}
    pdna.reserve_num_graphs(len(settings["build_networks"]))

    # yeah, starting to hardcode stuff, not great, but can only
    # do nearest queries on the first graph I initialize due to crummy
    # limitation in pandana
    for key in settings["build_networks"].keys():
        nets[key] = make_network_from_settings(
            settings['build_networks'][key]
        )

    return nets


@orca.step('local_pois')
def local_pois(settings):
    # because of the aforementioned limit of one netowrk at a time for the
    # POIS, as well as the large amount of memory used, this is now a
    # preprocessing step
    n = make_network(
        settings['build_networks']['walk']['name'],
        "weight", 3000)

    n.init_pois(
        num_categories=1,
        max_dist=3000,
        max_pois=1)

    cols = {}

    locations = pd.read_csv(os.path.join(misc.data_dir(), 'bart_stations.csv'))
    n.set_pois("tmp", locations.lng, locations.lat)
    cols["bartdist"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    locname = 'pacheights'
    locs = orca.get_table('locations').local.query("name == '%s'" % locname)
    n.set_pois("tmp", locs.lng, locs.lat)
    cols["pacheights"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    df.index.name = "node_id"
    df.to_csv('local_poi_distances.csv')


@orca.step('neighborhood_vars')
def neighborhood_vars(net):
    nodes = networks.from_yaml(net["walk"], "neighborhood_vars.yaml")
    nodes = nodes.replace(-np.inf, np.nan)
    nodes = nodes.replace(np.inf, np.nan)
    nodes = nodes.fillna(0)

    # nodes2 = pd.read_csv('data/local_poi_distances.csv', index_col="node_id")
    # nodes = pd.concat([nodes, nodes2], axis=1)

    print nodes.describe()
    orca.add_table("nodes", nodes)


@orca.step('regional_vars')
def regional_vars(net):
    nodes = networks.from_yaml(net["drive"], "regional_vars.yaml")
    nodes = nodes.fillna(0)

    nodes2 = pd.read_csv('data/regional_poi_distances.csv',
                         index_col="tmnode_id")
    nodes = pd.concat([nodes, nodes2], axis=1)

    print nodes.describe()
    orca.add_table("tmnodes", nodes)


@orca.step('regional_pois')
def regional_pois(settings, locations):
    # because of the aforementioned limit of one netowrk at a time for the
    # POIS, as well as the large amount of memory used, this is now a
    # preprocessing step
    n = make_network(
        settings['build_networks']['drive']['name'],
        "CTIMEV", 75)

    n.init_pois(
        num_categories=1,
        max_dist=75,
        max_pois=1)

    cols = {}
    for locname in ["embarcadero", "stanford", "pacheights"]:
        locs = locations.local.query("name == '%s'" % locname)
        n.set_pois("tmp", locs.lng, locs.lat)
        cols[locname] = n.nearest_pois(75, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    print df.describe()
    df.index.name = "tmnode_id"
    df.to_csv('regional_poi_distances.csv')


@orca.step('price_vars')
def price_vars(net):
    nodes2 = networks.from_yaml(net["walk"], "price_vars.yaml")
    nodes2 = nodes2.fillna(0)
    print nodes2.describe()
    nodes = orca.get_table('nodes')
    nodes = nodes.to_frame().join(nodes2)
    orca.add_table("nodes", nodes)


# this is not really simulation - just writing a method to get average
# appreciation per zone over the past X number of years
@orca.step("mls_appreciation")
def mls_appreciation(homesales, year, summary):
    buildings = homesales

    years = buildings.redfin_sale_year
    zone_ids = buildings.zone_id
    price = buildings.redfin_sale_price

    # minimum observations
    min_obs = 10

    def aggregate_year(year):
        mask = years == year
        s = price[mask].groupby(zone_ids[mask]).median()
        size = price[mask].groupby(zone_ids[mask]).size()
        return s, size

    current, current_size = aggregate_year(2013)

    zones = pd.DataFrame(index=zone_ids.unique())

    past, past_size = aggregate_year(year)
    appreciation = (current / past).pow(1.0/(2013-year))
    # zero out the zones with too few observations
    appreciation = appreciation * (current_size > min_obs).astype('int')
    appreciation = appreciation * (past_size > min_obs).astype('int')
    zones["appreciation"] = appreciation

    print zones.describe()

    summary.add_zone_output(zones, "appreciation", year)
    summary.write_zone_output()

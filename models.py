from urbansim.utils import misc
import os
import sys
import orca
import datasources
import variables
import subsidies
import summaries
from datasources import parcel_id_to_geom_id
from urbansim.utils import networks
import pandana.network as pdna
from urbansim_defaults import models
from urbansim_defaults import utils
from urbansim.developer import sqftproforma, developer
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


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time
@orca.step("scheduled_development_events")
def scheduled_development_events(buildings, development_projects,
                                 development_events, summary, year, parcels,
                                 settings, years_per_iter,
                                 building_sqft_per_job):

    # then build
    dps = development_projects.to_frame().query("%d <= year_built < %d" %
        (year, year + years_per_iter))

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


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func, parcels_geography):

    kwargs = settings['residential_developer']

    from urbansim.developer.developer import Developer as dev
    num_units = dev.compute_units_to_build(
        len(households),
        buildings["residential_units"].sum(),
        kwargs['target_vacancy'])

    targets = []
    typ = "Residential"
    # now apply limits - limits are assumed to be yearly, apply to an
    # entire jurisdiction and be in terms of residential_units or job_spaces
    if typ in settings['development_limits']:

        juris_name = parcels_geography.juris_name.\
            reindex(parcels.index).fillna('Other')

        juris_list = settings['development_limits'][typ].keys()
        for juris, limit in settings['development_limits'][typ].items():

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


@orca.step('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year,
                              settings, summary, form_to_btype_func,
                              add_extra_columns_func, parcels_geography):

    kwargs = settings['non_residential_developer']

    for typ in ["Office", "Retail", "Industrial"]:

        print "\nRunning for type: ", typ

        num_jobs_of_this_type = \
            (jobs.preferred_general_type == typ).value_counts()[True]

        num_job_spaces_of_this_type = \
            (buildings.job_spaces * (buildings.general_type == typ)).sum()

        from urbansim.developer.developer import Developer as dev
        num_units = dev.compute_units_to_build(num_jobs_of_this_type,
                                               num_job_spaces_of_this_type,
                                               kwargs['target_vacancy'])

        if num_units == 0:
            continue

        targets = []
        # now apply limits - limits are assumed to be yearly, apply to an
        # entire jurisdiction and be in terms of residential_units or
        # job_spaces
        if year > 2015 and typ in settings['development_limits']:

            juris_name = parcels_geography.juris_name.\
                reindex(parcels.index).fillna('Other')

            juris_list = settings['development_limits'][typ].keys()
            for juris, limit in settings['development_limits'][typ].items():

                # the actual target is the limit times the number of years run
                # so far in the simulation (plus this year), minus the amount
                # built in previous years - in other words, you get rollover
                # and development is lumpy

                current_total = parcels.total_job_spaces[
                    (juris_name == juris) & (parcels.newest_building > 2015)]\
                    .sum()

                target = (year - 2015 + 1) * limit - current_total

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
                **kwargs)

            if new_buildings is not None:
                 new_buildings["subsidized"] = False

            summary.add_parcel_output(new_buildings)


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

from urbansim.utils import misc
import os
import sys
import orca
import datasources
import variables
import subsidies
import summaries
from utils import parcel_id_to_geom_id
from urbansim.utils import networks
import pandana.network as pdna
from urbansim import accounts
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
import pandas as pd
from cStringIO import StringIO


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
                                 settings,
                                 building_sqft_per_job):

    # then build
    dps = development_projects.to_frame().query("year_built == %d" % year)

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


@orca.step('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year,
                              settings, summary, form_to_btype_func,
                              add_extra_columns_func):

    kwargs = settings['non_residential_developer']

    for typ in ["Office"]:
        # , "Retail", "Industrial"]:

        print "Running developer for type %s" % typ

        num_jobs_of_this_type = \
            (jobs.preferred_general_type == typ).value_counts()[True]

        num_job_spaces_of_this_type = \
            (buildings.job_spaces * (buildings.general_type == typ)).sum()

        from urbansim.developer.developer import Developer as dev
        num_units = dev.compute_units_to_build(num_jobs_of_this_type,
                                               num_job_spaces_of_this_type,
                                               kwargs['target_vacancy'])

        new_buildings = utils.run_developer(
            typ.lower(),
            jobs,
            buildings,
            "job_spaces",
            parcels.parcel_size,
            parcels.ave_sqft_per_unit,
            parcels.total_job_spaces,
            feasibility,
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            residential=False,
            num_units_to_build=num_units,
            **kwargs)

        summary.add_parcel_output(new_buildings)


@orca.injectable('net', cache=True)
def build_networks(settings):
    nets = {}
    net_settings = settings['build_networks']

    pdna.reserve_num_graphs(2)

    for key, value in net_settings.items():
        name = value["name"]
        st = pd.HDFStore(os.path.join(misc.data_dir(), name), "r")
        nodes, edges = st.nodes, st.edges
        weight_col = value.get("weight_col", "weight")
        net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                           edges[[weight_col]])
        net.precompute(value['max_distance'])
        nets[key] = net

    return nets


@orca.step('neighborhood_vars')
def neighborhood_vars(net):
    nodes = networks.from_yaml(net["walk"], "neighborhood_vars.yaml")
    nodes = nodes.fillna(0)
    print nodes.describe()
    orca.add_table("nodes", nodes)


@orca.step('regional_vars')
def regional_vars(net):
    nodes = networks.from_yaml(net["drive"], "regional_vars.yaml")
    nodes = nodes.fillna(0)
    print nodes.describe()
    orca.add_table("tmnodes", nodes)


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

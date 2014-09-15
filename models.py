from urbansim.developer import developer
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim.utils import networks
import os
import random
import utils
import dataset
import variables
import pandana as pdna
import pandas as pd
import numpy as np


@sim.model('rsh_estimate')
def rsh_estimate(homesales, nodes):
    return utils.hedonic_estimate("rsh.yaml", homesales, nodes)


@sim.model('rsh_simulate')
def rsh_simulate(buildings, nodes):
    return utils.hedonic_simulate("rsh.yaml", buildings, nodes,
                                  "residential_sales_price")


@sim.model('rrh_estimate')
def rrh_estimate(apartments, nodes):
    return utils.hedonic_estimate("rrh.yaml", apartments, nodes)


@sim.model('rrh_simulate')
def rrh_simulate(buildings, nodes):
    return utils.hedonic_simulate("rrh.yaml", buildings, nodes,
                                  "residential_rent")


@sim.model('nrh_estimate')
def nrh_estimate(costar, nodes):
    return utils.hedonic_estimate("nrh.yaml", costar, nodes)


@sim.model('nrh_simulate')
def nrh_simulate(buildings, nodes):
    return utils.hedonic_simulate("nrh.yaml", buildings, nodes,
                                  "non_residential_rent")


@sim.model('hlcmo_estimate')
def hlcmo_estimate(households, buildings, nodes):
    return utils.lcm_estimate("hlcmo.yaml", households, "building_id",
                              buildings, nodes)


@sim.model('hlcmo_simulate')
def hlcmo_simulate(households, buildings, nodes):
    return utils.lcm_simulate("hlcmo.yaml", households, buildings, nodes,
                              "building_id", "residential_units",
                              "vacant_residential_units")


@sim.model('hlcmr_estimate')
def hlcmr_estimate(households, buildings, nodes):
    return utils.lcm_estimate("hlcmr.yaml", households, "building_id",
                              buildings, nodes)


@sim.model('hlcmr_simulate')
def hlcmr_simulate(households, buildings, nodes):
    return utils.lcm_simulate("hlcmr.yaml", households, buildings, nodes,
                              "building_id", "residential_units",
                              "vacant_residential_units")


@sim.model('elcm_estimate')
def elcm_estimate(jobs, buildings, nodes):
    return utils.lcm_estimate("elcm.yaml", jobs, "building_id",
                              buildings, nodes)


@sim.model('elcm_simulate')
def elcm_simulate(jobs, buildings, nodes):
    return utils.lcm_simulate("elcm.yaml", jobs, buildings, nodes,
                              "building_id", "job_spaces",
                              "vacant_job_spaces")


@sim.model('households_relocation')
def households_relocation(households):
    return utils.simple_relocation(households, .05, "building_id")


@sim.model('jobs_relocation')
def jobs_relocation(jobs):
    return utils.simple_relocation(jobs, .05, "building_id")


@sim.model('households_transition')
def households_transition(households):
    return utils.simple_transition(households, .05, "building_id")


@sim.model('jobs_transition')
def jobs_transition(jobs):
    return utils.simple_transition(jobs, .05, "building_id")


@sim.model('build_networks')
def build_networks():
    st = pd.HDFStore(os.path.join(misc.data_dir(), "osm_bayarea.h5"), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])
    net.precompute(2000)
    sim.add_injectable("net", net)


@sim.model('neighborhood_vars')
def neighborhood_vars(net):
    nodes = networks.from_yaml(net, "networks.yaml")
    print nodes.describe()
    print pd.Series(nodes.index).describe()
    sim.add_table("nodes", nodes)


@sim.model('price_vars')
def price_vars(net):
    nodes = networks.from_yaml(net, "networks2.yaml")
    print nodes.describe()
    print pd.Series(nodes.index).describe()
    sim.add_table("nodes_prices", nodes)


@sim.model('feasibility')
def feasibility(parcels):
    utils.run_feasibility(parcels,
                          variables.parcel_average_price,
                          variables.parcel_is_allowed,
                          residential_to_yearly=True)


def random_type(form):
    form_to_btype = sim.get_injectable("form_to_btype")
    return random.choice(form_to_btype[form])


def add_extra_columns(df):
    for col in ["residential_sales_price", "residential_rent",
                "non_residential_rent"]:
        df[col] = 0
    return df


@sim.model('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year):
    utils.run_developer("residential",
                        households,
                        buildings,
                        "residential_units",
                        parcels.parcel_size,
                        parcels.ave_unit_size,
                        parcels.total_units,
                        feasibility,
                        year=year,
                        target_vacancy=.08,
                        form_to_btype_callback=random_type,
                        add_more_columns_callback=add_extra_columns,
                        bldg_sqft_per_job=400.0)


@sim.model('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year):
    utils.run_developer(["office", "retail", "industrial"],
                        jobs,
                        buildings,
                        "job_spaces",
                        parcels.parcel_size,
                        parcels.ave_unit_size,
                        parcels.total_job_spaces,
                        feasibility,
                        year=year,
                        target_vacancy=.08,
                        form_to_btype_callback=random_type,
                        add_more_columns_callback=add_extra_columns,
                        residential=False,
                        bldg_sqft_per_job=400.0)


@sim.model("travel_model_output")
def travel_model_output(households, jobs, buildings, zones):
    households = households.to_frame()
    jobs = jobs.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()

    zones['tothh'] = households.\
        groupby('zone_id').size()
    zones['hhpop'] = households.\
        groupby('zone_id').persons.sum()

    #    totpop

    #    empres

    zones['sfdu'] = \
        households.query("building_type_id == 1 | building_type_id == 2").\
        groupby('zone_id').size()
    zones['mfdu'] = \
        households.query("building_type_id == 3 | building_type_id == 12").\
        groupby('zone_id').size()

    zones['hhincq1'] = households.query("income < 25000").\
        groupby('zone_id').size()
    zones['hhincq2'] = households.query("income >= 25000 and income < 45000").\
        groupby('zone_id').size()
    zones['hhincq3'] = households.query("income >= 45000 and income < 75000").\
        groupby('zone_id').size()
    zones['hhincq4'] = households.query("income >= 75000").\
        groupby('zone_id').size()

    # attempting to get at total zonal developed acres
    zones['NEWdevacres'] = \
        (buildings.query("building_sqft > 0").
         groupby('zone_id').unit_lot_size.sum()) / 43560

    zones['totemp'] = jobs.\
        groupby('zone_id').size()
    zones['agrempn'] = jobs.query("empsix == 'AGREMPN'").\
        groupby('zone_id').size()
    zones['mwtempn'] = jobs.query("empsix == 'MWTEMPN'").\
        groupby('zone_id').size()
    zones['retempn'] = jobs.query("empsix == 'RETEMPN'").\
        groupby('zone_id').size()
    zones['fpsempn'] = jobs.query("empsix == 'FPSEMPN'").\
        groupby('zone_id').size()
    zones['herempn'] = jobs.query("empsix == 'HEREMPN'").\
        groupby('zone_id').size()
    zones['othempn'] = jobs.query("empsix == 'OTHEMPN'").\
        groupby('zone_id').size()

    sim.add_table("travel_model_output", zones)


@sim.model("clear_cache")
def clear_cache():
    sim.clear_cache()

from urbansim.developer import developer
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim.utils import networks
from urbansim.models import transition
import os
import random
import utils
import time
import dataset
import variables
import pandana as pdna
import pandas as pd
import numpy as np


@sim.model('rsh_estimate')
def rsh_estimate(homesales, nodes, logsums):
    return utils.hedonic_estimate("rsh.yaml", homesales, [nodes, logsums])


@sim.model('rsh_simulate')
def rsh_simulate(buildings, nodes, logsums):
    return utils.hedonic_simulate("rsh.yaml", buildings, [nodes, logsums],
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
def households_transition(households, household_controls, year):
    ct = household_controls.to_frame()
    hh = households.to_frame(households.local_columns+['income_quartile'])
    print "Total households before transition: {}".format(len(hh))
    tran = transition.TabularTotalsTransition(ct, 'total_number_of_households')
    model = transition.TransitionModel(tran)
    new, added_hh_idx, new_linked = model.transition(hh, year)
    new.loc[added_hh_idx, "building_id"] = -1
    print "Total households after transition: {}".format(len(new))
    sim.add_table("households", new)


@sim.model('simple_households_transition')
def simple_households_transition(households):
    return utils.simple_transition(households, .05, "building_id")


@sim.model('jobs_transition')
def jobs_transition(jobs, employment_controls, year):
    ct = employment_controls.to_frame()
    hh = jobs.to_frame(jobs.local_columns+['empsix_id'])
    print "Total jobs before transition: {}".format(len(hh))
    tran = transition.TabularTotalsTransition(ct, 'number_of_jobs')
    model = transition.TransitionModel(tran)
    new, added_hh_idx, new_linked = model.transition(hh, year)
    new.loc[added_hh_idx, "building_id"] = -1
    print "Total jobs after transition: {}".format(len(new))
    sim.add_table("jobs", new)


@sim.model('simple_jobs_transition')
def jobs_transition(jobs):
    return utils.simple_transition(jobs, .015, "building_id")


@sim.model('build_networks')
def build_networks():
    st = pd.HDFStore(os.path.join(misc.data_dir(), "osm_bayarea.h5"), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])
    net.precompute(1500)
    sim.add_injectable("net", net)


@sim.model('neighborhood_vars')
def neighborhood_vars(net):
    nodes = networks.from_yaml(net, "networks.yaml")
    print nodes.describe()
    sim.add_table("nodes", nodes)


@sim.model('price_vars')
def price_vars(net):
    nodes = networks.from_yaml(net, "networks2.yaml")
    print nodes.describe()
    sim.add_table("nodes_prices", nodes)


@sim.model('feasibility')
def feasibility(parcels):
    utils.run_feasibility(parcels,
                          variables.parcel_average_price,
                          variables.parcel_is_allowed,
                          historic_preservation='oldest_building > 1940 and '
                                                'oldest_building < 2000',
                          residential_to_yearly=True,
                          pass_through=["oldest_building", "total_sqft",
                                        "max_far", "max_dua", "land_cost",
                                        "residential", "min_max_fars",
                                        "max_far_from_dua", "max_height",
                                        "max_far_from_heights",
                                        "building_purchase_price",
                                        "building_purchase_price_sqft"])


def add_extra_columns(df):
    for col in ["residential_sales_price", "residential_rent",
                "non_residential_rent"]:
        df[col] = 0
    return df


@sim.model('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year):
    new_buildings = utils.run_developer(
        "residential",
        households,
        buildings,
        "residential_units",
        parcels.parcel_size,
        parcels.ave_unit_size,
        parcels.total_units,
        feasibility,
        year=year,
        target_vacancy=.13,
        min_unit_size=1000,
        form_to_btype_callback=sim.get_injectable("form_to_btype_f"),
        add_more_columns_callback=add_extra_columns,
        bldg_sqft_per_job=400.0)

    utils.add_parcel_output(new_buildings)


@sim.model('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year):
    new_buildings = utils.run_developer(
        ["office", "retail", "industrial"],
        jobs,
        buildings,
        "job_spaces",
        parcels.parcel_size,
        parcels.ave_unit_size,
        parcels.total_job_spaces,
        feasibility,
        year=year,
        target_vacancy=.49,
        form_to_btype_callback=sim.get_injectable("form_to_btype_f"),
        add_more_columns_callback=add_extra_columns,
        residential=False,
        bldg_sqft_per_job=400.0)

    utils.add_parcel_output(new_buildings)


@sim.model("diagnostic_output")
def diagnostic_output(households, homesales, buildings, zones, year):
    households = households.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()
    homesales = homesales.to_frame()

    zones['residential_units'] = buildings.groupby('zone_id').\
        residential_units.sum()
    zones['non_residential_sqft'] = buildings.groupby('zone_id').\
        non_residential_sqft.sum()

    zones['retail_sqft'] = buildings.query('general_type == "Retail"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['office_sqft'] = buildings.query('general_type == "Office"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['industrial_sqft'] = buildings.query('general_type == "Industrial"').\
        groupby('zone_id').non_residential_sqft.sum()

    zones['average_income'] = households.groupby('zone_id').income.quantile()
    zones['household_size'] = households.groupby('zone_id').persons.quantile()

    zones['empirical_res_price'] = homesales.groupby('zone_id').\
        sale_price_flt.quantile()

    zones['residential_sales_price'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').\
        residential_sales_price.quantile()
    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['industrial_rent'] = \
        buildings[buildings.general_type == "Industrial"].\
        groupby('zone_id').non_residential_rent.quantile()

    utils.add_simulation_output(zones, "diagnostic_outputs", year)


@sim.model("travel_model_output")
def travel_model_output(households, jobs, buildings, zones, year):
    households = households.to_frame()
    jobs = jobs.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()

    zones['tothh'] = households.\
        groupby('zone_id').size()
    zones['hhpop'] = households.\
        groupby('zone_id').persons.sum()

    zones['sfdu'] = \
        buildings.query("building_type_id == 1 or building_type_id == 2").\
        groupby('zone_id').residential_units.sum()
    zones['mfdu'] = \
        buildings.query("building_type_id == 3 or building_type_id == 12").\
        groupby('zone_id').residential_units.sum()

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

    sim.add_table("travel_model_output", zones, year)

    utils.add_simulation_output(zones, "travel_model_outputs", year)
    utils.write_simulation_output(os.path.join(misc.runs_dir(),
                                               "run{}_simulation_output.json"))
    utils.write_parcel_output(os.path.join(misc.runs_dir(),
                                           "run{}_parcel_output.csv"))


@sim.model("clear_cache")
def clear_cache():
    sim.clear_cache()


# this method is used to push messages from urbansim to websites for live
# exploration of simulation results
@sim.model("pusher")
def pusher(year, run_number, uuid):
    try:
        import pusher
    except:
        # if pusher not installed, just return
        return
    import socket

    p = pusher.Pusher(
        app_id='90082',
        key='2fb2b9562f4629e7e87c',
        secret='2f0a7b794ec38d16d149'
    )
    host = "http://localhost:8765/"
    sim_output = host+"runs/run{}_simulation_output.json".format(run_number)
    parcel_output = host+"runs/run{}_parcel_output.csv".format(run_number)
    p['urbansim'].trigger('simulation_year_completed',
                          {'year': year,
                           'region': 'bayarea',
                           'run_number': run_number,
                           'hostname': socket.gethostname(),
                           'uuid': uuid,
                           'time': time.ctime(),
                           'sim_output': sim_output,
                           'field_name': 'residential_units',
                           'table': 'diagnostic_outputs',
                           'scale': 'jenks',
                           'parcel_output': parcel_output})


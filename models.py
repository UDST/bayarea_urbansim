from urbansim.developer import sqftproforma, developer
from urbansim.utils import networks
import urbansim.sim.simulation as sim
import urbansim.sim.yamlmodelrunner as ymr
from urbansim.utils import misc
import os
import random
import variables
import dataset
import pandas as pd
import numpy as np


def merge_nodes(df):
    return pd.merge(df.to_frame(),
                    sim.get_table("nodes").to_frame(),
                    left_on="_node_id",
                    right_index=True,
                    how="left")


def random_type(form):
    return random.choice(dataset.FORM_TO_BTYPE[form])


@sim.model('rsh_estimate')
def rsh_estimate(homesales):
    return ymr.hedonic_estimate(merge_nodes(homesales), "rsh.yaml")


@sim.model('rsh_simulate')
def rsh_simulate(buildings):
    return ymr.hedonic_simulate(merge_nodes(buildings), "rsh.yaml",
                                "buildings", "residential_sales_price")


@sim.model('rrh_estimate')
def rrh_estimate(apartments):
    return ymr.hedonic_estimate(merge_nodes(apartments), "rrh.yaml")


@sim.model('rrh_simulate')
def rrh_simulate(buildings):
    return ymr.hedonic_simulate(merge_nodes(buildings), "rrh.yaml",
                                "buildings", "residential_rent")


@sim.model('nrh_estimate')
def nrh_estimate(costar):
    return ymr.hedonic_estimate(merge_nodes(costar), "nrh.yaml")


@sim.model('nrh_simulate')
def nrh_simulate(buildings):
    return ymr.hedonic_simulate(merge_nodes(buildings), "nrh.yaml",
                                "buildings", "non_residential_rent")


def _hlcm_estimate(households, buildings, cfgname):
    return ymr.lcm_estimate(households,
                            "building_id",
                            merge_nodes(buildings),
                            cfgname)


def _hlcm_simulate(households, buildings, cfgname):
    units = ymr.get_vacant_units(households,
                                 "building_id",
                                 merge_nodes(buildings),
                                 "residential_units")
    return ymr.lcm_simulate(households, units, cfgname, "households", "building_id")


@sim.model('hlcmo_estimate')
def hlcmo_estimate(households, buildings):
    return _hlcm_estimate(households, buildings, "hlcmo.yaml")


@sim.model('hlcmo_simulate')
def hlcmo_simulate(households, buildings):
    return _hlcm_simulate(households, buildings, "hlcmo.yaml")


@sim.model('hlcmr_estimate')
def hlcmr_estimate(households, buildings):
    return _hlcm_estimate(households, buildings, "hlcmr.yaml")


@sim.model('hlcmr_simulate')
def hlcmr_simulate(households, buildings):
    return _hlcm_simulate(households, buildings, "hlcmr.yaml")


@sim.model('elcm_estimate')
def elcm_estimate(jobs, buildings):
    return ymr.lcm_estimate(jobs,
                            "building_id",
                            merge_nodes(buildings),
                            "elcm.yaml")


@sim.model('elcm_simulate')
def elcm_simulate(jobs, buildings):
    units = ymr.get_vacant_units(jobs,
                                 "building_id",
                                 merge_nodes(buildings),
                                 "non_residential_units")
    return ymr.lcm_simulate(jobs, units, "elcm.yaml", "jobs", "building_id")


@sim.model('households_relocation')
def households_relocation():
    return ymr.simple_relocation("households", .05)


@sim.model('jobs_relocation')
def jobs_relocation():
    return ymr.simple_relocation("jobs", .05)


@sim.model('households_transition')
def households_transition():
    return ymr.simple_transition("households", .05)


@sim.model('jobs_transition')
def jobs_transition():
    return ymr.simple_transition("jobs", .05)


@sim.model('build_networks')
def build_networks():
    if networks.NETWORKS is None:
        networks.NETWORKS = networks.Networks(
            [os.path.join(misc.data_dir(), x) for x in ['osm_bayarea.jar']],
            factors=[1.0],
            maxdistances=[2000],
            twoway=[1],
            impedances=None)


@sim.model('neighborhood_vars')
def neighborhood_vars():
    nodes = networks.from_yaml("networks.yaml")
    sim.add_table("nodes", nodes)


@sim.model('price_vars')
def price_vars():
    nodes = networks.from_yaml("networks2.yaml")
    sim.add_table("nodes_prices", nodes)


@sim.model('feasibility')
def feasibility(parcels):
    pf = sqftproforma.SqFtProForma()

    df = parcels.to_frame()

    # add prices for each use
    for use in pf.config.uses:
        df[use] = variables.parcel_average_price(use)

    # convert from cost to yearly rent
    df["residential"] *= pf.config.cap_rate
    print df[pf.config.uses].describe()

    d = {}
    for form in pf.config.forms:
        print "Computing feasibility for form %s" % form
        d[form] = pf.lookup(form, df[variables.parcel_is_allowed(form)])

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)

    sim.add_table("feasibility", far_predictions)


@sim.model('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year):
    residential_target_vacancy = .15
    dev = developer.Developer(feasibility.to_frame())

    target_units = dev.compute_units_to_build(len(households),
                                              buildings.residential_units.sum(),
                                              residential_target_vacancy)

    new_buildings = dev.pick("residential",
                             target_units,
                             parcels.parcel_size,
                             parcels.ave_unit_sqft,
                             parcels.total_units,
                             max_parcel_size=200000,
                             drop_after_build=True)

    new_buildings["year_built"] = year
    new_buildings["form"] = "residential"
    new_buildings["building_type_id"] = new_buildings["form"].apply(random_type)
    new_buildings["stories"] = new_buildings.stories.apply(np.ceil)
    for col in ["residential_sales_price", "residential_rent", "non_residential_rent"]:
        new_buildings[col] = np.nan

    print "Adding {} buildings with {:,} residential units".format(len(new_buildings),
                                                                   new_buildings.residential_units.sum())

    all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                              new_buildings[buildings.local_columns])
    sim.add_table("buildings", all_buildings)


@sim.model('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year):
    non_residential_target_vacancy = .15
    dev = developer.Developer(feasibility.to_frame())

    target_units = dev.compute_units_to_build(len(jobs),
                                              buildings.non_residential_units.sum(),
                                              non_residential_target_vacancy)

    new_buildings = dev.pick(["office", "retail", "industrial"],
                             target_units,
                             parcels.parcel_size,
                             # This is hard-coding 500 as the average sqft per job
                             # which isn't right but it doesn't affect outcomes much
                             # developer will build enough units assuming 500 sqft
                             # per job but then it just returns the result as square
                             # footage and the actual building_sqft_per_job will be
                             # used to compute non_residential_units.  In other words,
                             # we can over- or under- build the number of units here
                             # but we should still get roughly the right amount of
                             # development out of this and the final numbers are precise.
                             # just move this up and down if dev is over- or under-
                             # buildings things
                             pd.Series(500, index=parcels.index),
                             parcels.total_nonres_units,
                             max_parcel_size=200000,
                             drop_after_build=True,
                             residential=False)

    new_buildings["year_built"] = year
    new_buildings["building_type_id"] = new_buildings["form"].apply(random_type)
    new_buildings["residential_units"] = 0
    new_buildings["stories"] = new_buildings.stories.apply(np.ceil)
    for col in ["residential_sales_price", "residential_rent", "non_residential_rent"]:
        new_buildings[col] = np.nan

    print "Adding {} buildings with {:,} non-residential sqft".format(len(new_buildings),
                                                                      new_buildings.non_residential_sqft.sum())

    all_buildings = dev.merge(buildings.to_frame(buildings.local_columns),
                              new_buildings[buildings.local_columns])
    sim.add_table("buildings", all_buildings)


@sim.model("write_output")
def write_output():
    fname = os.path.join(misc.runs_dir(), "run%d.h5" % misc.get_run_number())
    tblnames = ["buildings", "households", "jobs", "parcels"]

    store = pd.HDFStore(fname, "w")
    for tblname in tblnames:
        tbl = sim.get_table(tblname)
        store[tblname] = tbl.to_frame(tbl.local_columns)
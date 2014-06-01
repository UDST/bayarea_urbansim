from urbansim.developer import sqftproforma
from urbansim.utils import networks
import urbansim.models.yamlmodelrunner as ymr
from dataset import *
import time


# residential sales hedonic
def rsh_estimate(dset):
    df = dset.merge_nodes(HomeSales(dset).build_df())
    return ymr.hedonic_estimate(df, "rsh.yaml")


def rsh_simulate(dset):
    df = dset.merge_nodes(Buildings(dset, addprices=False).build_df())
    return ymr.hedonic_simulate(df, "rsh.yaml", dset.buildings, "residential_sales_price")


# residential rent hedonic
def rrh_estimate(dset):
    df = dset.merge_nodes(Apartments(dset).build_df())
    return ymr.hedonic_estimate(df, "rrh.yaml")


def rrh_simulate(dset):
    df = dset.merge_nodes(Buildings(dset, addprices=False).build_df())
    return ymr.hedonic_simulate(df, "rrh.yaml", dset.buildings, "residential_rent")


# non-residential hedonic
def nrh_estimate(dset):
    df = dset.merge_nodes(CoStar(dset).build_df())
    return ymr.hedonic_estimate(df, "nrh.yaml")


def nrh_simulate(dset):
    df = dset.merge_nodes(Buildings(dset, addprices=False).build_df())
    return ymr.hedonic_simulate(df, "nrh.yaml", dset.buildings, "non_residential_rent")


# household location choice
def _hlcm_estimate(dset, cfgname):
    buildings = dset.merge_nodes(Buildings(dset).build_df().query("general_type == 'Residential'"))
    return ymr.lcm_estimate(Households(dset).build_df(), "building_id", buildings, cfgname)


def _hlcm_simulate(dset, cfgname):
    households = Households(dset).build_df()
    buildings = dset.merge_nodes(Buildings(dset).build_df().query("general_type == 'Residential'"))
    units = ymr.get_vacant_units(households, "building_id", buildings, "residential_units")
    return ymr.lcm_simulate(households, units, cfgname, dset.households, "building_id")


# household location choice owner
def hlcmo_estimate(dset):
    return _hlcm_estimate(dset, "hlcmo.yaml")


def hlcmo_simulate(dset):
    return _hlcm_simulate(dset, "hlcmo.yaml")


# household location choice renter
def hlcmr_estimate(dset):
    return _hlcm_estimate(dset, "hlcmr.yaml")


def hlcmr_simulate(dset):
    return _hlcm_simulate(dset, "hlcmr.yaml")


# employment location choice
def elcm_estimate(dset):
    buildings = dset.merge_nodes(Buildings(dset).build_df().query("general_type != 'Residential'"))
    return ymr.lcm_estimate(Jobs(dset).build_df(), "building_id", buildings, "elcm.yaml")


def elcm_simulate(dset):
    jobs = Jobs(dset).build_df()
    buildings = dset.merge_nodes(Buildings(dset).build_df().query("general_type != 'Residential'"))
    units = ymr.get_vacant_units(jobs, "building_id", buildings, "non_residential_units")
    return ymr.lcm_simulate(jobs, units, "elcm.yaml", dset.jobs, "building_id")


def households_relocation(dset):
    return ymr.simple_relocation(dset.households, .05)


def jobs_relocation(dset):
    return ymr.simple_relocation(dset.jobs, .08)


def households_transition(dset):
    return ymr.simple_transition(dset, "households", .05)


def jobs_transition(dset):
    return ymr.simple_transition(dset, "jobs", .05)


def build_networks():
    if not networks.NETWORKS:
        networks.NETWORKS = networks.Networks(
            [os.path.join(misc.data_dir(), x) for x in ['osm_bayarea.jar']],
            factors=[1.0],
            maxdistances=[2000],
            twoway=[1],
            impedances=None)


def _update_xys(dset):
    for dfname in ["households", "jobs"]:
        print "Adding xys to dataframe: %s" % dfname
        df = dset.add_xy(dset.fetch(dfname))
        dset.save_tmptbl(dfname, df)


def neighborhood_vars(dset):
    _update_xys(dset)
    nodes = networks.from_yaml(dset, "networks.yaml")
    dset.save_tmptbl("nodes", nodes)


def price_vars(dset):
    nodes = networks.from_yaml(dset, "networks2.yaml")
    dset.save_tmptbl("nodes_prices", nodes)


def feasibility(dset):
    dev = sqftproforma.SqFtProForma()

    parcels = Parcels(dset)
    df = parcels.build_df()

    # add prices for each use
    for use in dev.config.uses:
        df[use] = parcels.price(use)

    # convert from cost to yearly rent
    df["residential"] *= dev.config.cap_rate

    d = {}
    for form in dev.config.forms:
        print "Computing feasibility for form %s" % form
        d[form] = dev.lookup(form, df[parcels.allowed(form)])

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)

    dset.save_tmptbl("feasibility", far_predictions)


def developer(dset):
    type_d = {
        'residential': [1, 2, 3],
        'industrial': [7, 8, 9],
        'retail': [10, 11],
        'office': [4],
        'mixedresidential': [12],
        'mixedoffice': [14],
    }
    pass


def _run_models(dset, model_list, years):

    for year in years:

        dset.year = year

        t1 = time.time()

        for model in model_list:
            t2 = time.time()
            print "\n" + model + "\n"
            globals()[model](dset)
            print "Model %s executed in %.3fs" % (model, time.time()-t2)
        print "Year %d completed in %.3fs" % (year, time.time()-t1)
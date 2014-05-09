import pandas as pd
import numpy as np
import os
from urbansim.utils import misc, networks
from urbansim.models import RegressionModel, MNLLocationChoiceModel, util


# hedonic methods
def _hedonic_estimate(dset, df, cfgname):
    print "Running hedonic estimation\n"
    cfg = misc.config(cfgname)
    buildings = pd.merge(df, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    print hm.fit(buildings).summary()
    hm.to_yaml(str_or_buffer=cfg)


def _hedonic_simulate(dset, df, cfgname, outfname):
    print "Running hedonic simulation\n"
    cfg = misc.config(cfgname)
    buildings = pd.merge(df, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    price_or_rent = hm.predict(buildings)
    print price_or_rent.describe()
    dset.buildings[outfname] = price_or_rent.reindex(dset.buildings.index)


# residential sales hedonic
def rsh_estimate(dset):
    return _hedonic_estimate(dset, dset.homesales, "rsh.yaml")


def rsh_simulate(dset):
    df = dset.building_filter(residential=1)
    return _hedonic_simulate(dset, df, "rsh.yaml", "res_sales_price")


# residential rent hedonic
def rrh_estimate(dset):
    return _hedonic_estimate(dset, dset.apartments, "rrh.yaml")


def rrh_simulate(dset):
    df = dset.building_filter(residential=1)
    return _hedonic_simulate(dset, df, "rrh.yaml", "residential_rent")


# non-residential hedonic
def nrh_estimate(dset):
    return _hedonic_estimate(dset, dset.costar, "nrh.yaml")


def nrh_simulate(dset):
    df = dset.building_filter(residential=0)
    return _hedonic_simulate(dset, df, "nrh.yaml", "nonresidential_rent")


# location choice models
def _lcm_estimate(dset, choosers, chosen_fname, alternatives, cfgname):
    print "Running location choice model estimation\n"
    cfg = misc.config(cfgname)
    buildings = pd.merge(alternatives, dset.nodes,
                         left_on="_node_id", right_index=True)
    lcm = MNLLocationChoiceModel.from_yaml(str_or_buffer=cfg)
    lcm.fit(choosers, buildings, choosers[chosen_fname])
    lcm.report_fit()
    lcm.to_yaml(str_or_buffer=cfg)


def _get_vacant_units(choosers, buildings, supply_fname, vacant_units):
    print "There are %d total available units" % buildings[supply_fname].sum()
    print "    and %d total choosers" % len(choosers.index)
    print "    but there are %d overfull buildings" % \
        len(vacant_units[vacant_units < 0].index)
    vacant_units = vacant_units[vacant_units > 0]
    alternatives = buildings.loc[np.repeat(vacant_units.index,
                                 vacant_units.values.astype('int'))] \
        .reset_index()
    print "    for a total of %d empty units" % vacant_units.sum()
    print "    in %d buildings total in the region" % len(vacant_units)
    return alternatives


def _print_number_unplaced(df, fieldname="building_id"):
    counts = df[fieldname].isnull().value_counts()
    count = 0 if True not in counts else counts[True]
    print "Total currently unplaced: %d" % count


def _lcm_simulate(dset, choosers, output_fname, alternatives,
                  supply_fname, cfgname):
    print "Running location choice model simulation\n"
    cfg = misc.config(cfgname)
    buildings = pd.merge(alternatives, dset.nodes,
                         left_on="_node_id", right_index=True)
    vacant_units = buildings[supply_fname].sub(
        choosers.groupby(output_fname).size(), fill_value=0)
    alternatives = _get_vacant_units(choosers, buildings, supply_fname,
                                     vacant_units)
    lcm = MNLLocationChoiceModel.from_yaml(str_or_buffer=cfg)
    new_units = lcm.predict(choosers, alternatives)
    print "Assigned %d choosers to new units" % len(new_units.index)
    choosers[output_fname].loc[new_units.index] = \
        alternatives.loc[new_units.values][output_fname].values
    _print_number_unplaced(choosers, output_fname)


def _hlcm_estimate(dset, cfgname):
    return _lcm_estimate(dset, dset.households, "building_id",
                         dset.building_filter(residential=1),
                         cfgname)


def _hlcm_simulate(dset, cfgname):
    return _lcm_simulate(dset, dset.households, "building_id",
                         dset.building_filter(residential=1),
                         "residential_units",
                         cfgname)


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
    return _lcm_estimate(dset, dset.jobs, "building_id",
                         dset.building_filter(residential=0),
                         "elcm.yaml")


def elcm_simulate(dset):
    return _lcm_simulate(dset, dset.jobs, "building_id",
                         dset.building_filter(residential=0),
                         "non_residential_units",
                         "elcm.yaml")


# relocation model implementation
def _simple_relocation(choosers, relocation_rate, fieldname='building_id'):
    print "Running relocation\n"
    _print_number_unplaced(choosers, fieldname)
    chooser_ids = np.random.choice(choosers.index, size=relocation_rate *
                                   len(choosers.index), replace=False)
    choosers[fieldname].loc[chooser_ids] = np.nan
    _print_number_unplaced(choosers, fieldname)


def household_relocation(dset):
    return _simple_relocation(dset.households, .05)


def jobs_relocation(dset):
    return _simple_relocation(dset.jobs, .08)


def build_networks():
    if not networks.NETWORKS:
        networks.NETWORKS = networks.Networks(
            [os.path.join(misc.data_dir(), x) for x in ['osm_bayarea.jar']],
            factors=[1.0],
            maxdistances=[2000],
            twoway=[1],
            impedances=None)


def update_xys(dset):
    for dfname in ["households", "jobs"]:
        print "Adding xys to dataframe: %s" % dfname
        df = dset.add_xy(dset.fetch(dfname))
        dset.save_tmptbl(dfname, df)


def accessibility_variables(dset, cfgname):
    print "Computing accessibility variables"
    import yaml
    cfg = yaml.load(open(misc.config(cfgname)))

    nodes = pd.DataFrame(index=networks.NETWORKS.external_nodeids)

    for variable in cfg['variable_definitions']:

        name = variable["name"]
        print "Computing %s" % name

        decay = {
            "exponential": "DECAY_EXP",
            "linear": "DECAY_LINEAR",
            "flat": "DECAY_FLAT"
        }.get(variable.get("decay", "linear"))

        agg = {
            "sum": "AGG_SUM",
            "average": "AGG_AVE",
            "stddev": "AGG_STDDEV"
        }.get(variable.get("aggregation", "sum"))

        vname = variable.get("varname", None)

        radius = variable["radius"]

        dfname = variable["dataframe"]
        df = dset.fetch(dfname)

        if "filters" in variable:
            util.apply_filter_query(df, variable["filters"])

        print "    dataframe = %s, varname=%s" % (dfname, vname)
        print "    radius = %s, aggregation = %s, decay = %s" % (radius, agg, decay)

        nodes[name] = networks.NETWORKS.accvar(df,
                                               radius,
                                               agg=agg,
                                               decay=decay,
                                               vname=vname).astype('float').values

        if "apply" in variable:
            nodes[name] = nodes[name].apply(eval(variable["apply"]))


def networks(dset):
    return accessibility_variables(dset, "networks.yaml")


def networks2(dset):
    return accessibility_variables(dset, "networks2.yaml")
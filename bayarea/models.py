import pandas as pd
import numpy as np
import os

from dataset import *
from urbansim.utils import misc, networks
from urbansim.models import RegressionModel, MNLLocationChoiceModel, \
    GrowthRateTransition


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
    return _hedonic_estimate(dset, HomeSales(dset).build_df(), "rsh.yaml")


def rsh_simulate(dset):
    return _hedonic_simulate(dset, Buildings(dset).build_df(), "rsh.yaml",
                             "residential_sales_price")


# residential rent hedonic
def rrh_estimate(dset):
    return _hedonic_estimate(dset, Apartments(dset).build_df(), "rrh.yaml")


def rrh_simulate(dset):
    return _hedonic_simulate(dset, Buildings(dset).build_df(), "rrh.yaml",
                             "residential_rent")


# non-residential hedonic
def nrh_estimate(dset):
    return _hedonic_estimate(dset, CoStar(dset).build_df(), "nrh.yaml")


def nrh_simulate(dset):
    return _hedonic_simulate(dset, Buildings(dset).build_df(), "nrh.yaml",
                             "non_residential_rent")


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
    movers = choosers[choosers[output_fname].isnull()]
    new_units = lcm.predict(movers, alternatives)
    print "Assigned %d choosers to new units" % len(new_units.index)
    choosers[output_fname].loc[new_units.index] = \
        alternatives.loc[new_units.values][output_fname].values
    _print_number_unplaced(choosers, output_fname)


def _hlcm_estimate(dset, cfgname):
    return _lcm_estimate(dset, Households(dset).build_df(), "building_id",
                         Buildings(dset).build_df().query("general_type == 'Residential'"),
                         cfgname)


def _hlcm_simulate(dset, cfgname):
    return _lcm_simulate(dset, Households(dset).build_df(), "building_id",
                         Buildings(dset).build_df().query("general_type == 'Residential'"),
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
    return _lcm_estimate(dset, Jobs(dset).build_df(), "building_id",
                         Buildings(dset).build_df().query("general_type != 'Residential'"),
                         "elcm.yaml")


def elcm_simulate(dset):
    return _lcm_simulate(dset, Jobs(dset).build_df(), "building_id",
                         Buildings(dset).build_df().query("general_type != 'Residential'"),
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


def households_relocation(dset):
    return _simple_relocation(dset.households, .05)


def jobs_relocation(dset):
    return _simple_relocation(dset.jobs, .08)


def _simple_transition(dset, dfname, rate):
    transition = GrowthRateTransition(rate)
    df = dset.fetch(dfname)
    print "%d agents before transition" % len(df.index)
    df, added, copied, removed = transition.transition(df, None)
    print "%d agents after transition" % len(df.index)
    dset.save_tmptbl(dfname, df)


def households_transition(dset):
    return _simple_transition(dset, "households", .05)


def jobs_transition(dset):
    return _simple_transition(dset, "jobs", .05)


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
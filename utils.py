from urbansim.models import RegressionModel, SegmentedRegressionModel, \
    MNLLocationChoiceModel, SegmentedMNLLocationChoiceModel, GrowthRateTransition
import numpy as np
import pandas as pd
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim.sim.simulation import TableSourceWrapper


def enable_logging():
    from urbansim.utils import logutil
    logutil.set_log_level(logutil.logging.INFO)
    logutil.log_to_stream()


def deal_with_nas(df):
    lenbefore = len(df)
    df = df.dropna(how='any')
    lenafter = len(df)
    if lenafter != lenbefore:
        print "Dropped %d rows because they contained nans" % (lenbefore-lenafter)
    return df


def maybe_convert(df):
    if isinstance(df, TableSourceWrapper):
        return df.convert()
    else:
        return df


def merge_nodes(tbl, nodes, cfg):
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    tbl = maybe_convert(tbl)
    nodes = maybe_convert(nodes)
    columns = misc.column_map([tbl, nodes], cfg.columns_used(), onlyfound=True)
    df = sim.merge_tables(target=tbl.name, tables=[tbl, nodes], columns=columns)
    df = deal_with_nas(df)
    return df


def choosers_columns_used(choosers, chosen_fname, cfg):
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    # cache the tables to get column names
    choosers = maybe_convert(choosers)
    columns = misc.column_map([choosers], cfg.columns_used(), onlyfound=True)
    columns.append(chosen_fname)
    return choosers.to_frame(columns)


def _print_number_unplaced(df, fieldname="building_id"):
    counts = (df[fieldname] == -1).value_counts()
    count = 0 if True not in counts else counts[True]
    print "Total currently unplaced: %d" % count


def yaml_to_class(cfg):
    import yaml
    model_type = yaml.load(open(cfg))["model_type"]
    return {
        "regression": RegressionModel,
        "segmented_regression": SegmentedRegressionModel,
        "locationchoice": MNLLocationChoiceModel,
        "segmented_locationchoice": SegmentedMNLLocationChoiceModel
    }[model_type]


def hedonic_estimate(cfg, tbl, nodes):
    cfg = misc.config(cfg)
    df = merge_nodes(tbl, nodes, cfg)
    return yaml_to_class(cfg).fit_from_cfg(df, cfg)


def hedonic_simulate(cfg, tbl, nodes, out_dfname, out_fname):
    cfg = misc.config(cfg)
    df = merge_nodes(tbl, nodes, cfg)
    price_or_rent = yaml_to_class(cfg).predict_from_cfg(df, cfg)
    sim.partial_update(price_or_rent, out_dfname, out_fname)


def lcm_estimate(cfg, choosers, chosen_fname, buildings, nodes):
    cfg = misc.config(cfg)
    choosers = choosers_columns_used(choosers, chosen_fname, cfg)
    choosers = deal_with_nas(choosers)
    alternatives = merge_nodes(buildings, nodes, cfg)
    return yaml_to_class(cfg).fit_from_cfg(choosers, chosen_fname, alternatives, cfg)


def get_vacant_units(choosers, location_fname, locations, supply_fname):
    """
    This is a bit of a nuanced method for this skeleton which computes
    the vacant units from a building dataset for both households and jobs.

    Parameters
    ----------
    choosers : DataFrame
        A dataframe of agents doing the choosing.
    location_fname : string
        A string indicating a column in choosers which indicates the locations
        from the locations dataframe that these agents are located in.
    locations : DataFrame
        A dataframe of locations which the choosers are location in and which
        have a supply.
    supply_fname : string
        A string indicating a column in locations which is an integer value
        representing the number of agents that can be located at that location.
    """
    vacant_units = locations[supply_fname].sub(
        choosers[location_fname].value_counts(), fill_value=0)
    print "There are %d total available units" % locations[supply_fname].sum()
    print "    and %d total choosers" % len(choosers.index)
    print "    but there are %d overfull buildings" % \
        len(vacant_units[vacant_units < 0])
    vacant_units = vacant_units[vacant_units > 0]
    alternatives = locations.loc[np.repeat(vacant_units.index,
                                 vacant_units.values.astype('int'))] \
        .reset_index()
    print "    for a total of %d empty units" % vacant_units.sum()
    print "    in %d buildings total in the region" % len(vacant_units)
    return alternatives


def lcm_simulate(cfg, choosers, buildings, nodes, out_dfname, out_fname, supply_fname):
    """
    Simulate the location choices for the specified choosers

    Parameters
    ----------
    cfg : string
        The name of the yaml config file from which to read the location
        choice model.
    choosers : DataFrame
        A dataframe of agents doing the choosing.
    buildings : DataFrame
        A dataframe of buildings which the choosers are locating in and which
        have a supply.
    nodes : DataFrame
        A land use dataset to give neighborhood info around the buildings - will
        be joined to the buildings.
    out_dfname : string
        The name of the dataframe to write the simulated location to.
    out_fname : string
        The column name to write the simulated location to.
    supply_fname : string
        The string in the buildings table that indicates the amount of vacant
        units there will be for choosers.
    """
    cfg = misc.config(cfg)

    choosers = choosers_columns_used(choosers, out_fname, cfg)
    choosers = deal_with_nas(choosers)
    movers = choosers[choosers[out_fname] == -1]

    units = get_vacant_units(choosers,
                             out_fname,
                             merge_nodes(buildings, nodes, cfg),
                             supply_fname)
    alternatives = merge_nodes(units, nodes, cfg)

    new_units = class_of_yaml(cfg).predict_from_cfg(movers, alternatives, cfg)
    new_units = pd.Series(alternatives.loc[new_units.values][out_fname].values,
                          index=new_units.index)
    sim.partial_update(new_units, out_dfname, out_fname)
    _print_number_unplaced(sim.get_table(out_dfname), out_fname)


def simple_relocation(choosers, relocation_rate, fieldname='building_id'):
    choosers_name = choosers
    choosers = sim.get_table(choosers)
    print "Total agents: %d" % len(choosers[fieldname])
    _print_number_unplaced(choosers, fieldname)
    chooser_ids = np.random.choice(choosers.index, size=int(relocation_rate *
                                   len(choosers)), replace=False)
    s = choosers[fieldname]
    print "Assinging for relocation..."
    s.loc[chooser_ids] = -1
    sim.add_column(choosers_name, fieldname, s)
    _print_number_unplaced(choosers, fieldname)


def simple_transition(dfname, rate):
    transition = GrowthRateTransition(rate)
    tbl = sim.get_table(dfname)
    df = tbl.to_frame(tbl.local_columns)
    print "%d agents before transition" % len(df.index)
    df, added, copied, removed = transition.transition(df, None)
    print "%d agents after transition" % len(df.index)
    sim.add_table(dfname, df)
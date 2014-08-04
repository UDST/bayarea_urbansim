from urbansim.models import RegressionModel, SegmentedRegressionModel, \
    MNLLocationChoiceModel, SegmentedMNLLocationChoiceModel, GrowthRateTransition
import numpy as np
import pandas as pd
import urbansim.sim.simulation as sim
from urbansim.utils import misc


def change_scenario(scenario):
    assert scenario in sim.get_injectable("scenario_inputs"), "Invalid scenario name"
    print "Changing scenario to '%s'" % scenario
    sim.add_injectable("scenario", scenario)


def conditional_upzone(scenario, attr_name, upzone_name):
    scenario_inputs = sim.get_injectable("scenario_inputs")
    zoning_baseline = sim.get_table(scenario_inputs["baseline"]["zoning_table_name"])
    attr = zoning_baseline[attr_name]
    if scenario != "baseline":
        zoning_scenario = sim.get_table(scenario_inputs[scenario]["zoning_table_name"])
        upzone = zoning_scenario[upzone_name].dropna()
        attr = pd.concat([attr, upzone], axis=1).max(skipna=True, axis=1)
    return attr


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


def to_frame(tables, cfg, additional_columns=[]):
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    columns = misc.column_list(tables, cfg.columns_used()) + additional_columns
    if len(tables) > 1:
        df = sim.merge_tables(target=tables[0].name, tables=tables, columns=columns)
    else:
        df = tables[0].to_frame(columns)
    df = deal_with_nas(df)
    return df


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
    df = to_frame([tbl, nodes], cfg)
    return yaml_to_class(cfg).fit_from_cfg(df, cfg)


def hedonic_simulate(cfg, tbl, nodes, out_fname):
    cfg = misc.config(cfg)
    df = to_frame([tbl, nodes], cfg)
    price_or_rent, _ = yaml_to_class(cfg).predict_from_cfg(df, cfg)
    tbl.update_col_from_series(out_fname, price_or_rent)


def lcm_estimate(cfg, choosers, chosen_fname, buildings, nodes):
    cfg = misc.config(cfg)
    choosers = to_frame([choosers], cfg, additional_columns=[chosen_fname])
    alternatives = to_frame([buildings, nodes], cfg)
    return yaml_to_class(cfg).fit_from_cfg(choosers, chosen_fname, alternatives, cfg)


def lcm_simulate(cfg, choosers, buildings, nodes, out_fname, supply_fname, vacant_fname):
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
        The string in the buildings table that indicates the amount of available
        units there are for choosers, vacant or not.
    vacant_fname : string
        The string in the buildings table that indicates the amount of vacant
        units there will be for choosers.
    """
    cfg = misc.config(cfg)

    choosers_df = to_frame([choosers], cfg, additional_columns=[out_fname])
    locations_df = to_frame([buildings, nodes], cfg, [supply_fname, vacant_fname])

    available_units = locations_df[supply_fname]
    vacant_units = locations_df[vacant_fname]

    print "There are %d total available units" % available_units.sum()
    print "    and %d total choosers" % len(choosers.index)
    print "    but there are %d overfull buildings" % len(vacant_units[vacant_units < 0])

    vacant_units = vacant_units[vacant_units > 0]
    units = locations_df.loc[np.repeat(vacant_units.index,
                             vacant_units.values.astype('int'))].reset_index()

    print "    for a total of %d empty units" % vacant_units.sum()
    print "    in %d buildings total in the region" % len(vacant_units)

    movers = choosers_df[choosers_df[out_fname] == -1]

    new_units, _ = yaml_to_class(cfg).predict_from_cfg(movers, units, cfg)

    # go from units back to buildings
    new_buildings = pd.Series(units.loc[new_units.values][out_fname].values,
                              index=new_units.index)

    choosers.update_col_from_series(out_fname, new_buildings)
    _print_number_unplaced(choosers, out_fname)


def simple_relocation(choosers, relocation_rate, fieldname):
    print "Total agents: %d" % len(choosers)
    _print_number_unplaced(choosers, fieldname)

    print "Assinging for relocation..."
    chooser_ids = np.random.choice(choosers.index, size=int(relocation_rate *
                                   len(choosers)), replace=False)
    choosers.update_col_from_series(fieldname, pd.Series(-1, index=chooser_ids))

    _print_number_unplaced(choosers, fieldname)


def simple_transition(tbl, rate):
    transition = GrowthRateTransition(rate)
    df = tbl.to_frame(tbl.local_columns)

    print "%d agents before transition" % len(df.index)
    df, added, copied, removed = transition.transition(df, None)
    print "%d agents after transition" % len(df.index)

    sim.add_table(tbl.name, df)


def _print_number_unplaced(df, fieldname):
    print "Total currently unplaced: %d" % df[fieldname].value_counts().get(-1, 0)
import pandas as pd
import numpy as np
from urbansim.utils import misc
from urbansim.models import RegressionModel, MNLLocationChoiceModel


def rsh_estimate(dset):
    cfg = misc.config("rsh.yaml")
    buildings = pd.merge(
        dset.homesales, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    print hm.fit(buildings).summary()
    hm.to_yaml(str_or_buffer=cfg)


def rsh_simulate(dset):
    cfg = misc.config("rsh.yaml")
    buildings = dset.building_filter(residential=1)
    buildings = pd.merge(
        buildings, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    new_unit_price_res = hm.predict(buildings)
    print new_unit_price_res.describe()
    dset.buildings["res_sales_price"] = \
        new_unit_price_res.reindex(dset.buildings.index)


def rrh_estimate(dset):
    cfg = misc.config("rrh.yaml")
    buildings = pd.merge(
        dset.apartments, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    print hm.fit(buildings).summary()
    hm.to_yaml(str_or_buffer=cfg)


def rrh_simulate(dset):
    cfg = misc.config("rrh.yaml")
    buildings = dset.building_filter(residential=1)
    buildings = pd.merge(
        buildings, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    new_unit_rent = hm.predict(buildings)
    print new_unit_rent.describe()
    dset.buildings["res_rent"] = \
        new_unit_rent.reindex(dset.buildings.index)


def nrh_estimate(dset):
    cfg = misc.config("nrh.yaml")
    buildings = pd.merge(
        dset.costar, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    print hm.fit(buildings).summary()
    hm.to_yaml(str_or_buffer=cfg)


def nrh_simulate(dset):
    cfg = misc.config("nrh.yaml")
    buildings = dset.building_filter(residential=0)
    buildings = pd.merge(
        buildings, dset.nodes, left_on="_node_id", right_index=True)
    hm = RegressionModel.from_yaml(str_or_buffer=cfg)
    nonres_rent = hm.predict(buildings)
    print nonres_rent.describe()
    dset.buildings["nonresidential_rent"] = \
        nonres_rent.reindex(dset.buildings.index)


def hlcmo_estimate(dset):
    cfg = misc.config("hlcmo.yaml")
    buildings = pd.merge(dset.building_filter(residential=1),
                         dset.nodes, left_on="_node_id", right_index=True)
    lcm = MNLLocationChoiceModel.from_yaml(str_or_buffer=cfg)
    lcm.fit(dset.households, buildings, dset.households.building_id)
    lcm.report_fit()
    lcm.to_yaml(str_or_buffer=cfg)


def get_vacant_units(buildings, vacant_units):
    vacant_units = vacant_units[vacant_units > 0].order(ascending=False)
    alternatives = buildings.loc[np.repeat(vacant_units.index,
                                 vacant_units.values.astype('int'))] \
        .reset_index()
    print("There are %s empty units in %s locations total in the region" %
          (vacant_units.sum(), len(vacant_units)))
    return alternatives


def hlcmo_simulate(dset):
    cfg = misc.config("hlcmo.yaml")
    buildings = pd.merge(dset.building_filter(residential=1),
                         dset.nodes, left_on="_node_id", right_index=True)
    vacant_units = buildings.residential_units.sub(
        dset.households.groupby("building_id").size(), fill_value=0)
    alternatives = get_vacant_units(buildings, vacant_units)
    lcm = MNLLocationChoiceModel.from_yaml(str_or_buffer=cfg)
    new_units = lcm.predict(dset.households, alternatives)
    dset.households["building_id"].loc[new_units.index] = \
        alternatives.loc[new_units.values].building_id.values

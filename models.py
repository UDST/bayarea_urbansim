import pandas as pd
from urbansim.utils import misc
from urbansim.models import RegressionModel


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

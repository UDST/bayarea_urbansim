import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt
from utils import save_and_restore_state


def check_household_controls(households, household_controls, year):
    print households


@orca.step()
def simulation_validation(
        parcels, buildings, households, jobs, residential_units, year,
        household_controls, employment_controls):

    # this does a save and restore state for debugging
    d = save_and_restore_state(locals())
    for k in d.keys():
        exec("{} = d['{}']".format(k, k))

    # assert we fanned out the residential units correctly
    assert len(residential_units) == buildings.residential_units.sum()

    # make sure the unit counts per building add up
    pdt.assert_series_equal(
        buildings.residential_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.building_id.value_counts().sort_index(),
        check_names=False,
        check_dtype=False
    )

    # make sure we moved deed restricted units to the res units table correctly
    pdt.assert_series_equal(
        buildings.deed_restricted_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.deed_restricted.groupby(
            residential_units.building_id).sum().sort_index(),
        check_names=False,
        check_dtype=False
    )

    # check control totals are adding up

    # check no households are inplaced

    # check job allocation

    # check proportional jobs model is working

    # check household unit ids and building ids line up

    # check vacancies are reasonable

    # some sort of test to check local retail doesn't drop
    # below a certain level

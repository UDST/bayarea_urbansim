import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt
from utils import save_and_restore_state


def assert_series_equal(s1, s2):
    s1 = s1.sort_index()
    s2 = s2.sort_index()
    pdt.assert_series_equal(s1, s2, check_names=False, check_dtype=False)


# make sure the household controls are currently being matched
def check_household_controls(households, household_controls, year):
    print "Check household controls"
    current_household_controls = household_controls.loc[year]
    current_household_controls = current_household_controls.\
        set_index("base_income_quartile").total_number_of_households

    assert_series_equal(
        current_household_controls,
        households.base_income_quartile.value_counts()
    )


# make sure the employment controls are currently being matched
def check_job_controls(jobs, employment_controls, year, settings):
    print "Check job controls"
    current_employment_controls = employment_controls.loc[year]
    current_employment_controls = current_employment_controls.\
        set_index("empsix_id").number_of_jobs

    empsix_map = settings["empsix_name_to_id"]
    current_counts = jobs.empsix.map(empsix_map).value_counts()

    assert_series_equal(
        current_employment_controls,
        current_counts
    )


def check_residential_units(residential_units, buildings):
    # assert we fanned out the residential units correctly
    assert len(residential_units) == buildings.residential_units.sum()

    # make sure the unit counts per building add up
    assert_series_equal(
        buildings.residential_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.building_id.value_counts().sort_index()
    )

    # make sure we moved deed restricted units to the res units table correctly
    assert_series_equal(
        buildings.deed_restricted_units[
            buildings.residential_units > 0].sort_index(),
        residential_units.deed_restricted.groupby(
            residential_units.building_id).sum().sort_index()
    )


@orca.step()
def simulation_validation(
        parcels, buildings, households, jobs, residential_units, year,
        household_controls, employment_controls, settings):

    print jobs.empsix.value_counts()

    # this does a save and restore state for debugging
    d = save_and_restore_state(locals())
    for k in d.keys():
        exec("{} = d['{}']".format(k, k))

    print jobs.empsix.value_counts()

    check_job_controls(jobs, employment_controls, year, settings)

    check_household_controls(households, household_controls, year)

    check_residential_units(residential_units, buildings)

    # check no households are unplaced

    # check job allocation

    # check proportional jobs model is working

    # check household unit ids and building ids line up

    # check vacancies are reasonable

    # some sort of test to check local retail doesn't drop
    # below a certain level

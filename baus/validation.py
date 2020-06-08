from __future__ import print_function

import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt
from baus.utils import save_and_restore_state
from urbansim.utils import misc


def assert_series_equal(s1, s2, head=None):
    s1 = s1.sort_index()
    s2 = s2.sort_index()
    if head:
        # for really long series, it makes sense to only compare the first
        # head number of items in each series
        s1 = s1.head(head)
        s2 = s2.head(head)
    pdt.assert_series_equal(s1, s2, check_names=False, check_dtype=False)


# make sure the household controls are currently being matched
def check_household_controls(households, household_controls, year):
    print("Check household controls")
    current_household_controls = household_controls.local.loc[year]
    current_household_controls = current_household_controls.\
        set_index("base_income_quartile").total_number_of_households

    assert_series_equal(
        current_household_controls,
        households.base_income_quartile.value_counts()
    )


# make sure the employment controls are currently being matched
def check_job_controls(jobs, employment_controls, year, mapping):
    print("Check job controls")
    current_employment_controls = employment_controls.local.loc[year]
    current_employment_controls = current_employment_controls.\
        set_index("empsix_id").number_of_jobs

    empsix_map = mapping["empsix_name_to_id"]
    current_counts = jobs.empsix.map(empsix_map).value_counts()

    assert_series_equal(
        current_employment_controls,
        current_counts
    )


def check_residential_units(residential_units, buildings):
    print("Check residential units")
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


# make sure everyone gets a house - this might not exist in the real world,
# but due to the nature of control totals it exists here
def check_no_unplaced_households(households, year):
    print("Check no unplaced households")
    # for some reason, since we added renter/owner models, we do have
    # unplaced households in the first couple of years, which eventually
    # evens out
    # 02 26 2019 ET: all years can be activated now that tenure is off
    # if year <= 2030:
    #    return
    assert -1 not in households.building_id.value_counts()


def check_no_unplaced_jobs(jobs, year):
    print("Check no unplaced jobs")
    assert -1 not in jobs.building_id.value_counts()


# check not more households than units or jobs than job spaces
def check_no_overfull_buildings(households, buildings):
    print("Check no overfull buildings")
    assert True not in (buildings.vacant_res_units < 0).value_counts()
    # there are overfull job spaces based on the assignment and also
    # proportional job model
    # assert True not in (buildings.vacant_job_spaces < 0).value_counts()


# households have both unit ids and building ids - make sure they're in sync
def check_unit_ids_match_building_ids(households, residential_units):
    print("Check unit ids and building ids match")
    building_ids = misc.reindex(
        residential_units.building_id, households.unit_id)
    assert_series_equal(building_ids, households.building_id, 25000)


@orca.step()
def simulation_validation(
        parcels, buildings, households, jobs, residential_units, year,
        household_controls, employment_controls, mapping):

    # this does a save and restore state for debugging
    # d = save_and_restore_state(locals())
    # for k in d.keys():
    #     locals()[k].local = d[k]

    check_job_controls(jobs, employment_controls, year, mapping)

    check_household_controls(households, household_controls, year)

    check_residential_units(residential_units, buildings)

    check_no_unplaced_households(households, year)

    check_no_unplaced_jobs(jobs, year)

    check_no_overfull_buildings(households, buildings)

    check_unit_ids_match_building_ids(households, residential_units)

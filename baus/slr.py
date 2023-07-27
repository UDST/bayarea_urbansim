from __future__ import print_function

import orca
import numpy as np
import pandas as pd
from urbansim_defaults import utils
from baus import datasources
from baus import variables
from baus import summaries


@orca.step()
def slr_inundate(slr_progression, slr_parcel_inundation, year, parcels):

    # inundated parcels are all parcels at or below the SLR progression level in that year
    slr_progression = slr_progression.to_frame()
    orca.add_table("slr_progression", slr_progression)
    inundation_yr = slr_progression.query("year==@year")["inundated"].item()
    print("Inundation in model year is %d inches" % inundation_yr)

    # tag parcels that are indundated in the current year
    # slr mitigation is applied by modifying the set of inundated parcels in the list
    slr_parcel_inundation = slr_parcel_inundation.to_frame()
    orca.add_injectable("slr_mitigation", "applied")

    destroy_parcels = slr_parcel_inundation.query("inundation<=@inundation_yr").astype(
        "bool"
    )
    orca.add_table("destroy_parcels", destroy_parcels)
    print("Number of parcels destroyed: %d" % len(destroy_parcels))

    slr_nodev = pd.Series(False, parcels.index)
    destroy = pd.Series(destroy_parcels["inundation"])
    slr_nodev.update(destroy)
    orca.add_column("parcels", "slr_nodev", slr_nodev)
    parcels = orca.get_table("parcels")


@orca.step()
def slr_remove_dev(buildings, year, parcels, households, jobs):

    destroy_parcels = orca.get_table("destroy_parcels")
    slr_demolish = buildings.local[buildings.parcel_id.isin(destroy_parcels.index)]
    orca.add_table("slr_demolish", slr_demolish)

    # remove buildings from parcels
    print("Demolishing %d buildings" % len(slr_demolish))
    households = households.to_frame()
    hh_unplaced = households[households["building_id"] == -1]
    jobs = jobs.to_frame()
    jobs_unplaced = jobs[jobs["building_id"] == -1]
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(
        buildings.to_frame(buildings.local_columns),
        slr_demolish,
        unplace_agents=["households", "jobs"],
    )

    # remove households from these buildings and mark them as "unplaced"
    households = orca.get_table("households")
    households = households.to_frame()
    hh_unplaced_slr = households[households["building_id"] == -1]
    hh_unplaced_slr = hh_unplaced_slr[~hh_unplaced_slr.index.isin(hh_unplaced.index)]
    orca.add_injectable("hh_unplaced_slr", hh_unplaced_slr)
    # remove jobs from these buildings and mark them as "unplaced"
    jobs = orca.get_table("jobs")
    jobs = jobs.to_frame()
    jobs_unplaced_slr = jobs[jobs["building_id"] == -1]
    jobs_unplaced_slr = jobs_unplaced_slr[
        ~jobs_unplaced_slr.index.isin(jobs_unplaced.index)
    ]
    orca.add_injectable("jobs_unplaced_slr", jobs_unplaced_slr)

    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print("Demolished %d buildings" % (l1 - len(buildings)))

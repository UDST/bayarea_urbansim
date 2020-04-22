from __future__ import print_function

import orca
import numpy as np
import pandas as pd
from urbansim_defaults import utils
from baus import datasources
from baus import variables
from baus import summaries


# select and tag parcels that are indundated in the current year:
# all parcels at or below the SLR which corresponds to that year

@orca.step()
def slr_inundate(scenario, parcels, slr_progression_C, slr_progression_R,
                 slr_progression_B, slr_parcel_inundation,
                 slr_parcel_inundation_mf, slr_parcel_inundation_mp,
                 year, hazards):

    if scenario not in hazards["slr_scenarios"]["enable_in"]:
        return

    if scenario in hazards["slr_scenarios"]["rtff_prog"]:
        slr_progression = slr_progression_R.to_frame()
    elif scenario in hazards["slr_scenarios"]["cag_prog"]:
        slr_progression = slr_progression_C.to_frame()
    elif scenario in hazards["slr_scenarios"]["bttf_prog"]:
        slr_progression = slr_progression_B.to_frame()
    orca.add_table("slr_progression", slr_progression)

    inundation_yr = slr_progression.query('year==@year')['inundated'].item()
    print("Inundation in model year is %d inches" % inundation_yr)

    if scenario in hazards["slr_scenarios"]["mitigation_full"]:
        slr_parcel_inundation = slr_parcel_inundation_mf.to_frame()
        orca.add_injectable("slr_mitigation", 'full mitigation')
    elif scenario in hazards["slr_scenarios"]["mitigation_partial"]:
        slr_parcel_inundation = slr_parcel_inundation_mp.to_frame()
        orca.add_injectable("slr_mitigation", 'partial mitigation')
    else:
        slr_parcel_inundation = slr_parcel_inundation.to_frame()
        orca.add_injectable("slr_mitigation", 'none')

    destroy_parcels = slr_parcel_inundation.\
        query('inundation<=@inundation_yr').astype('bool')
    orca.add_table('destroy_parcels', destroy_parcels)
    print("Number of parcels destroyed: %d" % len(destroy_parcels))

    slr_nodev = pd.Series(False, parcels.index)
    destroy = pd.Series(destroy_parcels['inundation'])
    slr_nodev.update(destroy)
    orca.add_column('parcels', 'slr_nodev', slr_nodev)
    parcels = orca.get_table("parcels")


# remove building space from parcels,
# remove households and jobs and put in unplaced

@orca.step()
def slr_remove_dev(buildings, year, parcels, households, jobs,
                   scenario, hazards):

    if scenario not in hazards["slr_scenarios"]["enable_in"]:
        return

    destroy_parcels = orca.get_table("destroy_parcels")
    slr_demolish = buildings.local[buildings.parcel_id.isin
                                   (destroy_parcels.index)]
    orca.add_table("slr_demolish", slr_demolish)

    print("Demolishing %d buildings" % len(slr_demolish))
    households = households.to_frame()
    hh_unplaced = households[households["building_id"] == -1]
    jobs = jobs.to_frame()
    jobs_unplaced = jobs[jobs["building_id"] == -1]
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(
        buildings.to_frame(buildings.local_columns),
        slr_demolish,
        unplace_agents=["households", "jobs"])
    households = orca.get_table("households")
    households = households.to_frame()
    hh_unplaced_slr = households[households["building_id"] == -1]
    hh_unplaced_slr = hh_unplaced_slr[~hh_unplaced_slr.index.isin
                                      (hh_unplaced.index)]
    orca.add_injectable("hh_unplaced_slr", hh_unplaced_slr)
    jobs = orca.get_table("jobs")
    jobs = jobs.to_frame()
    jobs_unplaced_slr = jobs[jobs["building_id"] == -1]
    jobs_unplaced_slr = jobs_unplaced_slr[~jobs_unplaced_slr.index.isin
                                          (jobs_unplaced.index)]
    orca.add_injectable("jobs_unplaced_slr", jobs_unplaced_slr)
    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print("Demolished %d buildings" % (l1 - len(buildings)))

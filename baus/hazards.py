import orca
import numpy as np
import pandas as pd
from urbansim_defaults import utils
import datasources
import variables
import summaries


# select and tag parcels that are indundated in the current year:
# all parcels at or below the SLR which corresponds to that year


@orca.step()
def slr_inundate(parcels, slr_progression_f3, year, slr_parcel_inundation):
    # UPDATE slr_progression_fX based on "futures" scenario
    # (including function arg)
    slr_progression = slr_progression_f3.to_frame()
    inundation_yr = slr_progression.query('year==@year')['inundated'].item()
    print "Inundation in model year is %d inches" % inundation_yr
    slr_parcel_inundation = slr_parcel_inundation.to_frame()
    destroy_parcels = slr_parcel_inundation.\
        query('inundation<=@inundation_yr').astype('bool')
    orca.add_table('destroy_parcels', destroy_parcels)
    print "Number of parcels destroyed: %d" % len(destroy_parcels)

    slr_nodev = pd.Series(False, parcels.index)
    destroy = pd.Series(destroy_parcels['inundation'])
    slr_nodev.update(destroy)
    orca.add_column('parcels', 'slr_nodev', slr_nodev)
    parcels = orca.get_table("parcels")

# remove building space from parcels,
# remove households and jobs and put in unplaced


@orca.step()
def slr_remove_dev(buildings, destroy_parcels, year, parcels):
    slr_demolish = buildings.local[buildings.parcel_id.isin
                                   (destroy_parcels.index)]
    orca.add_table('slr_demolish', slr_demolish)

    print "Demolishing %d buildings" % len(slr_demolish)
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(
        buildings.to_frame(buildings.local_columns),
        slr_demolish,
        unplace_agents=["households", "jobs"])
    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print "Demolished %d buildings" % (l1 - len(buildings))


# floodier is parcels where buildings decline in value maybe
# floodier should not pass 2015

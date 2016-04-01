import models
import pandas as pd
import urbansim.sim.simulation as sim

parcels = sim.get_table("parcels")
buildings = sim.get_table("buildings")
households = sim.get_table("households")
jobs = sim.get_table("jobs")

s = buildings.parcel_id.isin(parcels.index)

print "Building's parcel id in parcel index\n", s.value_counts()

s = households.building_id.isin(buildings.index)

print "Households with no building assigned: \n", \
    (households.building_id == -1).value_counts()
print "Household's building id in building index\n", s.value_counts()

s = jobs.building_id.isin(buildings.index)

print "Jobs with no building assigned: \n", \
    (jobs.building_id == -1).value_counts()
print "Job's building id in building index\n", s.value_counts()

print "Len jobs"

print len(jobs)

print "Num job spaces"

print buildings.job_spaces.sum()

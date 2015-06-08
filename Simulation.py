import time
import models
import urbansim.sim.simulation as sim


print "Started", time.ctime()
in_year, out_year = 2010, 2012

sim.run([
    "neighborhood_vars",         # accessibility variables

    "rsh_simulate",              # residential sales hedonic
    "nrh_simulate",              # non-residential rent hedonic

    "households_relocation",
    "hlcm_simulate",
    "households_transition",

    "jobs_relocation",
    "elcm_simulate",
    "jobs_transition",


    "price_vars",

    "feasibility",
    "residential_developer",
    "non_residential_developer",
     
    "diagnostic_output",
    "travel_model_output"
], years=range(in_year, out_year))
print "Finished", time.ctime()

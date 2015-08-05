import time
import models
import pandas as pd
import urbansim.sim.simulation as sim

sim.run([
    "neighborhood_vars",         # accessibility variables
    "rsh_estimate"               # residential sales hedonic
    "rrh_estimate"               # residential rental hedonic
])

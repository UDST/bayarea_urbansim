import time
import models
import pandas as pd
import orca

orca.run([
    "neighborhood_vars",         # accessibility variables
    "nrh_estimate",              # non-res rent hedonic
    "nrh_simulate",
    "elcm_estimate",             # employment lcm
    "rsh_estimate",              # residential sales hedonic
    "rsh_simulate",
    "hlcm_estimate"               # household lcm
])

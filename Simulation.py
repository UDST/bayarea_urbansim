import os
import sys
import time
import models
import pandas as pd
import urbansim.sim.simulation as sim
import socket

SLACK = MAPS = False
INTERACT = False

if INTERACT:
    import code
    code.interact(local=locals())

if SLACK:
    from slacker import Slacker
    slack = Slacker('xoxp-7025187590-7026053537-7111663091-9eeeb6')
    host = socket.gethostname()
    run_num = sim.get_injectable("run_number")

print "Started", time.ctime()
in_year, out_year = 2010, 2012

if SLACK:
    slack.chat.post_message('#sim_updates', 
        'Starting simulation %d on host %s' % (run_num, host))

try:
  sim.run([
    "neighborhood_vars",         # accessibility variables

    "rsh_simulate",              # residential sales hedonic
    #"nrh_simulate",              # non-residential rent hedonic

    "households_relocation",
    "households_transition",
    "hlcm_simulate",

    #"jobs_relocation",
    #"jobs_transition",
    #"elcm_simulate",

    "price_vars",

    "feasibility",
    "residential_developer",
    #"non_residential_developer",
     
    "diagnostic_output",
    "travel_model_output"
  ], years=range(in_year, out_year))

except:
    if SLACK:
        slack.chat.post_message('#sim_updates', 
            'DANG!  Simulation failed for %d on host %s' % (run_num, host))
    sys.exit(0)

print "Finished", time.ctime()

if MAPS:
    os.system('python scripts/explorer.py %d' % run_num)
    os.system('python scripts/compare_to_targets.py %d' % run_num)
    os.system('python scripts/make_pda_result_maps.py %d' % run_num)

if SLACK:
    slack.chat.post_message('#sim_updates', 
        'Completed simulation %d on host %s' % (run_num, host))

    slack.chat.post_message('#sim_updates', 
        'UrbanSim explorer is available at http://urbanforecast.com/sim_explorer%d.html' % run_num)

    slack.chat.post_message('#sim_updates', 
        'PDA target comparison is available at http://urbanforecast.com/scratchpad/results_%d.html' % run_num)

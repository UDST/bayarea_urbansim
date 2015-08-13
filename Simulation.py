import os
import sys
import time
import traceback
import models
import pandas as pd
import urbansim.sim.simulation as sim
import socket
import warnings

warnings.filterwarnings("ignore")

SLACK = MAPS = True
INTERACT = False

if INTERACT:
    import code
    code.interact(local=locals())
    sys.exit()

run_num = sim.get_injectable("run_number")
sys.stdout = sys.stderr = open("logs/sim_out_%d" % run_num, 'w')

if SLACK:
    from slacker import Slacker
    slack = Slacker('xoxp-7025187590-7026053537-7111663091-9eeeb6')
    host = socket.gethostname()

print "Started", time.ctime()
in_year, out_year = 2010, 2040

if SLACK:
    slack.chat.post_message('#sim_updates', 
        'Starting simulation %d on host %s' % (run_num, host))

try:
  sim.run([ 
    "neighborhood_vars",            # accessibility variables
    
    "rsh_simulate",                 # residential sales hedonic
    "nrh_simulate",                 # non-residential rent hedonic

    "households_relocation",
    "households_transition",
    "hlcm_simulate",

    "jobs_relocation",
    "jobs_transition",
    "elcm_simulate",

    "price_vars",

    "feasibility",
    
    "scheduled_development_events", # scheduled buildings additions
    "residential_developer",
    "non_residential_developer",
     
    "diagnostic_output",
    "travel_model_output"
  ], years=range(in_year, out_year))

except Exception as e:
    print traceback.print_exc()
    if SLACK:
        slack.chat.post_message('#sim_updates', 
            'DANG!  Simulation failed for %d on host %s' % (run_num, host))
    else:
        raise e
    sys.exit(0)

print "Finished", time.ctime()

if MAPS:
    try:
        os.system('python scripts/explorer.py %d' % run_num)
        os.system('python scripts/compare_to_targets.py %d' % run_num)
        os.system('python scripts/make_pda_result_maps.py %d' % run_num)
    except Exception as e:
        if SLACK:
            slack.chat.post_message('#sim_updates', 
                'DANG! Output generation failed for %d on host %s' % (run_num, host))
        else:
            raise e
        sys.exit(0)


if SLACK:
    slack.chat.post_message('#sim_updates', 
        'Completed simulation %d on host %s' % (run_num, host))

    slack.chat.post_message('#sim_updates', 
        'UrbanSim explorer is available at http://urbanforecast.com/sim_explorer%d.html' % run_num)

    slack.chat.post_message('#sim_updates', 
        'PDA target comparison is available at http://urbanforecast.com/scratchpad/results_%d.html' % run_num)

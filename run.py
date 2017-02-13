import os
import sys
import time
import traceback
from baus import models
from baus import ual
import pandas as pd
import orca
import socket
import warnings
from baus.utils import compare_summary

warnings.filterwarnings("ignore")

args = sys.argv[1:]

# Suppress scientific notation in pandas output
pd.set_option('display.float_format', lambda x: '%.3f' % x)

SLACK = MAPS = "URBANSIM_SLACK" in os.environ
LOGS = True
INTERACT = False
SCENARIO = None
MODE = "ual_simulation"
S3 = False
EVERY_NTH_YEAR = 5
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read()
COMPARE_TO_NO_PROJECT = True
NO_PROJECT = 611
IN_YEAR, OUT_YEAR = 2010, 2011

LAST_KNOWN_GOOD_RUNS = {
    "0": 1057,
    "1": 1058,
    "2": 1059,
    "3": 1060,
    "4": 1059,
    "5": 1059
}

orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)

if len(args) and args[0] == "-i":
    SLACK = MAPS = LOGS = False
    INTERACT = True

if len(args) and args[0] == "-s":
    orca.add_injectable("scenario", args[1])

SCENARIO = orca.get_injectable("scenario")

if INTERACT:
    import code
    code.interact(local=locals())
    sys.exit()

run_num = orca.get_injectable("run_number")

if LOGS:
    print '***The Standard stream is being written to /runs/run{0}.log***'\
        .format(run_num)
    sys.stdout = sys.stderr = open("runs/run%d.log" % run_num, 'w')

if SLACK:
    from slacker import Slacker
    slack = Slacker(os.environ["SLACK_TOKEN"])
    host = socket.gethostname()


def get_simulation_models(SCENARIO):

    models = [
        "neighborhood_vars",            # local accessibility vars
        "regional_vars",                # regional accessibility vars

        "rsh_simulate",                 # residential sales hedonic
        "nrh_simulate",                 # non-residential rent hedonic

        "households_relocation",
        "households_transition",

        "jobs_relocation",
        "jobs_transition",

        "price_vars",

        "scheduled_development_events",  # scheduled buildings additions

        "lump_sum_accounts",             # run the subsidized acct system
        "subsidized_residential_developer_lump_sum_accts",

        "alt_feasibility",

        "residential_developer",
        "developer_reprocess",
        "retail_developer",
        "office_developer",

        "hlcm_simulate",                 # put these last so they don't get
        "elcm_simulate",                 # displaced by new dev

        "topsheet",
        "parcel_summary",
        "building_summary",
        "diagnostic_output",
        "geographic_summary",
        "travel_model_output"
    ]

    # calculate VMT taxes
    if SCENARIO in ["1", "3", "4"]:
        # calculate the vmt fees at the end of the year

        # note that you might also have to change the fees that get
        # imposed - look for fees_per_unit column in variables.py
        if SCENARIO == "3":
            orca.get_injectable("settings")["vmt_fee_res"] = True
        if SCENARIO == "1":
            orca.get_injectable("settings")["vmt_fee_com"] = True
        if SCENARIO == "4":
            orca.get_injectable("settings")["vmt_fee_com"] = True
        models.insert(models.index("diagnostic_output"),
                      "calculate_vmt_fees")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_feasibility")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_developer_vmt")

    return models


def run_models(MODE, SCENARIO):

    orca.run(["correct_baseyear_data"])

    if MODE == "simulation":

        years_to_run = range(IN_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
        models = get_simulation_models(SCENARIO)
        orca.run(models, iter_vars=years_to_run)

    elif MODE == "ual_simulation":

        years_to_run = range(IN_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
        models = get_simulation_models(SCENARIO)

        # Initialization steps
        orca.run([
            "ual_initialize_residential_units",
            "ual_match_households_to_units",
            "ual_assign_tenure_to_units",
#           "ual_load_rental_listings",     # required to estimate rental hedonic
        ])

        # Estimation steps
        orca.run([
#           "neighborhood_vars",             # street network accessibility
#           "regional_vars",                 # road network accessibility
#             
#           "ual_rrh_estimate",              # estimate residential rental hedonic
# 
#           "ual_hlcm_owner_estimate",       # estimate location choice for owners
#           "ual_hlcm_renter_estimate",      # estimate location choice for renters

        ])

        # Simulation steps
        orca.run([

            "neighborhood_vars",                # street network accessibility
            "regional_vars",                    # road network accessibility
            
            "ual_rsh_simulate",                 # residential sales hedonic for units
            "ual_rrh_simulate",                 # residential rental hedonic for units
            "nrh_simulate",                     # non-residential rent hedonic
            
            "ual_assign_tenure_to_new_units",   # (based on higher of predicted price or rent)
 
            "ual_households_relocation",        # uses conditional probabilities
            "households_transition",
            "ual_reconcile_unplaced_households",  # update building/unit/hh correspondence

            "ual_hlcm_owner_simulate",          # allocate owners to vacant owner-occupied units
            "ual_hlcm_renter_simulate",         # allocate renters to vacant rental units
            "ual_reconcile_placed_households",  # update building/unit/hh correspondence

            "jobs_relocation",
            "jobs_transition",
            "elcm_simulate",

            "ual_update_building_residential_price",  # apply unit prices to buildings          
            "price_vars",
            "scheduled_development_events",
            "alt_feasibility",
            
            "residential_developer",
            "developer_reprocess",
            "retail_developer",
            "office_developer",
            
            "ual_remove_old_units",               # (for buildings that were removed)
            "ual_initialize_new_units",           # set up units for new residential buildings
            "ual_reconcile_unplaced_households",  # update building/unit/hh correspondence

#             "ual_save_intermediate_tables",       # saves output for visualization
            
            "topsheet",
            "diagnostic_output",
            "geographic_summary",
            "travel_model_output"

        ], iter_vars=years_to_run)

    elif MODE == "estimation":

        orca.run([

            "neighborhood_vars",         # local accessibility variables
            "regional_vars",             # regional accessibility variables
            "rsh_estimate",              # residential sales hedonic
            "nrh_estimate",              # non-res rent hedonic
            "rsh_simulate",
            "nrh_simulate",
            "hlcm_estimate",             # household lcm
            "elcm_estimate",             # employment lcm

        ], iter_vars=[2010])

    elif MODE == "baseyearsim":

        orca.run([

            "neighborhood_vars",            # local accessibility vars
            "regional_vars",                # regional accessibility vars

            "rsh_simulate",                 # residential sales hedonic

            "households_transition",

            "hlcm_simulate",                 # put these last so they don't get

            "geographic_summary",
            "travel_model_output"

        ], iter_vars=[2010])

        for geog_name in ["juris", "pda", "superdistrict", "taz"]:
            os.rename(
                "runs/run%d_%s_summaries_2010.csv" % (run_num, geog_name),
                "output/baseyear_%s_summaries_2010.csv" % geog_name)

    elif MODE == "feasibility":

        orca.run([

            "neighborhood_vars",            # local accessibility vars
            "regional_vars",                # regional accessibility vars

            "rsh_simulate",                 # residential sales hedonic
            "nrh_simulate",                 # non-residential rent hedonic

            "price_vars",
            "subsidized_residential_feasibility"

        ], iter_vars=[2010])

        # the whole point of this is to get the feasibility dataframe
        # for debugging
        df = orca.get_table("feasibility").to_frame()
        df = df.stack(level=0).reset_index(level=1, drop=True)
        df.to_csv("output/feasibility.csv")

    else:

        raise "Invalid mode"


print "Started", time.ctime()
print "Current Commit : ", CURRENT_COMMIT.rstrip()
print "Current Scenario : ", orca.get_injectable('scenario').rstrip()


if SLACK:
    slack.chat.post_message(
        '#sim_updates',
        'Starting simulation %d on host %s (scenario: %s)' %
        (run_num, host, SCENARIO), as_user=True)

try:

    run_models(MODE, SCENARIO)

except Exception as e:
    print traceback.print_exc()
    if SLACK:
        slack.chat.post_message(
            '#sim_updates',
            'DANG!  Simulation failed for %d on host %s'
            % (run_num, host), as_user=True)
    else:
        raise e
    sys.exit(0)

print "Finished", time.ctime()

if MAPS:

    from urbansim_explorer import sim_explorer as se
    se.start(
        'runs/run%d_simulation_output.json' % run_num,
        'runs/run%d_parcel_output.csv' % run_num,
        write_static_file='/var/www/html/sim_explorer%d.html' % run_num
    )

if SLACK:
    slack.chat.post_message(
        '#sim_updates',
        'Completed simulation %d on host %s' % (run_num, host), as_user=True)

    slack.chat.post_message(
        '#sim_updates',
        'UrbanSim explorer is available at ' +
        'http://urbanforecast.com/sim_explorer%d.html' % run_num, as_user=True)

    slack.chat.post_message(
        '#sim_updates',
        'Final topsheet is available at ' +
        'http://urbanforecast.com/runs/run%d_topsheet_2040.log' % run_num,
        as_user=True)

if MODE == "simulation":
    # compute and write the difference report at the superdistrict level
    prev_run = LAST_KNOWN_GOOD_RUNS[SCENARIO]
    # fetch the previous run off of the internet for comparison - the "last
    # known good run" should always be available on EC2
    df1 = pd.read_csv("http://urbanforecast.com/runs/run%d_superdistrict_summaries_2040.csv" % prev_run)
    df1 = df1.set_index(df1.columns[0]).sort_index()

    df2 = pd.read_csv("runs/run%d_superdistrict_summaries_2040.csv" % run_num)
    df2 = df2.set_index(df2.columns[0]).sort_index()

    supnames = \
        pd.read_csv("data/superdistricts.csv", index_col="number").name

    summary = compare_summary(df1, df2, supnames)
    with open("runs/run%d_difference_report.log" % run_num, "w") as f:
        f.write(summary)

if SLACK and MODE == "simulation":

    if len(summary.strip()) != 0:
        sum_lines = len(summary.strip().split("\n"))
        slack.chat.post_message(
            '#sim_updates',
            ('Difference report is available at ' +
             'http://urbanforecast.com/runs/run%d_difference_report.log ' +
             '- %d line(s)') % (run_num, sum_lines),
            as_user=True)
    else:
        slack.chat.post_message(
            '#sim_updates', "No differences with reference run.", as_user=True)

if S3:
    try:
        os.system(
            'ls runs/run%d_* ' % run_num +
            '| xargs -I file aws s3 cp file ' +
            's3://bayarea-urbansim-results')
    except Exception as e:
        raise e
    sys.exit(0)

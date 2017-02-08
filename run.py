import os
import sys
import time
import traceback
from baus import models
import pandas as pd
import orca
import socket
import argparse
import warnings
from baus.utils import compare_summary
from scripts.check_feedback import check_feedback

warnings.filterwarnings("ignore")

SLACK = MAPS = "URBANSIM_SLACK" in os.environ
LOGS = True
INTERACT = False
SCENARIO = None
MODE = "simulation"
S3 = False
EVERY_NTH_YEAR = 5
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read()
COMPARE_TO_NO_PROJECT = True
NO_PROJECT = 611
IN_YEAR, OUT_YEAR = 2010, 2040

LAST_KNOWN_GOOD_RUNS = {
    "0": 1057,
    "1": 1058,
    "2": 1059,
    "3": 1060,
    "4": 1059,
    "5": 1059
}

orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)

parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument('-c', action='store_true', dest='console',
                    help='run from the console (logs to stdout), no slack or maps')

parser.add_argument('-i', action='store_true', dest='interactive',
                    help='enter interactive mode after imports')

parser.add_argument('-s', action='store', dest='scenario',
                    help='specify which scenario to run')

parser.add_argument('-y', action='store', dest='out_year', type=int,
                    help='The year to which to run the simualtion.')

parser.add_argument('--mode', action='store', dest='mode',
                    help='which mode to run (see code for mode options)')

parser.add_argument('--disable-slack', action='store_true', dest='noslack',
                    help='disable slack outputs')

options = parser.parse_args()

if options.console:
    SLACK = MAPS = LOGS = False

if options.interactive:
    SLACK = MAPS = LOGS = False
    INTERACT = True

if options.out_year:
    OUT_YEAR = options.out_year

if options.scenario:
    orca.add_injectable("scenario", options.scenario)

if options.mode:
    MODE = options.mode

if options.noslack:
    SLACK = False

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
        "office_developer",
        "retail_developer",
        "additional_units",

        "hlcm_simulate",                 # put these last so they don't get
        "proportional_elcm",             # start with a proportional jobs model
        "elcm_simulate",                 # displaced by new dev

        "topsheet",
        "parcel_summary",
        "building_summary",
        "diagnostic_output",
        "geographic_summary",
        "travel_model_output"
    ]

    # calculate VMT taxes
    vmt_settings = orca.get_injectable("settings")["vmt_settings"]
    if SCENARIO in vmt_settings["com_for_com_scenarios"]:
        models.insert(models.index("office_developer"),
                      "subsidized_office_developer")

    if SCENARIO in vmt_settings["com_for_res_scenarios"] or \
        SCENARIO in vmt_settings["res_for_res_scenarios"]

        models.insert(models.index("diagnostic_output"),
                      "calculate_vmt_fees")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_feasibility")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_developer_vmt")

    return models


def run_models(MODE, SCENARIO):

    if MODE == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings"
        ])

    elif MODE == "simulation":

        years_to_run = range(IN_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
        models = get_simulation_models(SCENARIO)
        orca.run(models, iter_vars=years_to_run)

    elif MODE == "estimation":

        orca.run([

            "neighborhood_vars",         # local accessibility variables
            "regional_vars",             # regional accessibility variables
            "rsh_estimate",              # residential sales hedonic
            #"nrh_estimate",              # non-res rent hedonic
            #"rsh_simulate",
            #"nrh_simulate",
            #"hlcm_estimate",             # household lcm
            #"elcm_estimate",             # employment lcm

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

    slack.chat.post_message(
        '#sim_updates',
        'Targets comparison is available at ' +
        'http://urbanforecast.com/runs/run%d_targets_comparison_2040.csv' %
        run_num, as_user=True)

if MODE == "simulation":

    # copy base year into runs so as to avoid confusion

    import shutil

    for fname in [
        "baseyear_juris_summaries_2010.csv",
        "baseyear_pda_summaries_2010.csv",
        "baseyear_superdistrict_summaries_2010.csv",
        "baseyear_taz_summaries_2010.csv"
    ]:

        shutil.copy(
            os.path.join("output", fname),
            os.path.join("runs", "run{}_".format(run_num) + fname)
        )

if MODE == "simulation":
    # compute and write the difference report at the superdistrict level
    prev_run = LAST_KNOWN_GOOD_RUNS[SCENARIO]
    # fetch the previous run off of the internet for comparison - the "last
    # known good run" should always be available on EC2
    df1 = pd.read_csv(("http://urbanforecast.com/runs/run%d_superdistrict" +
                       "_summaries_2040.csv") % prev_run)
    df1 = df1.set_index(df1.columns[0]).sort_index()

    df2 = pd.read_csv("runs/run%d_superdistrict_summaries_2040.csv" % run_num)
    df2 = df2.set_index(df2.columns[0]).sort_index()

    supnames = \
        pd.read_csv("data/superdistricts.csv", index_col="number").name

    summary = compare_summary(df1, df2, supnames)
    with open("runs/run%d_difference_report.log" % run_num, "w") as f:
        f.write(summary)

if MODE == "simulation":
    with open("runs/run%d_check_feedback.log" % run_num, "w") as f:
        f.write(check_feedback(run_num))

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

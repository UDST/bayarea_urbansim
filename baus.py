import os
import sys
import time
import traceback
from baus import models
from baus import slr
from baus import earthquake
from baus import ual
from baus import validation
import pandas as pd
import orca
import socket
import argparse
import warnings
from baus.utils import compare_summary
import s3fs
import os

warnings.filterwarnings("ignore")

# Suppress scientific notation in pandas output
pd.set_option('display.float_format', lambda x: '%.3f' % x)

OUTPUT_STORE = False
DATA_OUT = './output/model_data_output.h5'
MODE = "simulation"
SLACK = MAPS = "URBANSIM_SLACK" in os.environ
LOGS = True
INTERACT = False
SCENARIO = None
EVERY_NTH_YEAR = 5
BRANCH = os.popen('git rev-parse --abbrev-ref HEAD').read()
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read()
COMPARE_TO_NO_PROJECT = True
NO_PROJECT = 611
EARTHQUAKE = False
OUTPUT_BUCKET = 'urbansim-outputs'
OUT_TABLES = [
    'parcels', 'jobs', 'households', 'buildings', 'units', 'zones',
    'establishments', 'persons', 'craigslist', 'mtc_skims',
    'beam_skims_raw', 'beam_skims_imputed',
    # these four tables are not used, just passed through from input
    # to output because activitysynth needs them for the time being
    'walk_nodes', 'walk_edges', 'drive_nodes', 'drive_edges'
]

IN_YEAR, OUT_YEAR = 2010, 2040
COMPARE_AGAINST_LAST_KNOWN_GOOD = False

LAST_KNOWN_GOOD_RUNS = {
    "0": 1057,
    "1": 1058,
    "2": 1059,
    "3": 1060,
    "4": 1059,
    "5": 1059
}

orca.add_injectable("earthquake", EARTHQUAKE)

parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument(
    '-c', action='store_true', dest='console',
    help='run from the console (logs to stdout), no slack or maps')

parser.add_argument('-i', action='store_true', dest='interactive',
                    help='enter interactive mode after imports')

parser.add_argument('-o', action='store_true', dest='output_store',
                    help='write output data tables to h5 data store')

parser.add_argument('-d', action='store_true', dest='data_out',
                    help='full filepath for output data')

parser.add_argument('-s', action='store', dest='scenario',
                    help='specify which scenario to run')

parser.add_argument('-k', action='store_true', dest='skip_base_year',
                    help='skip base year - used for debugging')

parser.add_argument('--years', '-y', action='store', dest='years',
                    help='comma separated input and output years.')

parser.add_argument('--mode', '-m', action='store', dest='mode',
                    help='which mode to run (see code for mode options)')

parser.add_argument('--disable-slack', action='store_true', dest='noslack',
                    help='disable slack outputs')

parser.add_argument('--years-per-iter', '-n', action='store_true',
                    dest='every_nth_year',
                    help='simulation iteration frequency')

options = parser.parse_args()

if options.every_nth_year:
    EVERY_NTH_YEAR = options.every_nth_year
orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)

if options.console:
    SLACK = MAPS = LOGS = False

if options.interactive:
    SLACK = MAPS = LOGS = False
    INTERACT = True

if options.years:
    IN_YEAR, OUT_YEAR = [int(x) for x in options.years.split(',')]
    orca.add_injectable('in_year', IN_YEAR)

if options.output_store:
    if options.data_out:
        DATA_OUT = options.data_out
    if os.path.exists(DATA_OUT):
        os.remove(DATA_OUT)
else:
    DATA_OUT = None

if options.scenario:
    orca.add_injectable("scenario", options.scenario)

SKIP_BASE_YEAR = options.skip_base_year

if options.mode:
    MODE = options.mode

if options.noslack:
    SLACK = False

SCENARIO = orca.get_injectable("scenario")

run_num = orca.get_injectable("run_number")

if LOGS:
    print '***The Standard stream is being written to /runs/run{0}.log***'\
        .format(run_num)
    sys.stdout = sys.stderr = open("runs/run%d.log" % run_num, 'w')


def get_simulation_models(SCENARIO):

    # ual has a slightly different set of models - might be able to get rid
    # of the old version soon

    models = [
        # "slr_inundate",
        # "slr_remove_dev",
        # "eq_code_buildings",
        # "earthquake_demolish",
        "load_rental_listings",
        "neighborhood_vars",    # street network accessibility
        "generate_skims_vars",
        "skims_aggregations_drive",
        "regional_vars",        # road network accessibility

        "nrh_simulate",         # non-residential rent hedonic

        # uses conditional probabilities
        "households_relocation",
        "households_transition",
        # update building/unit/hh correspondence
        "reconcile_unplaced_households",

        "jobs_relocation",
        "jobs_transition",

        "balance_rental_and_ownership_hedonics",

        "price_vars",
        "scheduled_development_events",

        # run the subsidized acct system
        "lump_sum_accounts",
        "subsidized_residential_developer_lump_sum_accts",

        "alt_feasibility",

        "residential_developer",
        "developer_reprocess",
        "retail_developer",
        "office_developer",
        "accessory_units",

        # (for buildings that were removed)
        "remove_old_units",
        # set up units for new residential buildings
        "initialize_new_units",
        # update building/unit/hh correspondence
        "reconcile_unplaced_households",

        "rsh_simulate",     # residential sales hedonic for units
        "rrh_simulate",     # residential rental hedonic for units

        # (based on higher of predicted price or rent)
        "assign_tenure_to_new_units",

        # allocate owners to vacant owner-occupied units
        "hlcm_owner_simulate",
        # allocate renters to vacant rental units
        "hlcm_renter_simulate",

        # we have to run the hlcm above before this one - we first want to
        # try and put unplaced households into their appropraite tenured
        # units and then when that fails, force them to place using the
        # code below.  technically the hlcms above could be moved above the
        # developer again, but we would have to run the hedonics twice and
        # also the assign_tenure_to_new_units twice.

        # force placement of any unplaced households, in terms of rent/own
        # is a noop except in the final simulation year
        "hlcm_owner_simulate_no_unplaced",
        # this one crashes right no because there are no unplaced, so
        # need to fix the crash in urbansim
        "hlcm_renter_simulate_no_unplaced",

        # update building/unit/hh correspondence
        "reconcile_placed_households",

        "proportional_elcm",        # start with a proportional jobs model
        "elcm_simulate",            # displaced by new dev

        # save_intermediate_tables", # saves output for visualization

        "topsheet",
        "simulation_validation",
        "parcel_summary",
        "building_summary",
        "diagnostic_output",
        "geographic_summary",
        # "travel_model_output",
        # "travel_model_2_output",
        # "hazards_slr_summary",
        # "hazards_eq_summary"

    ]

    # calculate VMT taxes
    vmt_settings = \
        orca.get_injectable("settings")["acct_settings"]["vmt_settings"]
    if SCENARIO in vmt_settings["com_for_com_scenarios"]:
        models.insert(models.index("office_developer"),
                      "subsidized_office_developer")

    if SCENARIO in vmt_settings["com_for_res_scenarios"] or \
            SCENARIO in vmt_settings["res_for_res_scenarios"]:

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
            "preproc_buildings",
        ])

    elif MODE == "fetch_data":

        orca.run(["fetch_from_s3"])

    elif MODE == "debug":

        orca.run(["simulation_validation"], [2010])

    elif MODE == "simulation":

        # see above for docs on this
        if not SKIP_BASE_YEAR:
            orca.run([

                # "slr_inundate",
                # "slr_remove_dev",
                # "eq_code_buildings",
                # "earthquake_demolish",
                "load_rental_listings",
                "generate_skims_vars",
                "skims_aggregations_drive",
                "neighborhood_vars",   # local accessibility vars
                "regional_vars",       # regional accessibility vars

                "rsh_simulate",    # residential sales hedonic for units
                "rrh_simulate",    # residential rental hedonic for units
                "nrh_simulate",

                # (based on higher of predicted price or rent)
                "assign_tenure_to_new_units",

                # uses conditional probabilities
                "households_relocation",
                "households_transition",
                # update building/unit/hh correspondence
                "reconcile_unplaced_households",
                "jobs_transition",

                # allocate owners to vacant owner-occupied units
                "hlcm_owner_simulate",
                # allocate renters to vacant rental units
                "hlcm_renter_simulate",
                # update building/unit/hh correspondence
                "reconcile_placed_households",

                "elcm_simulate",

                "price_vars",

                "topsheet",
                "simulation_validation",
                "parcel_summary",
                "building_summary",
                "geographic_summary",
                # "travel_model_output",
                # "travel_model_2_output",
                # "hazards_slr_summary",
                # "hazards_eq_summary",
                "diagnostic_output"
            ],
                iter_vars=[IN_YEAR],
                data_out=DATA_OUT,
                out_base_tables=[],
                out_base_local=True,
                out_run_tables=OUT_TABLES,
                out_run_local=True
            )

        # start the simulation in the next round - only the models above run
        # for the IN_YEAR
        years_to_run = range(IN_YEAR + EVERY_NTH_YEAR, OUT_YEAR + 1,
                             EVERY_NTH_YEAR)
        models = get_simulation_models(SCENARIO)
        orca.run(
            models, iter_vars=years_to_run,
            data_out=DATA_OUT,
            out_interval=len(years_to_run),  # hack to store only last iter
            out_base_tables=[],
            out_base_local=True,
            out_run_tables=OUT_TABLES,
            out_run_local=True
        )

    elif MODE == "estimation":

        orca.run([
            "generate_skims_vars",
            "skims_aggregations_drive",
            "neighborhood_vars",         # local accessibility variables
            "regional_vars",             # regional accessibility variables
            "rsh_estimate",              # residential sales hedonic
            "nrh_estimate",              # non-res rent hedonic
            "rsh_simulate",
            "nrh_simulate",
            # "hlcm_estimate",             # household lcm
            "elcm_estimate",             # employment lcm

        ], iter_vars=[2010])

        # Estimation steps

        orca.run([
            "load_rental_listings",  # required to estimate rental hedonic
            "generate_skims_vars",
            "skims_aggregations_drive",
            "neighborhood_vars",        # street network accessibility
            "regional_vars",            # road network accessibility
            "rrh_estimate",         # estimate residential rental hedonic
            "rrh_simulate",
            "hlcm_owner_estimate",  # estimate location choice owners
            "hlcm_renter_estimate",  # estimate location choice renters
        ])

    elif MODE == "feasibility":

        orca.run([

            "neighborhood_vars",            # local accessibility vars
            # "regional_vars",                # regional accessibility vars

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
print "Current Branch : ", BRANCH.rstrip()
print "Current Commit : ", CURRENT_COMMIT.rstrip()
print "Current Scenario : ", orca.get_injectable('scenario').rstrip()

try:
    run_models(MODE, SCENARIO)

except Exception as e:
    print traceback.print_exc()
    raise e
    sys.exit(0)

print "Finished", time.ctime()

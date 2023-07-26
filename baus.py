from __future__ import print_function

import os
import sys
import time
import traceback
from baus import datasources
from baus import models
from baus import slr
from baus import earthquake
from baus import ual
from baus import validation
from baus.summaries import core_summaries, metrics, geographic_summaries, \
                            travel_model_summaries, hazards_summaries, \
                            affordable_housing_summaries
import numpy as np
import pandas as pd
import orca
import socket
import argparse
import warnings
from baus.utils import compare_summary
import urbansim
import urbansim_defaults
import orca
import orca_test
import pandana

warnings.filterwarnings("ignore")

# Suppress scientific notation in pandas output
pd.set_option('display.float_format', lambda x: '%.3f' % x)

SLACK = MAPS = "URBANSIM_SLACK" in os.environ
LOGS = True
RANDOM_SEED = True
INTERACT = False
MODE = "simulation"
S3 = False
EVERY_NTH_YEAR = 5
BRANCH = os.popen('git rev-parse --abbrev-ref HEAD').read()
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read()
COMPARE_TO_NO_PROJECT = True
NO_PROJECT = 611
IN_YEAR, OUT_YEAR = 2010, 2050
COMPARE_AGAINST_LAST_KNOWN_GOOD = False
LAST_KNOWN_GOOD_RUN = 182 


orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)

orca.add_injectable("base_year", IN_YEAR)

orca.add_injectable("slack_enabled", SLACK)

parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument(
    '-c', action='store_true', dest='console',
    help='run from the console (logs to stdout), no slack or maps')

parser.add_argument('-i', action='store_true', dest='interactive',
                    help='enter interactive mode after imports')

parser.add_argument('-k', action='store_true', dest='skip_base_year',
                    help='skip base year - used for debugging')

parser.add_argument('-y', action='store', dest='out_year', type=int,
                    help='The year to which to run the simulation.')

parser.add_argument('--mode', action='store', dest='mode',
                    help='which mode to run (see code for mode options)')

parser.add_argument('--random-seed', action='store_true', dest='random_seed',
                    help='set a random seed for consistent stochastic output')

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

SKIP_BASE_YEAR = options.skip_base_year

if options.mode:
    MODE = options.mode

if options.random_seed:
    RANDOM_SEED = True

if options.noslack:
    SLACK = False

if INTERACT:
    import code
    code.interact(local=locals())
    sys.exit()

run_name = orca.get_injectable("run_name")

if LOGS:
    print('***The Standard stream is being written to {}.log***'.format(run_name))
    sys.stdout = sys.stderr = open(os.path.join(orca.get_injectable("outputs_dir"), "%s.log") % run_name, 'w')

if RANDOM_SEED:
    np.random.seed(12)

if SLACK:
    from slacker import Slacker
    slack = Slacker(os.environ["SLACK_TOKEN"])
    host = socket.gethostname()

if MAPS:
    from baus.utils import ue_config, ue_files


@orca.step()
def slack_report(buildings, households, year):

    if SLACK and IN_YEAR:
        dropped_devproj_geomid = orca.get_injectable("devproj_len") - orca.get_injectable("devproj_len_geomid")
        dropped_devproj_proc =  orca.get_injectable("devproj_len_geomid") -  orca.get_injectable("devproj_len_proc")
        slack.chat.post_message(
            '#urbansim_sim_update',
            'Development projects for run %s on %s: %d to start, '
            '%d dropped by geom_id check, '
            '%d dropped by processing'
            % (run_name, host, orca.get_injectable("devproj_len"), dropped_devproj_geomid, dropped_devproj_proc), as_user=True)
            
    if SLACK and (len(households.building_id[households.building_id == -1])) > 0:
        slack.chat.post_message(
            '#urbansim_sim_update',
            'WARNING: unplaced households in %d for run %s on %s'
            % (year, run_name, host), as_user=True)


def get_simulation_models():

    # ual has a slightly different set of models - might be able to get rid
    # of the old version soon

    models = [

        "slr_inundate",
        "slr_remove_dev",
        "eq_code_buildings",
        "earthquake_demolish",

        "neighborhood_vars",    # street network accessibility
        "regional_vars",        # road network accessibility

        "nrh_simulate",         # non-residential rent hedonic

        # uses conditional probabilities
        "household_relocation",
        "households_transition",
        # update building/unit/hh correspondence
        "reconcile_unplaced_households",

        "jobs_relocation",
        "jobs_transition",

        "balance_rental_and_ownership_hedonics",

        "price_vars",
        "scheduled_development_events",

        # preserve some units
        "preserve_affordable",
        # run the subsidized residential acct system
        "lump_sum_accounts",
        "subsidized_residential_developer_lump_sum_accts",

        # run the subsidized office acct system
        "office_lump_sum_accounts",
        "subsidized_office_developer_lump_sum_accts",

        "alt_feasibility",
        "subsidized_residential_feasibility",
        "subsidized_residential_developer_vmt",
#        "subsidized_residential_feasibility",
#        "subsidized_residential_developer_jobs_housing",

        "residential_developer",
        "developer_reprocess",
        "retail_developer",

        "office_developer",
        "subsidized_office_developer_vmt",

        "accessory_units_strategy",
        "calculate_vmt_fees",

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

        # we first put Q1 households only into deed-restricted units, 
        # then any additional unplaced Q1 households, Q2, Q3, and Q4 
        # households are placed in either deed-restricted units or 
        # market-rate units
        "hlcm_owner_lowincome_simulate",
        "hlcm_renter_lowincome_simulate",

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
        # 09 11 2020 ET: enabled for all simulation years
        "hlcm_owner_simulate_no_unplaced",
        "hlcm_owner_lowincome_simulate_no_unplaced",
        # this one crashes right no because there are no unplaced, so
        # need to fix the crash in urbansim
        # 09 11 2020 ET: appears to be working
        "hlcm_renter_simulate_no_unplaced",
        "hlcm_renter_lowincome_simulate_no_unplaced",

        # update building/unit/hh correspondence
        "reconcile_placed_households",

        "proportional_elcm",        # start with a proportional jobs model
        "elcm_simulate",            # displaced by new dev

        # save_intermediate_tables", # saves output for visualization
        "calculate_vmt_fees",
        "calculate_jobs_housing_fees"

    ]

    run_setup = orca.get_injectable("run_setup")

    # sea level rise and sea level rise mitigation
    if not run_setup["run_slr"]:
        models.remove("slr_inundate")
        models.remove("slr_remove_dev")

    # earthquake and earthquake mitigation
    if not run_setup["run_eq"]:
        models.remove("eq_code_buildings")
        models.remove("earthquake_demolish")

    # housing preservation
    if not run_setup["run_housing_preservation_strategy"]:
        models.remove("preserve_affordable")

    # office bonds
    if not run_setup["run_office_bond_strategy"]:
        models.remove("office_lump_sum_accounts")
        models.remove("subsidized_office_developer_lump_sum_accts")

    if not run_setup["run_adu_strategy"]:
        models.remove("accessory_units_strategy")

    # VMT taxes
    if not run_setup["run_vmt_fee_com_for_com_strategy"]:
        models.remove("calculate_vmt_fees")
        models.remove("subsidized_office_developer_vmt")
    if not run_setup["run_vmt_fee_com_for_res_strategy"] or run_setup["run_vmt_fee_res_for_res_strategy"]:
        models.remove("calculate_vmt_fees")
        models.remove("subsidized_residential_feasibility")
        models.remove("subsidized_residential_developer_vmt")

    # jobs-housing fees- subsidization needs further testing
    if not run_setup["run_jobs_housing_fee_strategy"]:
        models.remove("calculate_jobs_housing_fees")
    #    models.remove["subsidized_residential_feasibility"]
    #    models.remove["subsidized_residential_developer_jobs_housing"]

    return models


def get_summary_models():

    summary_models = [

        "simulation_validation",
        "diagnostic_output",

        "hazards_slr_summary",
        "hazards_eq_summary",

        "parcel_summary",
        "parcel_growth_summary",
        "building_summary",

        "deed_restricted_units_summary",
        "deed_restricted_units_growth_summary",

        "geographic_summary",
        "geographic_growth_summary",

        "growth_geography_metrics",
        "deed_restricted_units_metrics",
        "household_income_metrics",
        "equity_metrics",
        "jobs_housing_metrics",
        "jobs_metrics",
        "slr_metrics",
        "earthquake_metrics",
        "greenfield_metrics",

        "taz1_summary",
        "maz_marginals",
        "maz_summary",
        "taz2_marginals",
        "county_marginals",
        "region_marginals",
        "taz1_growth_summary",
        "maz_growth_summary",

        "slack_report"

    ]

    return summary_models

def get_baseyear_models():

    baseyear_models = [
        
        "slr_inundate",
        "slr_remove_dev",
        "eq_code_buildings",
        "earthquake_demolish",

        "neighborhood_vars",   # local accessibility vars
        "regional_vars",       # regional accessibility vars

        "rsh_simulate",    # residential sales hedonic for units
        "rrh_simulate",    # residential rental hedonic for units
        "nrh_simulate",

        # (based on higher of predicted price or rent)
        "assign_tenure_to_new_units",

        # uses conditional probabilities
        "household_relocation",
        "households_transition",
        # update building/unit/hh correspondence
        "reconcile_unplaced_households",
        "jobs_transition",

        # we first put Q1 households only into deed-restricted units, 
        # then any additional unplaced Q1 households, Q2, Q3, and Q4 
        # households are placed in either deed-restricted units or 
        # market-rate units
        "hlcm_owner_lowincome_simulate",
        "hlcm_renter_lowincome_simulate",

        # allocate owners to vacant owner-occupied units
        "hlcm_owner_simulate",
        # allocate renters to vacant rental units
        "hlcm_renter_simulate",

        # we have to run the hlcm above before this one - we first want
        # to try and put unplaced households into their appropraite
        # tenured units and then when that fails, force them to place
        # using the code below.

        # force placement of any unplaced households, in terms of
        # rent/own, is a noop except in the final simulation year
        # 09 11 2020 ET: enabled for all simulation years
        "hlcm_owner_simulate_no_unplaced",
        "hlcm_owner_lowincome_simulate_no_unplaced",
        # this one crashes right no because there are no unplaced, so
        # need to fix the crash in urbansim
        # 09 11 2020 ET: appears to be working
        "hlcm_renter_simulate_no_unplaced",
        "hlcm_renter_lowincome_simulate_no_unplaced",

        # update building/unit/hh correspondence
        "reconcile_placed_households",

        "elcm_simulate",

        "price_vars"
        # "scheduled_development_events"

    ]

    run_setup = orca.get_injectable("run_setup")

    # sea level rise and sea level rise mitigation
    if not run_setup["run_slr"]:
        baseyear_models.remove("slr_inundate")
        baseyear_models.remove("slr_remove_dev")

    # earthquake and earthquake mitigation
    if not run_setup["run_eq"]:
        baseyear_models.remove("eq_code_buildings")
        baseyear_models.remove("earthquake_demolish")

    return baseyear_models

def get_baseyear_summary_models():

    baseyear_summary_models = [

        "simulation_validation",
        "diagnostic_output",

        "hazards_slr_summary",
        "hazards_eq_summary",

        "parcel_summary",
        "building_summary",

        "deed_restricted_units_summary",

        "geographic_summary",

        "growth_geography_metrics",
        "deed_restricted_units_metrics",
        "household_income_metrics",
        "equity_metrics",
        "jobs_housing_metrics",
        "jobs_metrics",
        "slr_metrics",
        "earthquake_metrics",
        "greenfield_metrics",

        "taz1_summary",
        "maz_marginals",
        "maz_summary",
        "taz2_marginals",
        "county_marginals",
        "region_marginals",

        "slack_report"
    ]

    return baseyear_summary_models


def run_models(MODE):

    if MODE == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings",
            "initialize_residential_units"
        ])

    elif MODE == "fetch_data":

        orca.run(["fetch_from_s3"])

    elif MODE == "debug":

        orca.run(["simulation_validation"], [2010])

    elif MODE == "simulation":

        run_setup = orca.get_injectable("run_setup")

        # see above for docs on this
        if not SKIP_BASE_YEAR:
            baseyear_models = get_baseyear_models()
            if run_setup["run_summaries"]:
                baseyear_models.extend(get_baseyear_summary_models())
            orca.run(baseyear_models, iter_vars=[IN_YEAR])

        # start the simulation in the next round - only the models above run
        # for the IN_YEAR
        years_to_run = range(IN_YEAR+EVERY_NTH_YEAR, OUT_YEAR+1,
                             EVERY_NTH_YEAR)
        models = get_simulation_models()
        if run_setup["run_summaries"]:
            models.extend(get_summary_models())
        orca.run(models, iter_vars=years_to_run)
        

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

        # Estimation steps
        '''
        orca.run([
            "load_rental_listings", # required to estimate rental hedonic
            "neighborhood_vars",        # street network accessibility
            "regional_vars",            # road network accessibility

            "rrh_estimate",         # estimate residential rental hedonic

            "hlcm_owner_estimate",  # estimate location choice owners
            "hlcm_renter_estimate", # estimate location choice renters
        ])
        '''

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


print("Started", time.ctime())
print("Current Branch : ", BRANCH.rstrip())
print("Current Commit : ", CURRENT_COMMIT.rstrip())
print("Random Seed : ", RANDOM_SEED)
print("python version: %s" % sys.version.split('|')[0])
print("urbansim version: %s" % urbansim.__version__)
print("urbansim_defaults version: %s" % urbansim_defaults.__version__)
print("orca version: %s" % orca.__version__)
print("orca_test version: %s" % orca_test.__version__)
print("pandana version: %s" % pandana.__version__)
print("numpy version: %s" % np.__version__)
print("pandas version: %s" % pd.__version__)


if SLACK and MODE == "simulation":
    slack.chat.post_message('#urbansim_sim_update', 'Starting simulation %s on host %s' % (run_name, host), as_user=True)

try:
    run_models(MODE)
except Exception as e:
    print(traceback.print_exc())
    if SLACK and MODE == "simulation":
        slack.chat.post_message('#urbansim_sim_update', 'DANG!  Simulation failed for %s on host %s' % (run_name, host), as_user=True)
    else:
        raise e
    sys.exit(0)

print("Finished", time.ctime())

if MAPS and MODE == "simulation" and 'travel_model_output' in get_simulation_models():
    files_msg1, files_msg2 = ue_files(run_name)
    config_resp = ue_config(run_name, host)

if SLACK and MODE == "simulation":
    slack.chat.post_message('#urbansim_sim_update', 'Completed simulation %d on host %s' % (run_name, host), as_user=True)

    """slack.chat.post_message(
        '#sim_updates',
        'Urbanexplorer is available at ' +
        'http://urbanforecast.com/sim_explorer%d.html' % run_name, as_user=True)

    slack.chat.post_message(
        '#sim_updates',
        'Final topsheet is available at ' +
        'http://urbanforecast.com/runs/run%d_topsheet_2050.log' % run_name,
        as_user=True)

    slack.chat.post_message(
        '#sim_updates',
        'Targets comparison is available at ' +
        'http://urbanforecast.com/runs/run%d_targets_comparison_2050.csv' %
        run_name, as_user=True)"""
    
    
summary = ""
if MODE == "simulation" and COMPARE_AGAINST_LAST_KNOWN_GOOD:
    # compute and write the difference report at the superdistrict level
    prev_run = LAST_KNOWN_GOOD_RUN
    # fetch the previous run off of the internet for comparison - the "last
    # known good run" should always be available on EC2
    df1 = pd.read_csv(("http://urbanforecast.com/runs/run%d_superdistrict" + "_summaries_2050.csv") % prev_run)
    df1 = df1.set_index(df1.columns[0]).sort_index()

    df2 = pd.read_csv((orca.get_injectable("outputs_dir")+"/run%d_superdistrict_summaries_2050.csv") % run_name)
    df2 = df2.set_index(df2.columns[0]).sort_index()

    supnames = pd.read_csv((orca.get_injectable("inputs_dir") + "/basis_inputs/crosswalks/superdistricts_geography.csv"), index_col="number").name

    summary = compare_summary(df1, df2, supnames)
    with open((orca.get_injectable("outputs_dir") + "/run%d_difference_report.log") % run_name, "w") as f:
        f.write(summary)


if SLACK and MODE == "simulation" and COMPARE_AGAINST_LAST_KNOWN_GOOD:

    if len(summary.strip()) != 0:
        sum_lines = len(summary.strip().split("\n"))
        slack.chat.post_message('#urbansim_sim_update', ('Difference report is available at ' +
                                'http://urbanforecast.com/run/run%d_difference_report.log ' +  '- %d line(s)') 
                                % (run_name, sum_lines), as_user=True)
    else:
        slack.chat.post_message('#urbansim_sim_update', "No differences with reference run.", as_user=True)

if S3:
    os.system('ls ' + orca.get_injectable("outputs_dir") + '/run%d_* ' % run_name + '| xargs -I file aws s3 cp file ' + 
              's3://bayarea-urbansim-results')
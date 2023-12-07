from __future__ import print_function
import os
import sys
import time
import traceback
from baus import \
    datasources, variables, models, subsidies, ual, slr, earthquake, \
    utils, preprocessing
from baus.tests import validation
from baus.summaries import \
    core_summaries, geographic_summaries, affordable_housing_summaries, \
    hazards_summaries, metrics, travel_model_summaries
from baus.visualizer import push_model_files
import numpy as np
import pandas as pd
import orca
import socket
import argparse
import urbansim
import urbansim_defaults
import orca
import orca_test
import pandana
import yaml


MODE = "simulation"
EVERY_NTH_YEAR = 5
IN_YEAR, OUT_YEAR = 2010, 2050


SLACK = "URBANSIM_SLACK" in os.environ
if SLACK:
    host = socket.gethostname()
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    client = WebClient(token=os.environ["SLACK_TOKEN"])
    slack_channel = "#urbansim_sim_update"

SET_RANDOM_SEED = True
if SET_RANDOM_SEED:
    np.random.seed(12)


parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument('--mode', action='store', dest='mode', help='which mode to run (see code for mode options)')
parser.add_argument('-i', action='store_true', dest='interactive', help='enter interactive mode after imports')
parser.add_argument('--set-random-seed', action='store_true', dest='set_random_seed', help='set a random seed for consistent stochastic output')
parser.add_argument('--disable-slack', action='store_true', dest='no_slack', help='disable slack outputs')

options = parser.parse_args()

if options.interactive:
    INTERACT = True

if options.mode:
    MODE = options.mode

if options.set_random_seed:
    SET_RANDOM_SEED = True

if options.no_slack:
    SLACK = False

orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)
orca.add_injectable("base_year", IN_YEAR)
orca.add_injectable("final_year", OUT_YEAR)

run_setup = orca.get_injectable("run_setup")
run_name = orca.get_injectable("run_name")


def run_models(MODE):

    if MODE == "estimation":

        orca.run([
            "neighborhood_vars",         
            "regional_vars",             
            "rsh_estimate",             
            "nrh_estimate",         
            "rsh_simulate",
            "nrh_simulate",
            "hlcm_estimate",           
            "elcm_estimate",         
        ])


    elif MODE == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings",
            "initialize_residential_units"
        ])


    elif MODE == "simulation":

        def get_baseyear_models():

            baseyear_models = [
            
                "slr_inundate",
                "slr_remove_dev",
                "eq_code_buildings",
                "earthquake_demolish",

                "neighborhood_vars",  
                "regional_vars",  

                "rsh_simulate",   
                "rrh_simulate", 
                "nrh_simulate",
                "assign_tenure_to_new_units",

                "household_relocation",
                "households_transition",

                "reconcile_unplaced_households",
                "jobs_transition",

                "hlcm_owner_lowincome_simulate",
                "hlcm_renter_lowincome_simulate",

                "hlcm_owner_simulate",
                "hlcm_renter_simulate",

                "hlcm_owner_simulate_no_unplaced",
                "hlcm_owner_lowincome_simulate_no_unplaced",
                "hlcm_renter_simulate_no_unplaced",
                "hlcm_renter_lowincome_simulate_no_unplaced",

                "reconcile_placed_households",

                "elcm_simulate",

                "price_vars"]

            if not run_setup["run_slr"]:
                baseyear_models.remove("slr_inundate")
                baseyear_models.remove("slr_remove_dev")

            if not run_setup["run_eq"]:
                baseyear_models.remove("eq_code_buildings")
                baseyear_models.remove("earthquake_demolish")

            return baseyear_models
    
        def get_baseyear_summary_models():

            baseyear_summary_models = [

                "simulation_validation",

                "parcel_summary",
                "building_summary",

                "hazards_slr_summary",
                "hazards_eq_summary",

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
            ]

            return baseyear_summary_models
    
        def get_simulation_models():
        
            simulation_models = [

                "slr_inundate",
                "slr_remove_dev",
                "eq_code_buildings",
                "earthquake_demolish",

                "neighborhood_vars",   
                "regional_vars",      

                "nrh_simulate",

                "household_relocation",
                "households_transition",

                "reconcile_unplaced_households",

                "jobs_relocation",
                "jobs_transition",

                "balance_rental_and_ownership_hedonics",

                "price_vars",
                "scheduled_development_events",

                "preserve_affordable",

                "lump_sum_accounts",
                "subsidized_residential_developer_lump_sum_accts",


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

                "remove_old_units",
                "initialize_new_units",
                "reconcile_unplaced_households",

                "rsh_simulate",   
                "rrh_simulate", 

                "assign_tenure_to_new_units",

                "hlcm_owner_lowincome_simulate",
                "hlcm_renter_lowincome_simulate",

                # the hlcms above could be moved above the developer again, 
                # but we would have to run the hedonics and assign tenure to units twice
                "hlcm_owner_simulate",
                "hlcm_renter_simulate",
                "hlcm_owner_simulate_no_unplaced",
                "hlcm_owner_lowincome_simulate_no_unplaced",
                "hlcm_renter_simulate_no_unplaced",
                "hlcm_renter_lowincome_simulate_no_unplaced",

                "reconcile_placed_households",

                "proportional_elcm",
                "elcm_simulate",  

                "calculate_vmt_fees",
                "calculate_jobs_housing_fees"]

            if not run_setup["run_slr"]:
                simulation_models.remove("slr_inundate")
                simulation_models.remove("slr_remove_dev")

            if not run_setup["run_eq"]:
                simulation_models.remove("eq_code_buildings")
                simulation_models.remove("earthquake_demolish")

            if not run_setup["run_housing_preservation_strategy"]:
                simulation_models.remove("preserve_affordable")

            if not run_setup["run_office_bond_strategy"]:
                simulation_models.remove("office_lump_sum_accounts")
                simulation_models.remove("subsidized_office_developer_lump_sum_accts")

            if not run_setup["run_adu_strategy"]:
                simulation_models.remove("accessory_units_strategy")

            if not run_setup["run_vmt_fee_com_for_com_strategy"]:
                simulation_models.remove("calculate_vmt_fees")
                simulation_models.remove("subsidized_office_developer_vmt")
            if not run_setup["run_vmt_fee_com_for_res_strategy"] or run_setup["run_vmt_fee_res_for_res_strategy"]:
                simulation_models.remove("calculate_vmt_fees")
                simulation_models.remove("subsidized_residential_feasibility")
                simulation_models.remove("subsidized_residential_developer_vmt")

            if not run_setup["run_jobs_housing_fee_strategy"]:
                simulation_models.remove("calculate_jobs_housing_fees")
            #    simulation_models.remove["subsidized_residential_feasibility"]
            #    simulation_models.remove["subsidized_residential_developer_jobs_housing"]

            return simulation_models
        

        def get_simulation_validation_models():

            simulation_validation_models = [
                "simulation_validation"
            ]

            return simulation_validation_models

        def get_simulation_summary_models():

            simulation_summary_models = [

                "interim_zone_output",
                "new_buildings_summary",

                "parcel_summary",
                "parcel_growth_summary",
                "building_summary",

                "hazards_slr_summary",
                "hazards_eq_summary",

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
            ]

            return simulation_summary_models
        
        
        def get_simulation_visualization_models():

            simulation_visualization_models = [
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
            ]

            return simulation_visualization_models

        baseyear_models = get_baseyear_models()
        if run_setup["run_summaries"]:
            baseyear_models.extend(get_baseyear_summary_models())
        orca.run(baseyear_models, iter_vars=[IN_YEAR])

        years_to_run = range(IN_YEAR+EVERY_NTH_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
        simulation_models = get_simulation_models()
        if run_setup["run_summaries"]:
            simulation_models.extend(get_simulation_summary_models())
        if run_setup["run_simulation_validation"]:
            simulation_models.extend(get_simulation_validation_models())
        orca.run(simulation_models, iter_vars=years_to_run)

        if run_setup["run_visualizer"]:
            visualization_models = get_simulation_visualization_models()
            orca.run(visualization_models, iter_vars=[OUT_YEAR])
            

    elif MODE == "visualizer":

        orca.run([
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
        ])


    else:
        raise "Invalid mode"

print('***Copying run_setup.yaml to output directory')
with open(os.path.join(orca.get_injectable("outputs_dir"), "run_setup.yaml"), 'w') as f:
    yaml.dump(orca.get_injectable("run_setup"), f, sort_keys=False)

print('***The Standard stream is being written to {}.log***'.format(run_name))
sys.stdout = sys.stderr = open(os.path.join(orca.get_injectable("outputs_dir"), "%s.log") % run_name, 'w')
                                       
print("Started", time.ctime())
print("Current Branch : ", os.popen('git rev-parse --abbrev-ref HEAD').read().rstrip())
print("Current Commit : ", os.popen('git rev-parse HEAD').read().rstrip())
print("Set Random Seed : ", SET_RANDOM_SEED)
print("python version: %s" % sys.version.split('|')[0])
print("urbansim version: %s" % urbansim.__version__)
print("urbansim_defaults version: %s" % urbansim_defaults.__version__)
print("orca version: %s" % orca.__version__)
print("orca_test version: %s" % orca_test.__version__)
print("pandana version: %s" % pandana.__version__)
print("numpy version: %s" % np.__version__)
print("pandas version: %s" % pd.__version__)


if SLACK and MODE == "simulation":
    slack_start_message = f'Starting simulation {run_name} on host {host}'
    try:
        # For first slack channel posting of a run, catch any auth errors
        response = client.chat_postMessage(channel=slack_channel,text=slack_start_message)
    except SlackApiError as e:
        assert e.response["ok"] is False
        assert e.response["error"]  
        print(f"Slack Channel Connection Error: {e.response['error']}")

try:
    run_models(MODE)
except Exception as e:
    print(traceback.print_exc())
    if SLACK and MODE == "simulation":
        slack_fail_message = f'DANG!  Simulation failed for {run_name} on host {host}'
        response = client.chat_postMessage(channel=slack_channel,text=slack_fail_message)

    else:
        raise e
    sys.exit(0)

if SLACK and MODE == "simulation":
    slack_completion_message = f'Completed simulation {run_name} on host {host}'
    response = client.chat_postMessage(channel=slack_channel,text=slack_completion_message)

                                                                                            
print("Finished", time.ctime())         
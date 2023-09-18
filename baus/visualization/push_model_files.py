import pandas as pd
import numpy as np
import orca
import os


@orca.step()
def copy_files_to_viz_loc(run_name, outputs_dir, viz_dir):

    # copy files to BAUS output to the Visualizer storage location
    # and add a run_name column for use in Tableau
    
    taz_output_file = pd.read_csv(os.path.join(outputs_dir, "travel_model_summaries/{}_taz1_summary_growth.csv").format(run_name))
    taz_output_file["run_name"] = run_name
    taz_output_file.to_csv(os.path.join(viz_dir, "{}_taz1_summary_growth.csv").format(run_name))

    for geog in ["juris", "superdistrict", "county"]:
        output_file = pd.read_csv(os.path.join(outputs_dir, "geographic_summaries/{}_{}_summary_growth.csv").format(run_name, geog))
        output_file["run_name"] = run_name
        output_file.to_csv(os.path.join(viz_dir, "{}_{}_summary_growth.csv").format(run_name, geog))


@orca.step()
def add_to_model_run_inventory_file(run_name): 

    # check for the model run inventory file to add the run to
    try:
        model_run_inventory = pd.read_csv(os.path.join(orca.get_injectable("viz_dir"), "model_run_inventory.csv"))
    except:
        model_run_inventory = pd.DataFrame(columns={[run_name]}) 

    # append the run name to the list if not already there
    if run_name not in list(model_run_inventory['run_name']):
        model_run_inventory.loc[len(model_run_inventory.index)] = run_name

    # write out the updated inventory table
    model_run_inventory.to_csv(os.path.join(orca.get_injectable("viz_dir"), "model_run_inventory.csv"))
import pandas as pd
import numpy as np
import orca
import os
import pathlib


@orca.step()
def copy_files_to_viz_loc(run_name, outputs_dir, viz_dir):

    # copy files to BAUS output to the Visualizer storage location
    # and add a run_name column for use in Tableau

    # Create viz_dir if doesn't exist
    viz_dir_path = pathlib.Path(viz_dir)
    viz_dir_path.mkdir(parents=True, exist_ok=True)

    outputs_dir_path = pathlib.Path(outputs_dir)

    new_buildings = pd.read_csv(outputs_dir_path / "core_summaries" / f"{run_name}_new_buildings_summary.csv")
    new_buildings["run_name"] = run_name
    new_buildings.to_csv(viz_dir_path / f"{run_name}_new_buildings_summary.csv")
    
    taz_output_file = pd.read_csv(outputs_dir_path / "travel_model_summaries" / f"{run_name}_taz1_summary_growth.csv")
    taz_output_file["run_name"] = run_name
    taz_output_file.to_csv(viz_dir_path / f"{run_name}_taz1_summary_growth.csv")
    
    interim_taz_output_file = pd.read_csv(outputs_dir_path / "core_summaries" / f"{run_name}_interim_zone_output_allyears.csv")
    interim_taz_output_file["run_name"] = run_name
    interim_taz_output_file.to_csv(viz_dir_path / f"{run_name}_interim_zone_output_allyears.csv")

    for geog in ["juris", "superdistrict", "county"]:
        output_file = pd.read_csv(outputs_dir_path / "geographic_summaries" / f"{run_name}_{geog}_summary_growth.csv")
        output_file["run_name"] = run_name
        output_file.to_csv(viz_dir_path / f"{run_name}_{geog}_summary_growth.csv")


@orca.step()
def add_to_model_run_inventory_file(run_name):

    # Create viz_dir if doesn't exist
    viz_dir_path = pathlib.Path(orca.get_injectable("viz_dir"))
    viz_dir_path.mkdir(parents=True, exist_ok=True)

    # check for the model run inventory file to add the run to
    try:
        model_run_inventory = pd.read_csv(viz_dir_path / "model_run_inventory.csv")
    except FileNotFoundError:
        model_run_inventory = pd.DataFrame(columns=['run_name'])

    # append the run name to the list if not already there
    if run_name not in list(model_run_inventory['run_name']):
        model_run_inventory.loc[len(model_run_inventory.index)] = run_name

    # write out the updated inventory table
    model_run_inventory.to_csv(viz_dir_path / "model_run_inventory.csv", index=False)

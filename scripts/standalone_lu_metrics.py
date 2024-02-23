"""
Script Name: Standalone Land Use Metrics Calculation

Description:
This script dynamically loads data for various Bay Area UrbanSim model runs within a specified project directory,
calculates a set of metrics defined in the performance report related to land use, and aggregates these metrics across all
runs into a single CSV file for analysis and review.

Requirements:
- Python 3.6+
- pandas and pathlib, and logging libraries
- Access to the following folders on the M Drive:
    - M:/urban_modeling/baus/BAUS Inputs
    - M:/urban_modeling/baus/PBA50Plus

Usage:
Ensure the base path and crosswalk file path are correctly set to match the project directory structure.
Run the script in a Python environment where the required libraries are installed.

Functions:
- discover_model_runs: Discovers available model run directories.
- load_data_for_runs: Loads summary data for each discovered model run.
- load_crosswalk_data: Loads the parcels_geography file to complement the parcels characteristics.
- merge_with_crosswalk: Merges parcel summary dfs with the parcels geography df. 
- calculate_metrics: Calculates metrics for each model run.
- extract_ids_from_path:
- assemble_results:
- deed_restricted_affordable_share:
- new_prod_deed_restricted_affordable_share:
- low_income_households_share:
- job_housing_ratio:
- 
-
- 
- main: Orchestrates the workflow of the script.
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def discover_model_runs(base_path):
    """
    This function finds Plan Bay Area 50 Plus BAUS model run directories, which are expected to start with "PBA50Plus" 
    and contain subdirectories named "core_summaries" and "geographic_summaries".

     Parameters:
    - base_path (Path): The base directory path to search for model run directories.

    Returns:
    - list of tuples: A list where each tuple contains the Path object for a model run directory, a list of Paths to its 
                     "core_summaries" subdirectories, and a list of Paths to its "geographic_summaries" subdirectories. 
                     Only directories containing both "core_summaries" and "geographic_summaries" are included.
    """
    model_run_dirs = []
    for pba_dir in base_path.glob("PBA50Plus*"):
        core_summaries_paths = list(pba_dir.rglob("core_summaries"))
        geographic_summaries_paths = list(pba_dir.rglob("geographic_summaries"))
        if core_summaries_paths and geographic_summaries_paths:
            model_run_dirs.append((pba_dir, core_summaries_paths, geographic_summaries_paths))
            logging.info(f"Discovered model run: {pba_dir}")
            logging.info(f"Core summaries found: {len(core_summaries_paths)}, Geographic summaries found: {len(geographic_summaries_paths)}")
    return model_run_dirs

def load_data_for_runs(core_summaries_paths, geographic_summaries_paths):
    """
    Loads core and geographic summary data for various BAUS model runs. This function searches for files matching patterns 
    indicating summary data for the years 2020 and 2050, loads them into DataFrames, and aggregates these
    DataFrames into two lists: one for core summaries and one for geographic summaries.

    Parameters:
    - core_summaries_paths (list of Path objects): Paths to directories containing core summary files.
    - geographic_summaries_paths (list of Path objects): Paths to directories containing geographic summary files.

    Returns:
    - tuple of two lists: 
        - The first list contains DataFrames loaded from core summary files.
        - The second list contains DataFrames loaded from geographic summary files.
    
    Both lists include DataFrames for files that match the year-specific patterns, assuming files for both 2020 and 2050 are present.
    """
    core_summary_dfs = []
    geographic_summary_dfs = []

    # Patterns to match files ending with the specified years
    core_summary_patterns = ["*_parcel_summary_2020.csv", "*_parcel_summary_2050.csv"]
    geo_summary_patterns = ["*_county_summary_2020.csv", "*_county_summary_2050.csv"]

    # Load core summaries if files matching both patterns exist
    for core_path in core_summaries_paths:
        matching_files = [list(core_path.glob(pattern)) for pattern in core_summary_patterns]
        if all(matching_files):  # Check if there are files for both patterns
            for file_list in matching_files:
                for file in file_list:
                    df = pd.read_csv(file) # consider adding usecols when all metrics are defined to increase the speed
                    logging.info(f"Loaded file {file} with shape {df.shape}")
                    core_summary_dfs.append(df)

    # Load geographic summaries if files matching both patterns exist
    for geo_path in geographic_summaries_paths:
        matching_files = [list(geo_path.glob(pattern)) for pattern in geo_summary_patterns]
        if all(matching_files):  
            for file_list in matching_files:
                for file in file_list:
                    df = pd.read_csv(file)
                    logging.info(f"Loaded file {file} with shape {df.shape}")
                    geographic_summary_dfs.append(df)

    return core_summary_dfs, geographic_summary_dfs

# Load the crosswalk dataset
def load_crosswalk_data(crosswalk_path):
    """
    This function loads the crosswalk data called parcels_geography from the BAUS Inputs folder. This dataset map parcel IDs to other geographic identifiers, 
    facilitating the enrichment of parcel summary data with additional geographic information.

    Parameters: 
    - crosswalk_path (str or Path): The file path to the most recent parcels_geography CSV file.

    Returns:
    - pandas.DataFrame: A DataFrame containing the loaded crosswalk data.
    
    """
    parcels_geography = pd.read_csv(crosswalk_path, na_values="NA")
    return parcels_geography

# This function should be called after loading each run's data and before calculating metrics.
def merge_with_crosswalk(parcel_summary_dfs, parcels_geography):
    """
    This function merges each DataFrame in a list of parcel summary DataFrames with the parcels geography DataFrame on parcel ID values. 

    Parameters: 
    - parcel_summary_dfs (list of pandas.DataFrame): List of DataFrames containing parcel summary data.
    - parcels_geography (pandas.DataFrame): DataFrame containing the crosswalk data mapping parcel IDs to parcel summary data.

    Returns:
    - list of pandas.DataFrame: A list of DataFrames, each showing a parcel summary DataFrame merged with the parcels_geography data.
    """
    merged_dfs = []
    for df in parcel_summary_dfs:
        merged_df = df.merge(parcels_geography, left_on="parcel_id", right_on="PARCEL_ID", how="left")
        merged_dfs.append(merged_df)
    return merged_dfs

def calculate_metrics(run_name, core_summary_dfs, geographic_summary_dfs, modelrunID, blueprint):
    """
    Calculates performance metrics various Bay Area UrbanSim model run.

    This function aggregates results from multiple metrics calculations, including deed-restricted affordable share,
    new production deed-restricted affordable share, low-income households share, and job-housing ratio. It utilizes
    data from core and geographic summaries to compute these metrics and then combines them into a single DataFrame.

    Parameters:
    - run_name (str): The name of the model run. # this one might be removed or modified
    - core_summary_dfs (list of pd.DataFrame): A list containing two dataframes with core summary data for different years.
    - geographic_summary_dfs (list of pd.DataFrame): A list containing two dataframes with geographic summary data for different years.
    - modelrunID (str): An identifier for the model run.
    - blueprint (str): The blueprint code associated with the model run.

    Returns:
    - pd.DataFrame: A dataframe containing aggregated results from all metrics calculations, with each metric's results
                    appended as rows.
    """
    logging.info(f"Starting metrics calculation for {modelrunID} with blueprint {blueprint}")
    # Initialize an empty DataFrame for aggregated results
    aggregated_results = pd.DataFrame()

    if len(core_summary_dfs) == 2 and len(geographic_summary_dfs) == 2:
        dr_share_results = deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrunID, blueprint)
        new_prod_dr_share_results = new_prod_deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrunID, blueprint)
        li_households_share_results = low_income_households_share(core_summary_dfs[0], core_summary_dfs[1], modelrunID, blueprint)
        job_housing_ratio_results = job_housing_ratio(geographic_summary_dfs[0], geographic_summary_dfs[1], modelrunID, blueprint)

        # Aggregate the results
        all_results = pd.concat([dr_share_results, new_prod_dr_share_results, li_households_share_results, job_housing_ratio_results], ignore_index=True)
        all_results['run_name'] = run_name  # Add the run name to all results

        aggregated_results = pd.concat([aggregated_results, all_results], ignore_index=True)

    return aggregated_results

def extract_ids_from_path(path):
    """
    Extracts the model run identifier (modelrunID) and blueprint code from a given path.
    
    The function searches for the segment starting with "PBA50Plus" in the path, using it to derive the modelrunID.
    It identifies the blueprint by extracting the segment immediately following "PBA50Plus_" in the path.

    Parameters:
    - path (str): The path to a model run directory.

    Returns:
    - tuple: A tuple containing two strings: (modelrunID, blueprint), where modelrunID is the full path from "PBA50Plus"
             and blueprint is the segment immediately following "PBA50Plus_" in the path. Returns "Unknown" for either
             if the expected structure is not found.
    """
    parts = path.split("\\")
    blueprint = "Unknown"  # Default to "Unknown" if not found
    
    # Attempt to find the "PBA50Plus" part and extract the blueprint
    for part in parts:
        if "PBA50Plus" in part:
            segments = part.split("_")
            if len(segments) > 2:  # Checks if there are segments after "PBA50Plus"
                blueprint = segments[1]  # The segment immediately after "PBA50Plus"
                break  # Exit the loop once the blueprint is found
    
    # Reconstruct the modelrunID from the path, starting from "PBA50Plus"
    if "PBA50Plus" in path:
        modelrunID_start_index = path.find("PBA50Plus")
        modelrunID = path[modelrunID_start_index:].replace("\\", "/")  # Use forward slash for consistency
    else:
        modelrunID = "Unknown"
    
    return modelrunID, blueprint

def assemble_results(modelrunID, metric_category, metric_names, metric_years, blueprint, metric_values):
    """
    Assemble multi-year metric results into a DataFrame.

    Parameters:
    - modelrunID: str, identifier for the model run derived from the file path
    - metric_category: str, the code for the metric category (e.g., 'A2')
    - metric_names: list of str, names of the metrics
    - metric_years: list of int, the years for which metrics are calculated
    - blueprint: str, name of the blueprint derived from the model run identifier
    - metric_values: list of tuples/lists, each containing values for the metric across the specified years

    Returns:
    - pd.DataFrame with the assembled metric results.
    """
    results_list = []
    for name, share_values in zip(metric_names, metric_values):
        for year, value in zip(metric_years, share_values):
            results_list.append({
                "modelrunID": modelrunID,
                "metric": metric_category,
                "name": name,
                "year": year,
                "blueprint": blueprint,
                "value": value
            })

    return pd.DataFrame(results_list)

# ==============================
# ===== Affordable Metrics =====
# ==============================
def deed_restricted_affordable_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrunID, blueprint):
    """
    Calculate the share of housing that is deed-restricted affordable in 2020 and 2050
    at the regional level, within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - parcel_geog_summary_2020: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - parcel_geog_summary_2050: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    
    Returns:
    - result_dr_share: pd.DataFrame
        DataFrame containing the share of housing that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """
    def calculate_share_dr_units(df_2020, df_2050):
        total_units_2020 = df_2020['residential_units'].sum()
        total_units_2050 = df_2050['residential_units'].sum()
        deed_restricted_units_2020 = df_2020['deed_restricted_units'].sum()
        deed_restricted_units_2050 = df_2050['deed_restricted_units'].sum()
        dr_units_share_2020 = round((deed_restricted_units_2020 / total_units_2020), 2) if total_units_2020 > 0 else 0
        dr_units_share_2050 = round((deed_restricted_units_2050 / total_units_2050), 2) if total_units_2050 > 0 else 0
        
        return (dr_units_share_2020, dr_units_share_2050)

    # Overall calculation
    overall_dr_units_share = calculate_share_dr_units(parcel_geog_summary_2020, parcel_geog_summary_2050)

    # Calculation for HRA
    hra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['hra_id'] == 'HRA']
    hra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['hra_id'] == 'HRA']
    hra_dr_units_share = calculate_share_dr_units(hra_filter_2020, hra_filter_2050)

    # Calculation for EPC
    epc_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['epc_id'] == 'EPC']
    epc_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['epc_id'] == 'EPC']
    epc_dr_units_share = calculate_share_dr_units(epc_filter_2020, epc_filter_2050)

    # Define the metrics and years for iteration
    metrics_names = ["deed_restricted_affordable_share_total",
                     "deed_restricted_affordable_share_hra",
                     "deed_restricted_affordable_share_epc"]

    metrics_values = [overall_dr_units_share, hra_dr_units_share, epc_dr_units_share]
    metrics_years = [2020, 2050]
    result_dr_units_share = assemble_results(modelrunID, 'A2', metrics_names, metrics_years, blueprint, metrics_values)
    return result_dr_units_share

def new_prod_deed_restricted_affordable_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrunID, blueprint):
    """
    Calculate the share of new housing production that is deed-restricted affordable between 2020 and 2050
    at the regional level, within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - parcel_geog_summary_2020: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - parcel_geog_summary_2050: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    
    Returns:
    - result_dr_share: pd.DataFrame
        DataFrame containing the share of new housing production that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """
    def calculate_share_new_dr_units(df_2020, df_2050):
        total_units_2020 = df_2020['residential_units'].sum()
        total_units_2050 = df_2050['residential_units'].sum()
        deed_restricted_units_2020 = df_2020['deed_restricted_units'].sum()
        deed_restricted_units_2050 = df_2050['deed_restricted_units'].sum()

        total_increase = total_units_2050 - total_units_2020
        deed_restricted_increase = deed_restricted_units_2050 - deed_restricted_units_2020

        return round(deed_restricted_increase / total_increase, 2) if total_increase > 0 else 0

    # Overall calculation
    overall_new_prod_dr_units_share = calculate_share_new_dr_units(parcel_geog_summary_2020, parcel_geog_summary_2050)

    # Calculation for HRA
    hra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['hra_id'] == 'HRA']
    hra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['hra_id'] == 'HRA']
    hra_dr_units_share = calculate_share_new_dr_units(hra_filter_2020, hra_filter_2050)

    # Calculation for EPC
    epc_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['epc_id'] == 'EPC']
    epc_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['epc_id'] == 'EPC']
    epc_dr_units_share = calculate_share_new_dr_units(epc_filter_2020, epc_filter_2050)

    # Define the metrics and years for iteration
    metrics_names = ["new_prod_deed_restricted_affordable_share_total",
                     "new_prod_deed_restricted_affordable_share_hra",
                     "new_prod_deed_restricted_affordable_share_epc"]
    
    metrics_values = [[overall_new_prod_dr_units_share], [hra_dr_units_share], [epc_dr_units_share]]
    metrics_years = [2050]
    result_new_dr_units_share = assemble_results(modelrunID, 'A2', metrics_names, metrics_years, blueprint, metrics_values)
    return result_new_dr_units_share

# ===============================
# ====== Connected Metrics ======
# ===============================






# ==============================
# ====== Diverse Metrics =======
# ==============================
def low_income_households_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrunID, blueprint):
    """
    Calculate the share of households that are low-income in Transit-Rich Areas (TRA), High-Resource Areas (HRA), and both
    
    Parameters:
    - parcel_geog_summary_2020: pd.DataFrame
        DataFrame with columns ['hra_id', 'tra_id', 'tothh', 'hhq1'].
    - parcel_geog_summary_2050: pd.DataFrame
        DataFrame with columns ['hra_id', 'tra_id', 'tothh', 'hhq1'].
    
    Returns:
    - result_hh_share: pd.DataFrame
        DataFrame containing the share of low-income households at the regional level and for TRAs, HRAs, and both.
    """
    def calculate_share_low_inc(df_2020, df_2050):
        total_hhs_2020 = df_2020['tothh'].sum()
        total_hhs_2050 = df_2050['tothh'].sum()
        low_inc_hhs_2020 = df_2020['hhq1'].sum()
        low_inc_hhs_2050 = df_2050['hhq1'].sum()
        low_inc_hh_share_2020 = round((low_inc_hhs_2020 / total_hhs_2020), 2) if total_hhs_2020 > 0 else 0
        low_inc_hh_share_2050 = round((low_inc_hhs_2050 / total_hhs_2050), 2) if total_hhs_2050 > 0 else 0
        
        return low_inc_hh_share_2020, low_inc_hh_share_2050
    
    # Overall calculation
    overall_low_inc_hh_share = calculate_share_low_inc(parcel_geog_summary_2020, parcel_geog_summary_2050)

    # Calculation for HRA
    hra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['hra_id'] == 'HRA']
    hra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['hra_id'] == 'HRA']
    hra_low_inc_hh_share = calculate_share_low_inc(hra_filter_2020, hra_filter_2050)

    # Calculation for TRA
    tra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])]
    tra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])]
    tra_low_inc_hh_share = calculate_share_low_inc(tra_filter_2020, tra_filter_2050)

    # Calculation for both TRA and HRA
    tra_hra_filter_2020 = parcel_geog_summary_2020[(parcel_geog_summary_2020['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']))&(parcel_geog_summary_2020['hra_id'] == 'HRA')]
    tra_hra_filter_2050 = parcel_geog_summary_2050[(parcel_geog_summary_2050['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']))&(parcel_geog_summary_2050['hra_id'] == 'HRA')]
    tra_hra_low_inc_hh_share = calculate_share_low_inc(tra_hra_filter_2020, tra_hra_filter_2050)

    # Define the metrics and years for iteration
    metrics_names = ["low_income_households_share_total",
                     "low_income_households_share_hra",
                     "low_income_households_share_tra",
                     "low_income_households_share_tra_hra"]
    metrics_values = [overall_low_inc_hh_share, hra_low_inc_hh_share, tra_low_inc_hh_share, tra_hra_low_inc_hh_share]
    metrics_years = [2020, 2050]
    result_hh_share = assemble_results(modelrunID, 'D1', metrics_names, metrics_years, blueprint, metrics_values)
    return result_hh_share

# ==============================
# ====== Healthy Metrics =======
# ==============================






# ==============================
# ====== Vibrant Metrics =======
# ==============================
def job_housing_ratio(county_summary_2020, county_summary_2050, modelrunID, blueprint):
    """
    Calculate the jobs-to-housing ratio for 2020 and 2050 for each county and the whole region.
    
    Parameters:
    - county_summary_2020: pd.DataFrame
        DataFrame with columns ['county', 'totemp', 'residential_units'].
    - county_summary_2050: pd.DataFrame
        DataFrame with columns ['county', 'totemp', 'residential_units'].
    
    Returns:
    - result_jh_ratio: pd.DataFrame
        DataFrame containing the jobs-to-housing ratio for each county and the whole region for the initial and final plan years.
    """
    def calculate_ratio(df, year):
        # Initialize results list
        results = []
        # Calculate jobs-housing ratio for the entire region
        total_jobs = df['totemp'].sum()
        total_units = df['residential_units'].sum()
        jh_region_ratio = round(total_jobs / total_units, 2) if total_units > 0 else 0
        # Add region result to list
        results.append(('jobs_housing_ratio_region', jh_region_ratio))
        # Calculate jobs-housing ratio for each county and add to list
        for _, row in df.iterrows():
            county_ratio = round(row['totemp'] / row['residential_units'], 2) if row['residential_units'] > 0 else 0
            county_name = f"jobs_housing_ratio_{row['county'].lower().replace(' ', '_')}"
            results.append((county_name, county_ratio))
        return results

    # Calculate ratios for 2020 and 2050
    ratios_2020 = calculate_ratio(county_summary_2020, 2020)
    ratios_2050 = calculate_ratio(county_summary_2050, 2050)
    
    # Extract metric names and values separately for assembly
    metric_names = [name for name, _ in ratios_2020]  
    metric_values_2020 = [value for _, value in ratios_2020]
    metric_values_2050 = [value for _, value in ratios_2050]
    metric_values = list(zip(metric_values_2020, metric_values_2050))
    
    # Use the assemble_results function to structure the output
    result_jh_ratio = assemble_results(modelrunID, 'V1', metric_names, [2020, 2050], blueprint, metric_values)
    return result_jh_ratio

def main():
    # Define paths and load crosswalk data
    base_path = Path("M:/urban_modeling/baus")
    model_inputs_path = base_path / "BAUS Inputs"
    pba50plus_path = base_path / "PBA50Plus"
    crosswalk_path = model_inputs_path / "basis_inputs/crosswalks/parcels_geography_2024_02_14.csv"
    
    logging.info("Loading crosswalk data...")
    parcels_geography = load_crosswalk_data(crosswalk_path)

    logging.info("Discovering model runs...")
    model_runs = discover_model_runs(pba50plus_path)
    
    # Initialize an empty DataFrame to collect all metrics
    all_metrics = pd.DataFrame()

    for run, core_paths, geo_paths in model_runs:
        logging.info(f"Processing model run: {run}")
        # Load data for the current run
        core_dfs, geo_dfs = load_data_for_runs(core_paths, geo_paths)

        # Merge the loaded data with the crosswalk data
        core_dfs_merged = [df.merge(parcels_geography, left_on="parcel_id", right_on="PARCEL_ID", how="left") for df in core_dfs]

        # Extract 'modelrunID' and 'blueprint' for the current model run directory
        modelrunID, blueprint = extract_ids_from_path(str(run))

        # Calculate metri cs for the current run
        run_metrics = calculate_metrics(run.name, core_dfs_merged, geo_dfs, blueprint, modelrunID)

        # Collect the metrics from all runs
        all_metrics = pd.concat([all_metrics, run_metrics], ignore_index=True)

    logging.info("All model runs processed. Saving results...")
    all_metrics.to_csv(pba50plus_path / "Metrics" / "landuse_metrics_test_1.csv", index=False)

if __name__ == "__main__":
    main()
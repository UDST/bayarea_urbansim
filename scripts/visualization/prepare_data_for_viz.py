USAGE = """
Prepares UrbanSim model run data (output, key input, interim) for (Tableau) dashboard visualization.

Example call: python prepare_data_for_viz.py 
              "C:\\Users\\ywang\\Box\\Modeling and Surveys\\Urban Modeling\\Bay Area UrbanSim\\PBA50Plus_Development\\Clean Code PR #3"
              "C:\\Users\\ywang\\Box\\Modeling and Surveys\\Urban Modeling\\Bay Area UrbanSim\\PBA50Plus_Development\\Clean Code PR #3\\bayarea_urbansim"
              "s24" "Model Dev PR3" "model development test run for PR #3" "run143" --upload_to_redshift

Args: 
    run_dir: directory of model run data
    model_code_dir: directory of model code, with model setting/assumption yaml files
    run_scenario: e.g. 's24' for Final Blueprint, 's28' for EIR Alt2
    scenario_group: e.g. Final Blueprint, EIR Alt2, DevRun
    run_description: description of the scenario and/or run group
    run_id: model run_id from the output, e.g. 'run182' for the official Final Blueprint run
    --upload_to_redshift: whether to upload data to Redshift, defaults to False unless calls it
    --upload_to_agol: whether to upload data to ArcGIS Online, defaults to False unless calls it
    --last_run: 'yes'/'no' if the run is the latest run of the same scenario


Returns:
    Model input - settings:
    [RUN_ID]_model_settings.csv
    [RUN_ID]_regional_controls.csv

    Model input - pipeline and strategies:
    [RUN_ID]_pipeline.csv
    [RUN_ID]_strategy_projects.csv
    [RUN_ID]_strategy_upzoning.csv
    [RUN_ID]_baseline_IZ.csv
    [RUN_ID]_strategy_IZ.csv
    [RUN_ID]_strategy_housingBond.csv
    [RUN_ID]_strategy_devCost.csv
    [RUN_ID]_strategy_unitPreserve_target.csv
    [RUN_ID]_strategy_unitPreserve.csv

    Model output:
    [RUN_ID]_taz_summary.csv: TAZ-level key model output metrics; created from model taz output
    [RUN_ID]_juris_summary.csv: jurisdiction-level key model output metrics; created from model jurisdiction output
    [RUN_ID]_growth_geos_summary.csv: key model output metrics by Plan growth geographies; created from model geos output
    [RUN_ID]_parcel_output.csv: key metrics on individual development projects (buildings); created from model parcel_output output
    [RUN_ID]_dr_units_growth.csv: growth of deed-restricted residential units by source/Plan Strategy; created from model jurisdiction output
    
    Model run index:
    model_run_inventory.csv: not newly created by running this script, but updated to include the new run's info.
  
"""

import pandas as pd
from pandas import DataFrame
import numpy as np
import geopandas as gpd
import argparse, sys, logging, time, os, glob
import yaml
from typing import Any, Dict, Optional

# the following packages and config is used to upload data to Redshift AWS
import getpass
import boto3

user = getpass.getuser()
sys.dont_write_bytecode = True

# for Windows - assuming the 'dvutils' repo has been cloned to local drive, the following location
# TODO: make this directory a user input instead of hard-coded
util_repo_path = os.path.join('C:\\Users',
                        user,
                        'OneDrive - Metropolitan Transportation Commission\\Documents\\GitHub\\dvutils')
sys.path.insert(0, util_repo_path)
from utils_io import *

# get date for the logging file
today = time.strftime("%Y_%m_%d")

############ helpers

# dictionary to recode building 'source' - appear both in 'parcel_output' and 'development_projects_list'
building_source_recode = {
    'manual'            : 'Pipeline',
    'cs'                : 'Pipeline',
    'basis'             : 'Pipeline',
    'bas_bp_new'        : 'Pipeline', 
    'rf'                : 'Pipeline',
    'opp'               : 'Pipeline',

    'developer_model'   : 'Developer Model',
    
    'h5_inputs'         : 'H5 Inputs',
    
    'pub'               : 'Public Land',
    'mall_office'       : 'Mall & Office Park',
    'incubator'         : 'Incubator',
    'ppa'               : 'PPA'
}
# currently 'development_projects_list' contains both pipeline and strategy-based projects,
# based on the following spec 
pipeline_src = ['manual', 'cs', 'basis', 'bas_bp_new', 'rf', 'opp']
strategy_proj_src = ['pub', 'mall_office', 'incubator', 'ppa']

# dictionaries to recode growth geography types, and categories in each type
geo_tag_types = {
    'DIS': 'Displacement-risk',
    'GG':  'GG',
    'HRA': 'High-resource',
    'TRA': 'Transit-rich'}
geos_tag_recode = {
    'yes_DIS': 'in_DIS',
    'no_DIS': 'outside_DIS',
    'yes_GG': 'in_GG',
    'no_GG': 'outside_GG',
    'yes_HRA': 'in_HRA',
    'no_HRA': 'outside_HRA',
    'yes_tra': 'in_TRA',
    'no_tra': 'outside_TRA'}

# specify which zoningmods strategy attributes to include for visualization
zmods_cols = ['dua_up', 'far_up', 'dua_down', 'far_down']

# specify which development_projects_list columns to include for visualization
dev_proj_list_cols = ['development_projects_id', 'site_name', 'action',
                      'x', 'y', 'year_built', 'building_type', 'source',
                      'building_sqft', 'non_residential_sqft', 'residential_units',
                      'tenure', 'deed_restricted_units']

# specify which TAZ summary output attributes to include for visualization
# growth summaries 
taz_growth_attr_cols = ['TOTHH', 'TOTEMP', 'EMPRES', 'RES_UNITS']
# household / employment groups
taz_hh_attr_cols = ['HHINCQ1', 'HHINCQ2', 'HHINCQ3', 'HHINCQ4']
taz_emp_attr_cols = ['AGREMPN', 'FPSEMPN', 'HEREMPN', 'RETEMPN', 'MWTEMPN', 'OTHEMPN']

# specify which jurisdiction summary output attributes to include for visualization
# JURIS attributes for growth summaries
juris_growth_attr_cols = ['tothh', 'totemp', 'res_units']
# JURIS attributes for household / employment groups
juris_hh_attr_cols = ['hhincq1', 'hhincq2', 'hhincq3', 'hhincq4']
juris_emp_attr_cols = ['agrempn', 'mwtempn', 'retempn', 'fpsempn', 'herempn', 'othempn']
# deed-restricted housing attributes
juris_dr_units_atrr_cols = ['deed_restricted_units', 'preserved_units', 'inclusionary_units', 'subsidized_units']

# specify which columns in parcel_output to include for visualization
parcel_output_cols = ['development_id', 'SDEM', 'building_sqft', 'building_type', 'job_spaces', 'non_residential_sqft',
                      'residential', 'residential_sqft', 'residential_units', 'total_residential_units', 'total_sqft',
                      'source', 'x', 'y', 'year_built']


############ functions

def extract_modeling_settings(setting_yaml_file: str,
                              viz_file_dir: str,
                              get_cost_shifter: bool=True):
    """A catch-all function to get all needed model settings from settings.yaml.

    Args:
        setting_yaml_file (str): setting yaml file path
        viz_file_dir (str): viz-ready file path
        get_cost_shifter (bool, optional): if to extract cost_shifter setting

    Returns:
        cost_shifter_df: cost_shifter setting
    """    
    
    logger.info('Process model settings')
    with open(setting_yaml_file, 'r') as stream:
        try:
            setting_yaml = yaml.safe_load(stream)
            # print(setting_yaml)
        except yaml.YAMLError as exc:
            logger.warning(exc)

    # cost shifter
    if get_cost_shifter:
        cost_shifter_df = pd.DataFrame.from_dict(setting_yaml['cost_shifters'], orient='index')
        cost_shifter_df.columns = ['cost_shifter']

        # write out
        cost_shifter_df.to_csv(viz_file_dir)

    return cost_shifter_df


def process_dev_proj_list(dev_project_file: str,
                          pipeline_viz_file_dir: str, 
                          strategy_porj_viz_file_dir: str,
                          bldg_source_recode: dict,
                          pipeline_bldg_src: list,
                          strategy_proj_bldg_src: list,
                          keepcols: list):
    """Converts development_projects_list into a pipeline df and a strategy-project df.
    NOTE: currently pipeline projects and strategy-based asserted projects are in the same file;
    they will be in separately tables per the BASIS process

    Args:
        dev_project_file (str): development_projects_list file path
        pipeline_viz_file_dir (str): pipeline data viz-ready file path
        strategy_porj_viz_file_dir (str): strategy-project data viz-ready file path
        bldg_source_recode (dict): dictionary to code project sources into pipeline/strategy categories
        pipeline_bldg_src (list): pipeline projects sources
        strategy_proj_bldg_src (list): strategy projects sources
        keepcols (list): columns to include in visualization

    Returns:
        pipeline_df: pipeline projects
        strategy_projects_df: strategy-based projects
    """                          

    logger.info('start processing development project list')

    dev_project_df = pd.read_csv(dev_project_file)
    logger.info(
        'loaded {} rows of development projects, containing the following source: \n{}'.format(
            dev_project_df.shape[0],
            dev_project_df['source'].unique()))

    # split the data by two categories of "source"
    pipeline_df = dev_project_df.loc[dev_project_df.source.isin(pipeline_bldg_src)][keepcols]
    pipeline_df['source_cat'] = pipeline_df['source'].map(bldg_source_recode)

    strategy_projects_df = dev_project_df.loc[dev_project_df.source.isin(strategy_proj_bldg_src)][keepcols]
    strategy_projects_df['source_cat'] = strategy_projects_df['source'].map(bldg_source_recode)
    
    # write out
    pipeline_df.to_csv(pipeline_viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of pipeline data to {}'.format(
            pipeline_df.shape[0], pipeline_viz_file_dir))

    strategy_projects_df.to_csv(strategy_porj_viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of strategy-based projects data to {}'.format(
            strategy_projects_df.shape[0], strategy_porj_viz_file_dir))

    return pipeline_df, strategy_projects_df


def consolidate_regional_control(hh_control_file: str,
                                 emp_control_file: str,
                                 viz_file_dir: str):
    """Consolidate and stack regional household controls and employment controls into one table

    Args:
        hh_control_file (str): hh control file path
        emp_control_file (str): hh control file path
        viz_file_dir (str): viz-ready file path

    Returns:
        control_df: stacked hh and emp controls
    """                                 

    # load the files
    hh_control_df_raw = pd.read_csv(hh_control_file)
    emp_control_df_raw = pd.read_csv(emp_control_file)

    # convert wide table to long table
    hh_control_df_raw.set_index('year', inplace=True)
    hh_control_df = hh_control_df_raw.stack().reset_index()
    hh_control_df.columns = ['year', 'breakdown', 'hh_control']

    emp_control_df_raw.set_index('year', inplace=True)
    emp_control_df = emp_control_df_raw.stack().reset_index()
    emp_control_df.columns = ['year', 'breakdown', 'emp_control']

    # merge hh and emp controls into one table
    control_df = pd.concat([hh_control_df, emp_control_df])
    control_df.fillna(0, inplace=True)

    # write out
    control_df.to_csv(viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of regional controls data to {}'.format(
            control_df.shape[0], viz_file_dir))

    return control_df


def process_zmods(zmods_file: str,
                  keep_cols: list,
                  zoningmodcat_df: DataFrame,
                  viz_file_dir: str):
    """Simplify zoning strategy data.

    Args:
        zmods_file (str): zmods strategy file path
        keep_cols (list): columns to include in visualization
        zoningmodcat_df (DataFrame): zoningmodcat definition
        viz_file_dir (str): viz-ready file path

    Returns:
        zmod_df: zoning strategy data, only including geographies with selected zoning strategy attributes;
                 geographies without strategy intervention are dropped to reduce file size.
    """

    logger.info('process strategy zoming_mods')

    zmod_df = pd.read_csv(zmods_file)
    
    # make column names Plan-/scenario-agnostic
    if 'fbpzoningmodcat' in list(zmod_df):
        zmod_df.rename(columns = {'fbpzoningmodcat': 'zoningmodcat'}, inplace=True)
        zmod_df['geo_type'] = 'zoningmodcat_fbp'
    elif 'eirzoningmodcat' in list(zmod_df):
        zmod_df.rename(columns = {'eirzoningmodcat': 'zoningmodcat'}, inplace=True)
        zmod_df['geo_type'] = 'zoningmodcat_eir'
    elif 'zoningmodcat' in list(zmod_df):
        zmod_df['geo_type'] = 'zoningmodcat_generic'

    # keep needed fields
    zmod_df = zmod_df[keep_cols + ['zoningmodcat', 'geo_type']]

    zmod_df = pd.merge(
        zoningmodcat_df[['shapefile_juris_name', 'county_name', 'zoningmodcat', 'geo_type']],
        zmod_df,
        on=['zoningmodcat', 'geo_type'],
        how='right')

    # drop rows with no strategy to reduce file size
    logger.debug('zmod_df has {:,} rows'.format(zmod_df.shape[0]))
    zmod_df.dropna(subset=keep_cols, how='all', inplace=True)
    logger.debug('keep {:,} rows with strategy'.format(zmod_df.shape[0]))

    # write out
    zmod_df.to_csv(viz_file_dir, index=False)
    logger.info('wrote out {} rows of zmods data to {}'.format(zmod_df.shape[0], viz_file_dir))

    return zmod_df


# a helper function to reformat the inclusionary zoning data in policy.yaml, called by 'inclusionary_yaml_to_df()'
def explode_df_list_col_to_rows(df: DataFrame,
                                list_col: str):
    """Explode dataframe where one column contains list values.

    Args:
        df (DataFrame): dataframe to be exploded, e.g. pd.DataFrame({'non_list_col': [1, 2], 'list_col': [['a', 'b'], 'c']})
        list_col (str): name of the column whose values are lists

    Returns:
        exploded_df: exploded dataframe, e.g. pd.DataFrame({'non_list_col': [1, 1, 2], 'list_col': ['a', 'b', 'c']})
    """    

    # get columns whose values will be repeated for each item in the list of the list column
    repeating_cols = list(df)
    # print(list_col)
    repeating_cols.remove(list_col)
    # print(repeating_cols)

    tmp_dict = {}
    # iterate through rows and each item of each list; add the values to a temp dictionary
    for i in range(df.shape[0]):
        # print(i)
        # print(df.iloc[i, :][list_col])
        for list_item in df.iloc[i, :][list_col]:
            # print(list_item)
            # print(df.iloc[i, :][repeating_cols])
            tmp_dict[list_item] = df.iloc[i, :][repeating_cols]
    
    exploded_df = pd.DataFrame.from_dict(tmp_dict, orient='index').reset_index()

    return exploded_df


def inclusionary_yaml_to_df(yaml_f: dict,
                            default_viz_file_dir: str,
                            strategy_viz_file_dir: str,
                            zoningmodcat_df: DataFrame,
                            juris_name_crosswalk: DataFrame):
                            
    """Extract inclusionary zoning policy in yaml based on scenario and convert to dataframe.

    Args:
        yaml_f (DataFrame): loaded policy yaml file in dictionary format
        default_viz_file_dir (str): default inclusionary zoning policy viz-ready file path
        strategy_viz_file_dir (str): inclusionary zoning strategy viz-ready file path
    
    Returns:
        inclusionary_default_df: default inclusionary zoning policy with columns:
            - 'geo_type': type of inclusionary-zoning policy geography, e.g. jurisdiction, fbpchcat
            - 'geo_category': inclusionary-zoning policy geography, e.g. San Francisco
            - 'IZ_level': high/median/low
            - 'IZ_amt': the inclusionary zoning value in percentage
        inclusionary_strategy_df: inclusionary zoning strategy with same columns
    """

    logger.info('process inclusionary zoning existing policy and strategy from {}'.format(POLICY_INPUT_FILE))
    # policy_input = os.path.join(r'C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\PBA50Plus_Development\Clean Code PR #3\inputs', 'plan_strategies', 'policy.yaml')

    # get the inclusionary setting of the target scenario
    inclusionary_raw = yaml_f['inclusionary_housing_settings']
    logger.info('inclusionary data contains the following scenarios: {}'.format(inclusionary_raw.keys()))
#     print(inclusionary_raw)
    
    # convert default and strategy into dataframes
    if 'default' in inclusionary_raw:
        logger.info('process default IZ data')
        inclusionary_default_raw_df = pd.DataFrame(inclusionary_raw['default'])
        inclusionary_default_df = explode_df_list_col_to_rows(inclusionary_default_raw_df, 'values')
        inclusionary_default_df.rename(columns = {'type': 'geo_type',
                                                  'index': 'BAUS_geo_category',
                                                  'description': 'IZ_level',
                                                  'amount': 'IZ_amt'}, inplace=True)
        # convert the naming convention to the standard
        inclusionary_default_df = pd.merge(
            inclusionary_default_df,
            juris_name_crosswalk.rename(columns = {'shapefile_juris_name': 'geo_category'}),
            left_on='BAUS_geo_category',
            right_on='JURIS',
            how='left')
        inclusionary_default_df = inclusionary_default_df[['geo_category', 'geo_type', 'IZ_amt']]
        
        # write out
        inclusionary_default_df.to_csv(default_viz_file_dir, index=False)

    else:
        logger.warning('no default inclusionary data')
        inclusionary_default_df = None

    if 'inclusionary_strategy' in inclusionary_raw:
        logger.info('process strategy inclusionary data')

        """
        inclusionary_strategy_raw_df = pd.DataFrame(inclusionary_raw['inclusionary_strategy'])
        inclusionary_strategy_df = explode_df_list_col_to_rows(inclusionary_strategy_raw_df, 'values')
        inclusionary_strategy_df.rename(columns = {'type': 'geo_type',
                                                   'index': 'geo_category',
                                                   'description': 'IZ_level',
                                                   'amount': 'IZ_amt'}, inplace=True)
        """
        # NOTE: the above code applies to the current inclusionary strategy coding syntax, which will be
        # changed to be consistent with other strategies, using a query instead of a list of growth geography
        # concatenations. Before that change, use the hard-coded query as follows:
        
        inclusionary_strategy_df = zoningmodcat_df.copy()
        
        high_level_value = 0.2
        high_level_filter = "(gg_id > '') and (tra_id in ('tra1', 'tra2a', 'tra2b', 'tra2c', 'tra3')) and (sesit_id in ('hra', 'hradis'))"
        medium_level_value = 0.15
        medium_level_filter = "((gg_id > '') and (tra_id in ('tra1', 'tra2a', 'tra2b', 'tra2c')) and (sesit_id in ('dis', ''))) or ((gg_id > '') and (tra_id=='') and (sesit_id in ('hra', 'hradis')))"
        low_level_value = 0.1
        low_level_filter = "((gg_id > '') and (tra_id in ('tra3', '')) and (sesit_id in ('dis', ''))) or (gg_id == '')"

        inclusionary_strategy_df.loc[inclusionary_strategy_df.eval(high_level_filter), 'IZ_amt'] = high_level_value
        inclusionary_strategy_df.loc[inclusionary_strategy_df.eval(medium_level_filter), 'IZ_amt'] = medium_level_value
        inclusionary_strategy_df.loc[inclusionary_strategy_df.eval(low_level_filter), 'IZ_amt'] = low_level_value

        # keep only needed fields
        inclusionary_strategy_df = inclusionary_strategy_df[['shapefile_juris_name', 'county_name', 'zoningmodcat', 'geo_type', 'IZ_amt']]
        # drop rows with no strategy to reduce file size
        inclusionary_strategy_df = inclusionary_strategy_df.loc[inclusionary_strategy_df['IZ_amt'].notnull()]

        # write out
        inclusionary_strategy_df.to_csv(strategy_viz_file_dir, index=False)
    else:
        logger.warning('no strategy inclusionary data')
        inclusionary_strategy_df = None

    return (inclusionary_default_df, inclusionary_strategy_df)


def housing_preserve_yaml_to_df(yaml_f: dict,
                                zoningmodcat_df: DataFrame,
                                preserve_target_wirte_out_file_dir: str,
                                preserve_qualify_wirte_out_file_dir: str):
    """Extract housing preservation strategy from policy.yaml into 2 dataframes.

    Args:
        yaml_f (dict): loaded policy yaml file
        zoningmodcat_df (DataFrame): zoningmodcat definition
        preserve_target_wirte_out_file_dir (str): preservation strategy target part viz-ready file path
        preserve_qualify_wirte_out_file_dir (str): preservation strategy qualification part viz-ready file path

    Returns:
        strategy_preserveTarget_df: preservation target, number of units to be preserved by geography 
        strategy_preserveQualify_df: preservation qualification, whether a housing unit is a candidate for preservation based on
            its growth geography designations
    """

    # get preservation target amount from policy.yaml
    strategy_preserveTarget_df = pd.DataFrame(columns = ['geo_category', 'geo_type', 'batch1', 'batch2', 'batch3', 'batch4'])
    # also, tag geographies based on whether housing in a geography would be candicates for housing preservation
    strategy_preserveQualify_df = zoningmodcat_df.copy()

    # baseline scenario with no strategy
    strategy_preserveQualify_df['preserve_candidate'] = ''

    # if strategy scenario, policy.yaml should contain the strategy
    try:
        # extract preservation strategy geography type
        strategy_geo_type = yaml_f['housing_preservation']['geography']

        for geo_category in yaml_f['housing_preservation']['settings']:
            # get preservation unit target and qualify criteria
            config = yaml_f['housing_preservation']['settings'][geo_category]
            batch1_target = config['first_unit_target']
            batch1_idx = config['first_unit_filter']
            batch2_target = config['second_unit_target']
            batch2_idx = config['second_unit_filter']
            batch3_target = config['third_unit_target']
            batch3_idx = config['third_unit_filter']
            batch4_target = config['fourth_unit_target']
            batch4_idx = config['fourth_unit_filter']
            
            # add the target data to target df
            strategy_preserveTarget_df.loc[len(strategy_preserveTarget_df.index)] = \
                [geo_category, strategy_geo_type, batch1_target, batch2_target, batch3_target, batch4_target]
            
            # add the qualify criteria data to qualify df
            if strategy_geo_type == 'county_name':
                strategy_preserveQualify_df.loc[
                    (strategy_preserveQualify_df['county_name'] == geo_category) & \
                    (strategy_preserveQualify_df['preserve_candidate'] == '') & \
                    strategy_preserveQualify_df.eval(batch1_idx), 'preserve_candidate'] = 'batch1'
                strategy_preserveQualify_df.loc[
                    (strategy_preserveQualify_df['county_name'] == geo_category) & \
                    (strategy_preserveQualify_df['preserve_candidate'] == '') & \
                    strategy_preserveQualify_df.eval(batch2_idx), 'preserve_candidate'] = 'batch2'
                strategy_preserveQualify_df.loc[
                    (strategy_preserveQualify_df['county_name'] == geo_category) & \
                    (strategy_preserveQualify_df['preserve_candidate'] == '') & \
                    strategy_preserveQualify_df.eval(batch3_idx), 'preserve_candidate'] = 'batch3'
                strategy_preserveQualify_df.loc[
                    (strategy_preserveQualify_df['county_name'] == geo_category) & \
                    (strategy_preserveQualify_df['preserve_candidate'] == '') & \
                    strategy_preserveQualify_df.eval(batch4_idx), 'preserve_candidate'] = 'batch4'
            
            elif strategy_geo_type == 'jurisdiction':
                # NOTE: if the strategy is coded by jurisdiction, fill out the following code block
                pass

    except:
        logger.info('no housing_preservation strategy in policy.yaml for this model run')
    
    # fillna with 0 or '' (when no target for a county in a batch)
    strategy_preserveTarget_df.fillna(0, inplace=True)
    strategy_preserveQualify_df['preserve_candidate'].fillna('', inplace=True)

    logger.debug(
        'housing preserve_candidate value counts: \n{}'.format(
            strategy_preserveQualify_df['preserve_candidate'].value_counts(dropna=False)))
    
    # keep only need fields
    strategy_preserveQualify_df = \
        strategy_preserveQualify_df[['shapefile_juris_name', 'county_name', 'zoningmodcat', 'geo_type', 'preserve_candidate']]

    # drop rows with no strategy to reduce file size
    strategy_preserveQualify_df = \
        strategy_preserveQualify_df.loc[strategy_preserveQualify_df['preserve_candidate'] != '']
    logger.debug('keep {:,} rows with strategy'.format(strategy_preserveQualify_df.shape[0]))

    # write out
    strategy_preserveTarget_df.to_csv(preserve_target_wirte_out_file_dir, index=False)
    strategy_preserveQualify_df.to_csv(preserve_qualify_wirte_out_file_dir, index=False)

    return strategy_preserveTarget_df, strategy_preserveQualify_df


def housing_bond_subsidy_yaml_to_df(yaml_f: dict,
                                    zoningmodcat_df: DataFrame,
                                    housing_bond_viz_file_dir: str):
    """Extract housing bond subsidy strategy.

    Args:
        yaml_f (dict): loaded policy yaml file
        zoningmodcat_df (DataFrame): zoningmodcat definition
        housing_bond_viz_file_dir (str): viz-ready file path

    Returns:
        strategy_housing_bonds_df: housing bond subsidy qualification, whether a residential development qualifies for receiving
            subsidy based on its growth geography designations
    """

    # add column 'housing_bonds' to tag geography categories based on eligibility to receive bond subsidies
    strategy_housing_bonds_df = zoningmodcat_df.copy()

    # baseline scenario with no strategy
    strategy_housing_bonds_df['housing_bonds'] = 'Not qualify'

    residential_lump_sum_accts_setting = yaml_f['acct_settings']['lump_sum_accounts']

    for acct_setting in residential_lump_sum_accts_setting:
        
        if '_housing_bond_strategy' in residential_lump_sum_accts_setting[acct_setting]['name']:
            # get the strategy info
            qualify = residential_lump_sum_accts_setting[acct_setting]['receiving_buildings_filter']
            
            # if there is the strategy
            if not qualify is None:
                qualify_county = qualify.split(' & ')[0].split('== ')[1].strip("\'")
                qualify_geos_idx = qualify.split(' & ')[1]

                # add the info to strategy_housing_bonds_df dataframe
                strategy_housing_bonds_df.loc[
                    (strategy_housing_bonds_df['county_name'] == qualify_county) & \
                    strategy_housing_bonds_df.eval(qualify_geos_idx), 'housing_bonds'] = 'Qualify'
            else:
                logger.warning('no housing bond subsidy strategy data')

    logger.debug('housing_bond value counts: \n{}'.format(strategy_housing_bonds_df['housing_bonds'].value_counts(dropna=False)))

    # keep only need fields
    strategy_housing_bonds_df = \
        strategy_housing_bonds_df[['shapefile_juris_name', 'county_name', 'zoningmodcat', 'geo_type', 'housing_bonds']]

    # drop rows with no strategy to reduce file size
    strategy_housing_bonds_df = \
        strategy_housing_bonds_df.loc[strategy_housing_bonds_df['housing_bonds'] == 'Qualify']
    logger.debug('keep {:,} rows with strategy'.format(strategy_housing_bonds_df.shape[0]))

    # write out
    strategy_housing_bonds_df.to_csv(housing_bond_viz_file_dir, index=False)

    return strategy_housing_bonds_df


def dev_cost_reduction_yaml_to_df(yaml_f: dict, 
                                  zoningmodcat_df: DataFrame,
                                  viz_file_dir: str):
    """Extract housing development cost reduction strategy.

    Args:
        yaml_f (dict): loaded policy yaml file
        zoningmodcat_df (DataFrame): zoningmodcat definition
        viz_file_dir (str): viz-ready file path

    Returns:
        strategy_devcost_df: housing development cost reduction qualification and amount based on its growth geography designations
    """
    
    strategy_devcost_df = zoningmodcat_df.copy()

    # baseline scenario with no strategy
    strategy_devcost_df['housing_devCost_reduction_cat'] = ''
    strategy_devcost_df['housing_devCost_reduction_amt'] = ''

    profit_adjust_setting = yaml_f['acct_settings']['profitability_adjustment_policies']

    for strategy_name in profit_adjust_setting:
        if 'reduce_housing_costs_tier' in profit_adjust_setting[strategy_name]['name']:
            # get strategy info: cost reduction: tier, criteria, amount
            adjust_tier = profit_adjust_setting[strategy_name]['name'].split('_')[-2]
            formula = profit_adjust_setting[strategy_name]['profitability_adjustment_formula']
            if not formula is None:
                adjust_idx = formula.split('*')[0]
                adjust_amt = formula.split('*')[1]
                # add the info to dataframe strategy_devcost_df
                strategy_devcost_df.loc[strategy_devcost_df.eval(adjust_idx), 'housing_devCost_reduction_cat'] = adjust_tier
                strategy_devcost_df.loc[strategy_devcost_df.eval(adjust_idx), 'housing_devCost_reduction_amt'] = adjust_amt
            else:
                logger.warning('no housing bond subsidy strategy data')

    logger.debug(
        'devCost reduction amt value counts: \n{}'.format(
            strategy_devcost_df['housing_devCost_reduction_amt'].value_counts(dropna=False)))
    
    # keep only need fields
    strategy_devcost_df = \
        strategy_devcost_df[['shapefile_juris_name', 'county_name', 'zoningmodcat', 'geo_type',
                             'housing_devCost_reduction_cat', 'housing_devCost_reduction_amt']]

    # drop rows with no strategy to reduce file size
    strategy_devcost_df = \
        strategy_devcost_df.loc[strategy_devcost_df['housing_devCost_reduction_cat'] != '']
    logger.debug('keep {:,} rows with strategy'.format(strategy_devcost_df.shape[0]))

    # write out
    strategy_devcost_df.to_csv(viz_file_dir, index=False)

    return strategy_devcost_df


def consolidate_TAZ_summaries(model_run_output_dir: str,
                              model_run_id: str,
                              viz_file_dir: str,
                              growth_attr_cols: list, 
                              hh_attr_cols: list, 
                              emp_attr_cols: list):
    """Consolidate TAZ summaries (output) of base year and all forecast years, with only key metrics that need visualization.

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
        growth_attr_cols (list): columns in BAUS output taz_summaries for key growth metrics to be visualized
        hh_attr_cols (list): columns in BAUS output taz_summaries for household by income group
        emp_attr_cols (list): columns in BAUS output juris_summaries for employment by sector

    Returns:
        all_taz_summaries_df: TAZ-level key household and employment metrics for all years
    """    

    # collect all TAZ summaries outputs
    all_taz_files = glob.glob(model_run_output_dir + "\{}_taz_summaries*.csv".format(model_run_id))
    # NOTE: remove 2010 data since 2015 is the base year for PBA50
    all_taz_files = [file_dir for file_dir in all_taz_files if '2010' not in file_dir]
    logger.info(
        'processing {} taz_summaries files for key summaries: {}'.format(len(all_taz_files), all_taz_files))

    # loop through the list to process the files
    all_taz_summaries_df = pd.DataFrame()

    for file_dir in all_taz_files:
        year = file_dir.split("\\")[-1].split(".")[0].split("summaries_")[1][:4]
        logger.info('year: {}'.format(year))

        # load the data
        taz_df = pd.read_csv(
            file_dir,
            usecols = ['TAZ'] + growth_attr_cols + hh_attr_cols)
            # NOTE: if also need employment by-sector forecast, usecols = ['TAZ'] + growth_attr_cols + hh_attr_cols + emp_attr_cols
            
        # fill na with 0
        taz_df.fillna(0, inplace=True)
        # add year
        taz_df['year'] = year
        # append to the master table
        all_taz_summaries_df = pd.concat([all_taz_summaries_df, taz_df])
    
    # double check all years are here
    logger.debug('aggregated TAZ output has years: {}'.format(all_taz_summaries_df['year'].unique()))
    
    # TODO: modify field types, esp. integer fields
    logger.debug('aggregated TAZ output fields: \n{}'.format(all_taz_summaries_df.dtypes))
    logger.debug('header: \n{}'.format(all_taz_summaries_df.head()))

    # write out
    all_taz_summaries_df.to_csv(viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of TAZ summary data to {}'.format(
            all_taz_summaries_df.shape[0], viz_file_dir))

    return all_taz_summaries_df


def consolidate_juris_summaries(model_run_output_dir: str, 
                                model_run_id: str,
                                viz_file_dir: str,
                                growth_attr_cols: list, 
                                hh_attr_cols: list, 
                                emp_attr_cols: list,
                                juris_name_crosswalk: DataFrame):
    """Consolidate jurisdiction summaries (output) of base year and all forecast years,
            with only key metrics that need visualization.

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
        growth_attr_cols (list): columns in BAUS output juris_summaries for key growth metrics to be visualized
        hh_attr_cols (list): columns in BAUS output juris_summaries for household by income group
        emp_attr_cols (list): columns in BAUS output juris_summaries for employment by sector
        juris_name_crosswalk (DataFrame): a crosswalk betwen BAUS jurisdiction naming convention and spatial data's naming convention

    Returns:
        all_juris_summaries_df: jurisdiction-level key household and employment metrics for all years
    """    

    # collect all juris summaries outputs
    all_juris_files = glob.glob(model_run_output_dir + "\{}_juris_summaries*.csv".format(model_run_id))
    # NOTE: remove 2010 data since 2015 is the base year for PBA50
    all_juris_files = [file_dir for file_dir in all_juris_files if '2010' not in file_dir]
    logger.info(
        'processing {} juris_summaries files for key summaries: {}'.format(len(all_juris_files), all_juris_files))
    
    # loop through the list of outputs
    all_juris_summaries_df = pd.DataFrame()

    for file_dir in all_juris_files:
        year = file_dir.split("\\")[-1].split(".")[0].split("summaries_")[1][:4]
        logger.info('year: {}'.format(year))
        
        # load the summary data
        juris_summary_df = pd.read_csv(
            file_dir,
            usecols = ['juris'] + growth_attr_cols + hh_attr_cols)
            # NOTE: if also need employment by-sector forecast, usecols = ['juris'] + growth_attr_cols + hh_attr_cols + emp_attr_cols
                
        # fill na with 0
        juris_summary_df.fillna(0, inplace=True)
        # add year
        juris_summary_df['year'] = year    
        # append to the master table
        all_juris_summaries_df = pd.concat([all_juris_summaries_df, juris_summary_df])

    # double check all years are here
    logger.debug('aggregated Juris output has years: {}'.format(all_juris_summaries_df['year'].unique()))

    # make field names consistent with TAZ table
    all_juris_summaries_df_cols = [x.upper() for x in list(all_juris_summaries_df)]
    all_juris_summaries_df.columns = all_juris_summaries_df_cols

    all_juris_summaries_df = all_juris_summaries_df.merge(juris_name_crosswalk, on='JURIS', how='left')
    logger.debug(all_juris_summaries_df.head())

    # TODO: modify field types, esp. integer fields
    logger.debug('aggregated Juris output fields: \n{}'.format(all_juris_summaries_df.dtypes))
    logger.debug('header: \n{}'.format(all_juris_summaries_df.head()))

    # write out
    all_juris_summaries_df.to_csv(viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of Juris summary data to {}'.format(
            all_juris_summaries_df.shape[0], viz_file_dir))

    return all_juris_summaries_df


def summarize_deed_restricted_units(model_run_output_dir: str,
                                    model_run_id: str,
                                    viz_file_dir: str,
                                    dr_units_atrr_cols: list,
                                    juris_name_crosswalk: DataFrame):
    """Calculates the growth of deed-restricted housing units in each forecast year over the base year by source, including strategies.

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
        dr_units_atrr_cols (list): columns in BAUS output juris_summaries for deed-restricted unit info
        juris_name_crosswalk (DataFrame): a crosswalk betwen BAUS jurisdiction naming convention and spatial data's naming convention

    Returns:
        all_juris_dr_growth_df: dataframe of deed-restricted units growth (each forecast year against base year 2015) by unit source
    """    

    logger.info('processing juris_summaries files to calculate deed-restricted housing growth')

    # get base year 2015 data
    logger.info('year: 2015')
    juris_dr_2015_file = os.path.join(model_run_output_dir, "{}_juris_summaries_2015.csv".format(model_run_id))
    juris_dr_2015_df = pd.read_csv(
        juris_dr_2015_file,
        usecols=['juris'] + dr_units_atrr_cols).set_index('juris')

    # calculate units from project list
    juris_dr_2015_df['projs_list_units'] = \
            juris_dr_2015_df['deed_restricted_units'] - \
                juris_dr_2015_df[['preserved_units', 'inclusionary_units', 'subsidized_units']].sum(axis=1)
    # add year to colnames
    juris_dr_2015_df.columns = [x+'_2015' for x in list(juris_dr_2015_df)]

    # loop through juris output of other years and calculate growth from 2015
    all_juris_dr_growth_df = pd.DataFrame()
    
    # collect all forecast years juris summaries outputs
    forecast_juris_files = glob.glob(model_run_output_dir + "\{}_juris_summaries*.csv".format(model_run_id))
    # NOTE: remove 2010 data since 2015 is the base year for PBA50
    forecast_juris_files = [file_dir for file_dir in forecast_juris_files if ('2010' not in file_dir) and ('2015' not in file_dir)]
    logger.info(
        'processing {} juris_summaries files for key summaries: {}'.format(len(forecast_juris_files), forecast_juris_files))

    for file_dir in forecast_juris_files:
        if '2015' not in file_dir:
            # logger.debug(file_dir)
            juris_dr_forecast_df = pd.read_csv(
                file_dir,
                usecols = ['juris'] + dr_units_atrr_cols).set_index('juris')
            juris_dr_forecast_df['projs_list_units'] = \
                juris_dr_forecast_df['deed_restricted_units'] - juris_dr_forecast_df[['preserved_units', 'inclusionary_units', 'subsidized_units']].sum(axis=1)

            year = file_dir.split("\\")[-1].split(".")[0].split("summaries_")[1][:4]
            logger.info('year: {}'.format(year))
            juris_dr_forecast_df.columns = [x+'_{}'.format(year) for x in list(juris_dr_forecast_df)]
            logger.debug('raw juris DR data: \n{}'.format(juris_dr_forecast_df.head()))
            
            # merge the forecast data with 2015 base year data, and calculate growth of each DR housing source 
            dr_units_growth = pd.concat([juris_dr_2015_df, juris_dr_forecast_df], axis=1)
            for colname in list(dr_units_growth):
                dr_units_growth[colname].fillna(0, inplace=True)
            logger.debug('after merging with base year data: \n{}'.format(dr_units_growth.head()))
            for attr in dr_units_atrr_cols + ['projs_list_units']:
                dr_units_growth[attr+'_growth'] = dr_units_growth[attr+'_{}'.format(year)] - dr_units_growth[attr+'_2015']
            logger.debug('after calculating growth: \n{}'.format(dr_units_growth.head()))
            
            # add year tag to the table
            dr_units_growth['year'] = year

            # keep only needed fields
            dr_units_growth_export = dr_units_growth[[x for x in list(dr_units_growth) if 'growth' in x] + ['year']]
            logger.debug('after dropping raw fields: \n{}'.format(dr_units_growth_export.head()))
            
            # append to the master data
            all_juris_dr_growth_df = pd.concat([all_juris_dr_growth_df, dr_units_growth_export])
    
    # double check all years are here
    logger.debug(
        'aggregated DR units growth summary all_juris_dr_growth_df has years: {}'.format(all_juris_dr_growth_df['year'].unique()))

    # use crosswalk to get standard jurisdiction name
    all_juris_dr_growth_df.reset_index(inplace=True)
    all_juris_dr_growth_df.rename(columns = {'juris': 'JURIS'}, inplace=True)
    all_juris_dr_growth_df = all_juris_dr_growth_df.merge(juris_name_crosswalk, on='JURIS', how='left')

    # convert to integer
    all_juris_dr_growth_df = all_juris_dr_growth_df.astype({
        'deed_restricted_units_growth' :np.int32,
        'preserved_units_growth'   :np.int32,
        'inclusionary_units_growth'  :np.int32,
        'subsidized_units_growth'  :np.int32,
        'projs_list_units_growth'  :np.int32
        })
    
    logger.debug('final aggregated DR units growth table fields: \n{}'.format(all_juris_dr_growth_df.dtypes))

    # write out
    all_juris_dr_growth_df.to_csv(viz_file_dir, index=False)
    logger.info(
        'wrote out {} rows of Juris summary data to {}'.format(
            all_juris_dr_growth_df.shape[0], viz_file_dir))

    return all_juris_dr_growth_df


def summarize_growth_by_growth_geographies(model_run_output_dir: str,
                                           model_run_id: str,
                                           viz_file_dir: str,
                                           geo_tag_types: dict,
                                           geos_tag_recode: dict):
    """Aggregate and stack baus output files representing growth by key growth geography designations.

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
        geo_tag_types (dict): recode abbreviated growth geography type into full name, e.g. 'TRA' -> 'Transit-rich'
        geos_tag_recode (dict): recode growth geography designation into full name, e.g. 'no_tra' -> 'outside_TRA'

    Returns:
        all_growth_GEOs_df: a long dataframe with growth by all growth geography designations stacked, ready for Tableau viz
    """

    # get all growth-geography-based growth summary output
    growth_geos_file_candidates = \
        ['{}_{}_growth_summaries.csv'.format(model_run_id, i) for i in list(geo_tag_types)]
    
    # create a master dataframe to aggregate all outputs
    all_growth_GEOs_df = pd.DataFrame()

    # loop through the output files, recode geos tag, and append to the master
    for file_name in growth_geos_file_candidates:
        if os.path.exists(os.path.join(model_run_output_dir, file_name)):

            geo_tag = file_name.split('_')[1]

            logger.info('load {} for growth summary by {}'.format(file_name, geo_tag))

            growth_GGs_df = pd.read_csv(
                os.path.join(model_run_output_dir, file_name),
                usecols = ['county', 'juris', 'geo_category', 'tothh growth', 'totemp growth'])
            
            # add tag for type of growth geography
            growth_GGs_df['geography_type'] = geo_tag_types[geo_tag.upper()]
            # recode geography categories into more straight-forward descriptions
            growth_GGs_df['geo_category'] = growth_GGs_df['geo_category'].map(geos_tag_recode)

            logger.debug('geo categories contained: {}'.format(growth_GGs_df['geo_category'].unique()))
            logger.debug(growth_GGs_df.head())

            # append to master table
            all_growth_GEOs_df = pd.concat([all_growth_GEOs_df, growth_GGs_df])

    # double check all geos are included
    logger.debug(
        'aggregated geo-growth summary contain the following growth geographies: {}'.format(
            all_growth_GEOs_df['geography_type'].unique()))

    logger.debug('aggregated geo-growth summary table fields: \n{}'.format(all_growth_GEOs_df.dtypes))
    logger.debug('header: \n{}'.format(all_growth_GEOs_df.head()))

    # clean up: remove space from column names
    all_growth_GEOs_df.columns = all_growth_GEOs_df.columns.str.replace(' ', '_')

    # write out
    logger.info(
        'writing out {} rows of geo-growth summary data to {}'.format(
            all_growth_GEOs_df.shape[0], viz_file_dir))
    all_growth_GEOs_df.to_csv(viz_file_dir, index=False)

    return all_growth_GEOs_df


def simplify_parcel_output(model_run_output_dir: str,
                           model_run_id: str,
                           viz_file_dir: str,
                           keep_cols: Optional[list] = parcel_output_cols):
    """Simplify baus output file parcel_output, keeping only needed columns

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
        keep_cols (Optional[list], optional): columns in the parcel_output table to keep for visualization.
            Defaults to parcel_output_cols.

    Returns:
        _type_: _description_
    """

    file_dir = os.path.join(model_run_output_dir, '{}_parcel_output.csv'.format(model_run_id))
    logger.info('processing parcel_output data: {}'.format(file_dir))
    
    try:
        parcel_output_df = pd.read_csv(
            file_dir,
            usecols = keep_cols)
        logger.info('parcel_output has {} rows'.format(parcel_output_df.shape[0]))

        # recode 'source' to be consistent with 
        parcel_output_df['source_cat'] = parcel_output_df['source'].map(building_source_recode)

        # TODO: modify field types - e.g. year_built integer
        logger.debug('field types: \n{}'.format(parcel_output_df.dtypes))
        logger.debug('header: \n{}'.format(parcel_output_df.head()))

        # write out
        logger.info(
            'writing out {} rows of geo-growth summary data to {}'.format(
                parcel_output_df.shape[0], viz_file_dir))
        parcel_output_df.to_csv(viz_file_dir, index=False)
        
        return parcel_output_df

    except:
        logger.warning('parcel_output not exist for {}'.format(model_run_id))

        return None


def consolidate_taz_logsum(model_run_interim_dir: str,
                           model_run_id: str,
                           viz_file_dir: str):
    """Consolidate TAZ logsum data used in rsh model.

    Args:
        model_run_output_dir (str): model output folder path
        model_run_id (str): model run id, e.g. 'run182'
        viz_file_dir (str): viz-ready file path
    """
    # collect all TAZ logsum files
    all_logsum_files = glob.glob(model_run_interim_dir + "\{}_taz_logsums_*.csv".format(model_run_id))

    # only need 2015 (for 2015-2025 simulation), and 2030 (for 2030-2050 similation)
    keep_logsum_files = [file for file in all_logsum_files if ('_2015' in file) or ('_2030' in file)]

    agg_taz_logsum_df = pd.DataFrame()

    for file in keep_logsum_files:
        # print(i)
        filename = os.path.basename(file)
        year_tag = filename.split('.csv')[0][-4:]
        # print(year_tag)
        
        taz_logsum_df = pd.read_csv(file)
        taz_logsum_df['year'] = year_tag
        
        agg_taz_logsum_df = pd.concat([agg_taz_logsum_df, taz_logsum_df])
        
    # write out
    agg_taz_logsum_df.to_csv(viz_file_dir, index=False)

    return agg_taz_logsum_df


def upload_df_to_redshift(df: DataFrame,
                          table_name: str,
                          s3_bucket: Optional[str] = 'fms-urbansim',
                          s3_key_prefix: Optional[str] = '2022/',
                          redshift_schema: Optional[str] = 'urbansim_fms'):
    """Uploads dataframe to Redshift AWS.

    Args:
        df (DataFrame): dataframe to be uploaded
        table_name (str): table name to show in Redshift, e.g. 'test_data'
        s3_key (Optional[str], optional): defaults to '2022/', used to construct s3_key with df name, e.g. '2022/test_data.csv'
        s3_bucket (Optional[str], optional): S3 bucket name. Defaults to 'fms-urbansim'.
        redshift_schema (Optional[str], optional): _description_. Defaults to 'urbansim_fms'.
    """                                     

    logger.info('uploading {}'.format(table_name))

    if (df is None) or (df.shape[0] == 0):
        logger.warning('no data to upload')

    else:
        # get dataframe ctypes
        ctypes = create_column_type_dict(df)
        logger.debug('ctypes: \n{}'.format(ctypes))

        # post data to S3
        s3_key = '{}{}.csv'.format(s3_key_prefix, table_name)
        logger.debug('s3 key: {}'.format(s3_key))
        post_df_to_s3(df, s3_bucket, s3_key)

        # post data from S3 to Redshift
        redshift_tablename = '{}.{}'.format(redshift_schema, table_name)  # schema.table format
        create_redshift_table_via_s3(tablename=redshift_tablename,
                                    s3_path='s3://{}/{}'.format(s3_bucket, s3_key),
                                    ctypes=ctypes)

        logger.info('finished uploading')


def upload_gdf_to_agol(gdf: DataFrame,
                       ):
    """Upload spatial data to ArcGIS Online

    Args:
        gdf (DataFrame): geodataframe
    """    
    # TODO: write out the function


if __name__ == '__main__':

    # process one run at a time
    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('run_dir', help='Provide the full directory of the model run')
    parser.add_argument('model_code_dir', help='Provide the full directory of the urbansim code')
    parser.add_argument('run_scenario', help='Provide the run scenario, e.g. s24')
    parser.add_argument('scenario_group', help='Provide the run scenario group name, e.g. Final Blueprint')
    parser.add_argument('run_description', help='Provide the run description, e.g. Model development test runs using s24 assumptions')
    parser.add_argument('run_id', help='Provide the run id, e.g. run182')
    parser.add_argument('--upload_to_redshift', help='If upload the data to Redshift', action='store_true')
    parser.add_argument('--upload_to_agol', help='If upload the data to ArcGIS Online', action='store_true')
    parser.add_argument('--last_run', required=False, choices=['yes', 'no'], help='indicating if it is the most recent run of a scenario. Default to yes')
    
    args = parser.parse_args()

    ############ I/O
    # INPUT - model run data
    MOEDL_RUN_DIR = args.run_dir
    RAW_INPUT_DIR = os.path.join(MOEDL_RUN_DIR, 'inputs')
    RAW_INTERIM_DIR = os.path.join(MOEDL_RUN_DIR, 'interim')
    RAW_OUTPUT_DIR = os.path.join(MOEDL_RUN_DIR, 'outputs')
    MODEL_CODE_DIR = args.model_code_dir
    # 'C:\\Users\\{}\\Box\\Modeling and Surveys\\Urban Modeling\\Bay Area UrbanSim\\PBA50Plus_Development\\Clean Code PR #3\\bayarea_urbansim'.format(os.getenv('USERNAME'))
    # RAW_OUTPUT_DIR = MOEDL_RUN_DIR
    RUN_ID = args.run_id

    RUN_SCENARIO = args.run_scenario
    SCENARIO_GROUP = args.scenario_group
    RUN_DESCRIPTION = args.run_description

    if args.last_run:
        LAST_RUN = args.last_run
    else:
        LAST_RUN = 'yes'

    # INPUT - growth geometries
    # FBP growth geographies
    FBPZONINGMODCAT_SYSTEM_FILE = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\data_viz_ready\\crosswalks\\fbpzoningmodcat_attrs.csv'
    # EIR growth geographies
    EIRZONINGMODCAT_SYSTEM_FILE = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\data_viz_ready\\crosswalks\\eirzoningmodcat_attrs.csv'

    # INPUT - model settings, strategies, inputs
    POLICY_INPUT_FILE = os.path.join(RAW_INPUT_DIR, 'plan_strategies', 'policy.yaml')
    SETTING_INPUT_FILE = os.path.join(MODEL_CODE_DIR, 'configs', 'settings.yaml')
    DEV_PROJ_FILE = os.path.join(RAW_INPUT_DIR, 'basis_inputs', 'parcels_buildings_agents', '2021_0309_1939_development_projects.csv')
    HH_CONTROL_FILE = os.path.join(RAW_INPUT_DIR, 'regional_controls', 'household_controls.csv')
    EMP_CONTROL_FILE = os.path.join(RAW_INPUT_DIR, 'regional_controls', 'employment_controls.csv')
    ZMODS_FILE = os.path.join(RAW_INPUT_DIR, 'plan_strategies', 'zoning_mods.csv')

    # INPUT - other helper data
    JURIS_NAME_CROSSWALK_FILE = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\data_viz_ready\\crosswalks\\juris_county_id.csv'
    MODEL_RUN_INVENTORY_FILE = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\model_run_inventory.csv'
    
    # INPUT - spatial data, only for ArcGIS online
    SHAPEFILE_DIR = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\data_viz_ready\\spatial'
    TAZ_SHAPE_FILE = os.path.join(SHAPEFILE_DIR, 'Travel Analysis Zones.shp')
    JURIS_SHAPE_FILE = os.path.join(SHAPEFILE_DIR, 'jurisdictions_tiger2019vintage.shp')
    FBPZONINGMODCAT_SHP_FILE = os.path.join(SHAPEFILE_DIR, 'zoningmodcat_fbp.shp')
    EIRZONINGMODCAT_SHP_FILE = os.path.join(SHAPEFILE_DIR, 'zoningmodcat_eir.shp')
    FBPCHCAT_SHP_FILE = os.path.join(SHAPEFILE_DIR, 'fbpchcat_gdf.shp')

    # OUTPUT - folder for visualization data
    VIZ_DIR = 'M:\\Data\\Urban\\BAUS\\visualization_design'
    VIA_DATA_DIR = os.path.join(VIZ_DIR, 'data')
    # VIZ_READY_DATA_DIR = os.path.join(VIA_DATA_DIR, 'data_viz_ready', 'csv_v2')
    VIZ_READY_DATA_DIR = os.path.join(VIA_DATA_DIR, 'data_viz_ready', 'csv_test_script')
    # TODO: write viz-reday data into existing run folder
    LOG_FILE = os.path.join(VIZ_READY_DATA_DIR, "prepare_data_for_viz_{}_{}.log".format(RUN_ID, today))

    # OUTPUT - baus model inputs
    VIZ_MODEL_SETTING_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_model_settings.csv'.format(RUN_ID))
    VIZ_REGIONAL_CONTROLS_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_regional_controls.csv'.format(RUN_ID))
    VIZ_PIPELINE_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_pipeline.csv'.format(RUN_ID))
    VIZ_STRATEGY_PROJECT_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_projects.csv'.format(RUN_ID))
    VIZ_STRATEGY_UPZONING_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_upzoning.csv'.format(RUN_ID))
    VIZ_BASELINE_INCLUSIONARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_baseline_inclusionary.csv'.format(RUN_ID))
    VIZ_STRATEGY_INCLUSIONARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_inclusionary.csv'.format(RUN_ID))
    VIZ_STRATEGY_HOUSING_BOND_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_housingBond.csv'.format(RUN_ID))
    VIZ_STRATEGY_DEV_COST_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_devCost.csv'.format(RUN_ID))
    VIZ_STRATEGY_PRESERVE_TARGET_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_unitPreserve_target.csv'.format(RUN_ID))
    VIZ_STRATEGY_PRESERVE_QUALIFY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_strategy_unitPreserve.csv'.format(RUN_ID))

    # OUTPUT - baus model interim data
    VIZ_TAZ_LOGSUM_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_taz_logsum.csv'.format(RUN_ID))

    # OUTPUT - baus model outputs
    VIZ_TAZ_SUMMARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_taz_summary.csv'.format(RUN_ID))
    VIZ_JURIS_SUMMARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_juris_summary.csv'.format(RUN_ID))
    VIZ_GROWTH_GEOS_SUMMARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_growth_geos_summary.csv'.format(RUN_ID))
    VIZ_PARCEL_OUTPUT_SUMMARY_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_parcel_output.csv'.format(RUN_ID))
    VIZ_DR_UNITS_GROWTH_FILE = os.path.join(VIZ_READY_DATA_DIR, '{}_dr_units_growth.csv'.format(RUN_ID))
    UPDATED_MODEL_RUN_INVENTORY_FILE = 'M:\\Data\\Urban\\BAUS\\visualization_design\\data\\model_run_inventory.csv'

    # upload
    UPLOAD_TO_REDSHIFT = args.upload_to_redshift
    UPLOAD_TO_AGOL = args.upload_to_agol

    ############ set up logging
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel('INFO')
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)
    # file handler
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel('DEBUG')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)

    logger.info("running prepare_data_for_viz.py for: {}, {}, {}, {}".format(RUN_ID, RUN_SCENARIO, SCENARIO_GROUP, RUN_DESCRIPTION))
    logger.info("model data dir: {}".format(MOEDL_RUN_DIR))
    logger.info("write out files for dashboard viz at: {}".format(VIZ_READY_DATA_DIR))
    logger.info("update model run inventory {}".format(MODEL_RUN_INVENTORY_FILE))


    ############ load helper files
    # jurisdiction name crosswalk, used to make naming convention consistent with in shapefile
    # NOTE: this step may not be needed when we have consistent jurisdiction naming convention
    juris_name_crosswalk_df = pd.read_csv(
        JURIS_NAME_CROSSWALK_FILE, 
        usecols = ['baus_output_juris_name', 'shapefile_juris_name'])
    juris_name_crosswalk_df.rename(columns={'baus_output_juris_name': 'JURIS'}, inplace=True)

    # definition of strategy geometry categories
    if RUN_SCENARIO in ['s24', 's25']:
        zoningmodcat_df = pd.read_csv(FBPZONINGMODCAT_SYSTEM_FILE)
    elif RUN_SCENARIO in ['s28']:
        zoningmodcat_df = pd.read_csv(EIRZONINGMODCAT_SYSTEM_FILE)
    # just in case - fill NA and modify capital letter to be consistent with strategy queries
    for colname in list(zoningmodcat_df):
        zoningmodcat_df[colname].fillna('', inplace=True)

    ############ process model input data

    # model settings and assumptions
    # cost-shifter
    cost_shifter = extract_modeling_settings(SETTING_INPUT_FILE,
                                                VIZ_MODEL_SETTING_FILE,
                                                get_cost_shifter=True)
    

    # pipeline projects and strategy-based asserted projects
    pipeline, strategy_projects = process_dev_proj_list(DEV_PROJ_FILE,
                                                        VIZ_PIPELINE_FILE,
                                                        VIZ_STRATEGY_PROJECT_FILE,
                                                        building_source_recode,
                                                        pipeline_src,
                                                        strategy_proj_src,
                                                        dev_proj_list_cols)

    # regional controls
    regional_controls_df = consolidate_regional_control(HH_CONTROL_FILE,
                                                        EMP_CONTROL_FILE,
                                                        VIZ_REGIONAL_CONTROLS_FILE)

    # strategy - upzoning
    zmod_df = process_zmods(ZMODS_FILE,
                            zmods_cols,
                            zoningmodcat_df,
                            VIZ_STRATEGY_UPZONING_FILE)
    
    ## strategies in the yaml file
    with open(POLICY_INPUT_FILE, 'r') as stream:
        try:
            policy_yaml = yaml.safe_load(stream)
            # print(policy_yaml)
        except yaml.YAMLError as exc:
            logger.warning(exc)
    
    # get default inclusionary and inclusionary strategy tables
    inclusionary_default, inclusionary_strategy = inclusionary_yaml_to_df(policy_yaml,
                                                                          VIZ_BASELINE_INCLUSIONARY_FILE,
                                                                          VIZ_STRATEGY_INCLUSIONARY_FILE,
                                                                          zoningmodcat_df,
                                                                          juris_name_crosswalk_df)
    
    # strategy - housing bond subsidies
    housing_bond_subsidy = housing_bond_subsidy_yaml_to_df(policy_yaml,
                                                           zoningmodcat_df,
                                                           VIZ_STRATEGY_HOUSING_BOND_FILE)
    

    # strategy - development cost reduction, represented by columns 'housing_devCost_reduction_cat' and 'housing_devCost_reduction_amt'
    housing_dev_cost_reduction = dev_cost_reduction_yaml_to_df(policy_yaml, zoningmodcat_df, VIZ_STRATEGY_DEV_COST_FILE)
    
    
    # strategy - naturally-occurring affordable housing preservation
    housing_preserve_target, housing_reserve_qualify = housing_preserve_yaml_to_df(policy_yaml,
                                                                                   zoningmodcat_df,
                                                                                   VIZ_STRATEGY_PRESERVE_TARGET_FILE,
                                                                                   VIZ_STRATEGY_PRESERVE_QUALIFY_FILE)

    ############ process model interim data

    # TAZ logsum data
    taz_logsum = consolidate_taz_logsum(RAW_INTERIM_DIR,
                                        RUN_ID,
                                        VIZ_TAZ_LOGSUM_FILE)

    ############ process model output data
    
    # TAZ HH and Employment growth output, from '[RUN_ID]_taz_summaries_[year].csv' to '[RUN_ID]_taz_summary.csv' 
    taz_summaries_df = consolidate_TAZ_summaries(RAW_OUTPUT_DIR,
                                                 RUN_ID,
                                                 VIZ_TAZ_SUMMARY_FILE,
                                                 taz_growth_attr_cols,
                                                 taz_hh_attr_cols,
                                                 taz_emp_attr_cols)

    # Jurisdiction HH and Employment growth output, from '[RUN_ID]_juris_summaries_[year].csv' to '[RUN_ID]_juris_summary.csv'
    juris_summaries_df = consolidate_juris_summaries(RAW_OUTPUT_DIR,
                                                     RUN_ID,
                                                     VIZ_JURIS_SUMMARY_FILE,
                                                     juris_growth_attr_cols, 
                                                     juris_hh_attr_cols, 
                                                     juris_emp_attr_cols,
                                                     juris_name_crosswalk_df)

    # Deed-restrcited housing output, from '[RUN_ID]_juris_summaries_[year].csv' to  and '[RUN_ID]_dr_units_growth.csv'
    juris_dr_units_growth_df = summarize_deed_restricted_units(RAW_OUTPUT_DIR,
                                                               RUN_ID,
                                                               VIZ_DR_UNITS_GROWTH_FILE,
                                                               juris_dr_units_atrr_cols,
                                                               juris_name_crosswalk_df)

    # growth summary by Plan growth geographies, from '[RUN_ID]_[geos]_growth_summaries.csv' to '[RUN_ID]_growth_geos_summary.csv'
    growth_summary_GEOS = summarize_growth_by_growth_geographies(RAW_OUTPUT_DIR,
                                                                 RUN_ID,
                                                                 VIZ_GROWTH_GEOS_SUMMARY_FILE,
                                                                 geo_tag_types,
                                                                 geos_tag_recode)

    # simplify parcel_output data '[RUN_ID]_parcel_output.csv'
    parcel_output = simplify_parcel_output(RAW_OUTPUT_DIR,
                                           RUN_ID,
                                           VIZ_PARCEL_OUTPUT_SUMMARY_FILE)

  
    ############ uploading the files to Redshift for Tableau visualization
    
    if UPLOAD_TO_REDSHIFT:
        
        # all datasets to upload, along with the file name (used to construct Redshift table name)
        to_upload_to_redshift = {
            VIZ_MODEL_SETTING_FILE: cost_shifter,
            VIZ_REGIONAL_CONTROLS_FILE: regional_controls_df,
            VIZ_PIPELINE_FILE: pipeline,
            VIZ_STRATEGY_PROJECT_FILE: strategy_projects,
            VIZ_STRATEGY_UPZONING_FILE: zmod_df,
            VIZ_BASELINE_INCLUSIONARY_FILE: inclusionary_default,
            VIZ_STRATEGY_INCLUSIONARY_FILE: inclusionary_strategy,
            VIZ_STRATEGY_HOUSING_BOND_FILE: housing_bond_subsidy,
            VIZ_STRATEGY_DEV_COST_FILE: housing_dev_cost_reduction,
            VIZ_STRATEGY_PRESERVE_TARGET_FILE: housing_preserve_target,
            VIZ_STRATEGY_PRESERVE_QUALIFY_FILE: housing_reserve_qualify,

            VIZ_TAZ_LOGSUM_FILE: taz_logsum,

            VIZ_TAZ_SUMMARY_FILE: taz_summaries_df,
            VIZ_JURIS_SUMMARY_FILE: juris_summaries_df,
            VIZ_DR_UNITS_GROWTH_FILE: juris_dr_units_growth_df,
            VIZ_GROWTH_GEOS_SUMMARY_FILE: growth_summary_GEOS,
            VIZ_PARCEL_OUTPUT_SUMMARY_FILE: parcel_output
            }
        # loop through the list and upload
        for dataset_name in to_upload_to_redshift:
            redshift_table_name = os.path.basename(dataset_name).split('.csv')[0]
            upload_df_to_redshift(to_upload_to_redshift[dataset_name],
                                  redshift_table_name)

    ############ consolidate, create shapefile, and upload to ArcGIS online for ArcGIS visualization
    
    if UPLOAD_TO_AGOL:

        dataset_to_upload = []
        
        ## growth geography-level data, fbpchcat for inclusionary strategy and zoningmodcat for other strategies
        
        # zoningmodcat shapefile, to join to non-spatial data and then upload
        if RUN_SCENARIO in ['s24', 's25']:
            zoningmodcat_gdf = gpd.read_file(FBPZONINGMODCAT_SHP_FILE)
        elif RUN_SCENARIO in ['s28']:
            zoningmodcat_gdf = gpd.read_file(EIRZONINGMODCAT_SHP_FILE)
        
        # add other growth geography tags, e.g. gg_id, jurisdiction, etc.
        zoningmodcat_gdf = pd.merge(zoningmodcat_gdf,
                                    zoningmodcat_df,
                                    left_on=['zmodscat', 'geo_type'],
                                    right_on=['zoningmodcat', 'geo_type'],
                                    how='inner')
        # double check growth geography categories
        if zoningmodcat_gdf.shape[0] != zoningmodcat_df.shape[0]:
            logger.warning('growth geography categories not match!')

        # attach strategy data as columns
        zoningmodcat_strategy = pd.merge(zoningmodcat_gdf,
                                        zmod_df[['zoningmodcat'] + zmods_cols],
                                        on='zoningmodcat',
                                        how='left')
        zoningmodcat_strategy = pd.merge(zoningmodcat_strategy,
                                        housing_bond_subsidy[['zoningmodcat', 'housing_bonds']],
                                        on='zoningmodcat',
                                        how='left')
        zoningmodcat_strategy = pd.merge(zoningmodcat_strategy,
                                        housing_dev_cost_reduction[['zoningmodcat', 'housing_devCost_reduction_cat', 'housing_devCost_reduction_amt']],
                                        on='zoningmodcat',
                                        how='left')
        zoningmodcat_strategy = pd.merge(zoningmodcat_strategy,
                                        housing_reserve_qualify[['zoningmodcat', 'preserve_candidate']],
                                        on='zoningmodcat',
                                        how='left')
        # add to uploading list
        dataset_to_upload.append(zoningmodcat_strategy)

        # fbpchcat geography - inclusionary strategy
        fbpchcat_gdf = gpd.read_file(FBPCHCAT_SHP_FILE)
        fbpchcat_strategy = pd.merge(fbpchcat_gdf,
                                     inclusionary_strategy[['geo_category', 'IZ_amt']],
                                     left_on='fbpchcat',
                                     right_on='geo_category',
                                     how='left')
        # add to uploading list
        dataset_to_upload.append(fbpchcat_strategy)
        
        ## TAZ-level data

        # TAZ shapefile
        taz_shp = gpd.read_file(TAZ_SHAPE_FILE)

        # taz-level data
        taz_summaries_gdf = pd.merge(taz_shp[['TAZ1454', 'geometry']],
                                     taz_summaries_df,
                                     left_on='TAZ1454',
                                     right_on='TAZ',
                                     how='inner')
        # double check
        if taz_summaries_gdf.shape[0] != taz_summaries_df.shape[0]:
            logger.warning('TAZ not match')
        
        # add to uploading list
        dataset_to_upload.append(taz_summaries_gdf)

        ## jurisdiction-level data
        # jurisdiction shapefile
        juris_shp = gpd.read_file(JURIS_SHAPE_FILE)

        # attach cost shifter setting
        juris_input_gdf = pd.merge(juris_shp,
                                   cost_shifter.reset_index().rename(columns={'index': 'county'}),
                                   on='county',
                                   how='left')
        # attach default inclusionary policy
        juris_input_gdf = pd.merge(juris_input_gdf,
                                   inclusionary_default,
                                   left_on='jurisdicti',
                                   right_on='geo_category',
                                   how='left')
        # add to uploading list
        dataset_to_upload.append(juris_input_gdf)
        
        # jurisdiction model output
        juris_summaries_gdf = pd.merge(juris_shp,
                                       juris_summaries_df,
                                       left_on='jurisdicti',
                                       right_on='shapefile_juris_name',
                                       how='inner')
        # double check
        if juris_summaries_gdf.shape[0] != juris_summaries_df.shape[0]:
            logger.warning('Jurisdiction not match')

        # add to uploading list
        dataset_to_upload.append(juris_summaries_gdf)

        ## parcel-level dataalready have X/Y, add to uploading list
        for dataset in [pipeline, strategy_projects, parcel_output]:
            dataset_to_upload.append(dataset)

        ## upload to ArcGIS Online
        for dataset in dataset_to_upload:
            upload_gdf_to_agol(dataset)


    ############ update run inventory log

    logger.info('adding the new run to model run inventory')
    model_run_inventory = pd.read_csv(
        MODEL_RUN_INVENTORY_FILE,
        dtype = {'runid': int})
    logger.info('previous runs: {}'.format(model_run_inventory))

    # append the info of the new run if not already there
    if int(RUN_ID.split('run')[1]) not in list(model_run_inventory['runid']):
        NEW_RUN_INFO = [int(RUN_ID.split('run')[1]), RUN_SCENARIO, SCENARIO_GROUP, RUN_DESCRIPTION, LAST_RUN, MOEDL_RUN_DIR]
        logger.info('adding info of the new run: {}'.format(NEW_RUN_INFO))
        model_run_inventory.loc[len(model_run_inventory.index)] = NEW_RUN_INFO

        # for previous runs of the same scenario, update 'last_run' to 'no'
        model_run_inventory.loc[
            (model_run_inventory['scenario'] == RUN_SCENARIO) & (model_run_inventory['runid'] != int(RUN_ID.split('run')[1])), 'last_run'] = 'no'

    else:
        logger.info('run already in the inventory')

    # write out the updated inventory table
    model_run_inventory.to_csv(UPDATED_MODEL_RUN_INVENTORY_FILE, index=False)

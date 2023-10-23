USAGE = """

Calculates the development capacity (residential units, non-residential sqft) under certain version of hybrid base zoning.

"""
# Run the script by passing along the following inputs:
    # 1. folder dir of the hybrid base zoning, eg. "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim 1.5\
    #                                              PBA50\Policies\Base zoning\outputs\hybrid_base_zoning\2020_05_22_p10_plu_boc_urbansim_heuristic_10.csv"
    # 2. hybrid verion, eg. "idx_urbansim_heuristic.csv"
    # 3. folder dir for the ouput parcel-level development capacity, eg. "C:\Users\ywang\Box\Modeling and Surveys\Urban Modeling\
    #                                                                     Bay Area UrbanSim 1.5\PBA50\Policies\Base zoning\outputs\capacity"
    # 4. upzoning files, eg. 'zoning_mods_21', 'zoning_mods_22', 'zoning_mods_23' for Draft/Final Blueprint


import pandas as pd
import numpy as np
import os, argparse, time, logging


if os.getenv('USERNAME')=='ywang':
    GITHUB_PETRALE_DIR  = 'C:\\Users\\{}\\Documents\\GitHub\\petrale\\'.format(os.getenv('USERNAME'))
elif os.getenv('USERNAME')=='lzorn':
    GITHUB_PETRALE_DIR  = 'X:\\petrale'

JURIS_COUNTY_FILE = os.path.join(GITHUB_PETRALE_DIR, 'zones', 'jurisdictions', 'juris_county_id.csv')

# See Dataset_Field_Definitions_Phase1.xlsx, Build Out Capacity worksheet
# https://mtcdrive.box.com/s/efbpxbz8553e90eljvlnnq20465whyiv
ALLOWED_BUILDING_TYPE_CODES = ["HS","HT","HM","OF","HO","SC","IL","IW","IH","RS","RB","MR","MT","ME"]
RES_BUILDING_TYPE_CODES     = ["HS","HT","HM",                                        "MR"          ]
NONRES_BUILDING_TYPE_CODES  = [               "OF","HO","SC","IL","IW","IH","RS","RB","MR","MT","ME"]

INTENSITY_CODES             = ["far", "dua", "height"]

# human-readable idx values for hybrid indexing
USE_PBA40 = 0
USE_BASIS = 1

# used in calculate_capacity()
SQUARE_FEET_PER_ACRE        = 43560.0
SQUARE_FEET_PER_DU          = 1000.0
FEET_PER_STORY              = 12.0
PARCEL_USE_EFFICIENCY       = 0.8

SQUARE_FEET_PER_EMPLOYEE = {'OF': 355.0,
                            'HO': 1161.0,
                            'SC': 470.0,
                            'IL': 661.0,
                            'IW': 960.0,
                            'IH': 825.0,
                            'RS': 445.0,
                            'RB': 445.0,
                            'MR': 383.0,
                            'MT': 383.0,
                            'ME': 383.0}

def get_jurisdiction_county_df():
    """
    Returns dataframe with two columns: juris_name, county_name
    juris_name is lower case, no spaces, with underscores (e.g. "unincorporated_contra_costa")
    county_name includes spaces with first letters capitalized (e.g. "Contra Costa")
    """

    # obtain jurisdiction list
    juris_df = pd.read_csv(JURIS_COUNTY_FILE, usecols = ['juris_name_full', 'county_name'])
    juris_df.rename(columns = {'juris_name_full': 'juris_name'}, inplace = True)
    return juris_df

def set_allow_dev_type(df_original,boc_source):
    """
    Assign allow residential and/or non-residential by summing the columns
    for the residential/nonresidential allowed building type codes
    Returns dataframe with 3 columns: PARCEL_ID, allow_res_[boc_source], allow_nonres_[boc_source]
    """

    # don't modify passed df
    df = df_original.copy()

    # note that they can't be null because then they won't sum -- so make a copy and fillna with 0
    for dev_type in ALLOWED_BUILDING_TYPE_CODES:
        df[dev_type+"_"+boc_source] = df[dev_type+"_"+boc_source].fillna(value=0.0)    
    
    # allow_res is sum of allowed building types that are residential
    res_allowed_columns = [btype+'_'+boc_source for btype in RES_BUILDING_TYPE_CODES]
    df['allow_res_' +boc_source] = df[res_allowed_columns].sum(axis=1)
    
    # allow_nonres is the sum of allowed building types that are non-residential
    nonres_allowed_columns = [btype+'_'+boc_source for btype in NONRES_BUILDING_TYPE_CODES]
    df['allow_nonres_'+boc_source] = df[nonres_allowed_columns].sum(axis=1)
    
    return df[['PARCEL_ID',
               "allow_res_"    +boc_source,
               "allow_nonres_" +boc_source]]

def create_hybrid_parcel_data_from_juris_idx(logger, df_original,hybrid_idx):
    """
    Apply hybrid jurisdiction index to plu_boc parcel data
    * df_original is a parcel dataframe with pba40 and basis attributes
    * hybrid_idx is a dataframe with juris_name and _idx columns for each allowed building type or intensity attribute
      e.g. HS_idx, HT_idx, HM_idx, OF_idx, etc.... max_far_idx, max_dua_idx, etc
      Note: XX_idx is one of [ USE_PBA40, USE_BASIS ]

    Returns a dataframe with columns PARCEL_ID, juris_zmod, plus for each allowed building type or intensity attribute,
    * XX_idx: one of [ USE_PBA40, USE_BASIS ]
    * XX_urbansim: the value of XX_basis if XX_idx==USE_BASIS otherwise the value of XX_pba40

    """
    logger.info("Applying hybrid index; hybrid_idx:\n{}".format(hybrid_idx.head()))
    # logger.info("df_original.dtypes: \n{}".format(df_original.dtypes))

    # don't modify passed df
    df = df_original.copy()
    keep_cols = ['PARCEL_ID', 'juris_zmod']

    # join parcel dataframe with jurisdiction hybrid_idx on juris_zmod == juris_name
    # this brings in XX_idx
    urbansim_df = pd.merge(left    =df_original.copy(),
                           right   =hybrid_idx,
                           left_on ='juris_zmod',
                           right_on='juris_name',
                           how     = 'left')

    # bring in the allowed development type values
    for dev_type in ALLOWED_BUILDING_TYPE_CODES:
        # default to BASIS
        urbansim_df["{}_urbansim".format(dev_type)] = urbansim_df["{}_basis".format(dev_type)]
        # but set to PBA40 if the idx says to use PBA40
        urbansim_df.loc[ urbansim_df["{}_idx".format(dev_type)]==USE_PBA40, 
                         "{}_urbansim".format(dev_type) ]                   = urbansim_df["{}_pba40".format(dev_type)]
        # keep the idx and the new column
        keep_cols.append("{}_idx".format(dev_type))
        keep_cols.append("{}_urbansim".format(dev_type))

    # bring in the intensity type values
    for intensity in INTENSITY_CODES:
        # default to BASIS
        urbansim_df["max_{}_urbansim".format(intensity)] = urbansim_df["max_{}_basis".format(intensity)]
        # but set to PBA40 if the idx says to use PBA40
        urbansim_df.loc[ urbansim_df["max_{}_idx".format(intensity)]==USE_PBA40, 
                         "max_{}_urbansim".format(intensity) ]              = urbansim_df["max_{}_pba40".format(intensity)]

        # keep the idx and the new column
        keep_cols.append("max_{}_idx".format(intensity))
        keep_cols.append("max_{}_urbansim".format(intensity))

    return urbansim_df[keep_cols]


def calculate_capacity(df_original,boc_source,nodev_source,pass_thru_cols=[]):
    """
    Calculate the development capacity in res units, non-res sqft, and employee estimates
    
    Inputs:
     * df_original:                 parcel-zoning dataframe, mapping parcel_id to zoning attributes including
                                    allowable development types and development intensities
     * boc_source:                  source of parcel-zoning data, could be 'pba40', 'basis', 'urbansim', or 'upzoning'
     * nodev_source:                which 'nodev' labels for parcels are used, either 'pba40' or 'basis'
     * building_parcel_df_original: parcel data joined to building data, which is used to determine parcel
                                    existing characteristics - these characteristics determine if a parcel's
                                    net capacity is zero or equals the raw capacity
     * calculate_net:               if calculate_net = True, calculate both raw and net capacity; if 
                                    calculate_net = False, only calculate raw capacity.

    Returns dataframe with columns:
     * PARCEL_ID
     * columns in pass_thru_cols
     * zoned_du_[boc_source]: residential units, calculated from ACRES x max_dua_[boc_source]
                           (set to zero if allow_res_[boc_source]==0 or nodev_[nodev_source]==1)
     * zoned_sqft_[boc_source] : building sqft, calculated from ACRES x max_far_[boc_source]
                           (set to zero if allow_nonres_[boc_source]==0 or nodev_[nodev_source]==1)
     * zoned_Ksqft_[boc_source]: sqft_[boc_source]/1,000
     * job_spaces_[boc_source]  : estimate of employees from sqft_[boc_source]

     And if calculate_net = True, additionally columns to determine raw vs. net capacity:
     * parcel_vacant
     * has_old_building
     * is_under_built_[boc_source]
     * res_zoned_existing_ratio_[boc_source]
     * nonres_zoned_existing_ratio_[boc_source]
    
    """
    
    df = df_original.copy()
    
    # DUA calculations apply to parcels 'allowRes'
    df['zoned_du_'+boc_source] = df['ACRES'] * df['max_dua_'+boc_source]   
    
    # zero out units for 'nodev' parcels or parcels that don't allow residential
    zero_unit_idx = (df['allow_res_'+boc_source] == 0) | (df['nodev_'+nodev_source] == 1)
    df.loc[zero_unit_idx,'zoned_du_'   +boc_source] = 0
        
    # FAR calculations apply to parcels 'allowNonRes'
    df['zoned_sqft_' +boc_source] = df['ACRES'] * df['max_far_'+boc_source] * SQUARE_FEET_PER_ACRE 
    
    # zero out sqft for 'nodev' parcels or parcels that don't allow non-residential
    zero_sqft_idx = (df['allow_nonres_'+boc_source] == 0) | (df['nodev_'+nodev_source] == 1)
    df.loc[zero_sqft_idx,'zoned_sqft_' +boc_source] = 0
    
    df['zoned_Ksqft_'+boc_source] = df['zoned_sqft_'+boc_source]*0.001

    # calculate job_spaces
    df['job_spaces_'+boc_source] = 0
    for dev_type in NONRES_BUILDING_TYPE_CODES:
        df.loc[df[dev_type+'_'+boc_source] == 1, 'job_spaces_'+boc_source] = df['zoned_sqft_'+boc_source] / SQUARE_FEET_PER_EMPLOYEE[dev_type]
    
    keep_cols = ['PARCEL_ID'] + pass_thru_cols + \
    [
        "zoned_du_"        +boc_source,
        "zoned_sqft_"      +boc_source,
        "zoned_Ksqft_"     +boc_source,
        "job_spaces_"      +boc_source
    ]

    return df[keep_cols]


def calculate_net_capacity(logger, df_original,boc_source,nodev_source,
                           building_parcel_df_original,
                           net_pass_thru_cols=[]):
    """
    Calculate the net development capacity in res units, non-res sqft, and employee estimates
    
    Inputs:
     * df_original:                 parcel-zoning dataframe, mapping parcel_id to zoning attributes including
                                    allowable development types and development intensities
     * boc_source:                  source of parcel-zoning data, could be 'pba40', 'basis', 'urbansim', or 'upzoning'
     * nodev_source:                which 'nodev' labels for parcels are used, either 'pba40' or 'basis'
     * building_parcel_df_original: parcel data joined to building data, which is used to determine parcel
                                    existing characteristics - these characteristics determine if a parcel's
                                    net capacity is zero or equals the raw capacity

    Returns dataframe with columns:
     * PARCEL_ID
     * columns in pass_thru_cols
     * parcel_vacant
     * has_old_building
     * is_under_built_[boc_source]
     * res_zoned_existing_ratio_[boc_source]
     * nonres_zoned_existing_ratio_[boc_source]

       net capacity based on various criteria:
     * zoned_du_vacant_[boc_source]: capacity only of vacant parcels
     * zoned_sqft_vacant_[boc_source]
     * zoned_Ksqft_vacant_[boc_source]
     * job_spaces_vacant_[boc_source]

     * zoned_du_underbuild_[boc_source]: capacity only of under-built parcels
     * zoned_sqft_underbuild_[boc_source]
     * zoned_Ksqft_underbuild_[boc_source]
     * job_spaces_underbuild_[boc_source]

     * zoned_du_underbuild_noProt_[boc_source]: capacity of parcels that are vacant or under-built, but without protected (old) building
     * zoned_sqft_underbuild_noProt_[boc_source]
     * zoned_Ksqft_underbuild_noProt_[boc_source]
     * job_spaces_underbuild_noProt_[boc_source]
    
    """
    capacity_raw = calculate_capacity(df_original,boc_source,nodev_source,pass_thru_cols=net_pass_thru_cols)

    building_parcel_df = building_parcel_df_original.copy()

    # label vacant building based on building's development_type_id
    # https://github.com/BayAreaMetro/petrale/blob/master/incoming/dv_buildings_det_type_lu.csv
    building_parcel_df["building_vacant"] = 0.0
    building_parcel_df.loc[building_parcel_df.development_type_id== 0, "building_vacant"] = 1.0
    building_parcel_df.loc[building_parcel_df.development_type_id== 15, "building_vacant"] = 1.0

    # parcel_building_df_original is building-level data, therefore need to 
    # aggregate buildings at the parcel level
    building_groupby_parcel = building_parcel_df.groupby(['PARCEL_ID']).agg({
        'ACRES'               :'max',
        'LAND_VALUE'          :'max',
        'improvement_value'   :'sum',
        'residential_units'   :'sum',
        'residential_sqft'    :'sum',
        'non_residential_sqft':'sum',
        'building_sqft'       :'sum',
        'year_built'          :'min',
        'building_id'         :'min',
        'building_vacant'     :'prod'}) # all buildings must be vacant to call this building_vacant

    # Identify vacant parcels
    building_groupby_parcel["parcel_vacant"] = False
    building_groupby_parcel.loc[ building_groupby_parcel['building_id'].isnull(),   "parcel_vacant" ] = True
    building_groupby_parcel.loc[ building_groupby_parcel['building_vacant'] == 1.0, "parcel_vacant" ] = True
    building_groupby_parcel.loc[(building_groupby_parcel['improvement_value'   ] == 0) & 
                                (building_groupby_parcel['residential_units'   ] == 0) &
                                (building_groupby_parcel['residential_sqft'    ] == 0) &
                                (building_groupby_parcel['non_residential_sqft'] == 0) &
                                (building_groupby_parcel['building_sqft'       ] == 0), "parcel_vacant"] = True
    logger.info("Vacant parcel statistics: \n {}".format(building_groupby_parcel.parcel_vacant.value_counts()))

    # Identify parcels with old buildings which are protected (if multiple buildings on one parcel, take the oldest)
    # and not build on before-1940 parcels
    building_groupby_parcel['building_age'] = 'missing'
    building_groupby_parcel.loc[building_groupby_parcel.year_built >= 2000, 'building_age' ] = 'after 2000'
    building_groupby_parcel.loc[building_groupby_parcel.year_built <  2000, 'building_age' ] = '1980-2000'
    building_groupby_parcel.loc[building_groupby_parcel.year_built <  1980, 'building_age' ] = '1940-1980'
    building_groupby_parcel.loc[building_groupby_parcel.year_built <  1940, 'building_age' ] = 'before 1940'

    building_groupby_parcel['has_old_building'] = False
    building_groupby_parcel.loc[building_groupby_parcel.building_age == 'before 1940','has_old_building'] = True
    logger.info('Parcel statistics by the age of the oldest building: \n {}'.format(building_groupby_parcel.building_age.value_counts()))


    # Identify single-family parcels smaller than 0.5 acre
    building_groupby_parcel['small_HS_parcel'] = False
    small_HS_idx = (building_groupby_parcel.residential_units == 1.0) & (building_groupby_parcel.ACRES < 0.5)                   
    building_groupby_parcel.loc[small_HS_idx, 'small_HS_parcel'] = True
    logger.info("Small single-family parcel statistics: \n {}".format(building_groupby_parcel.small_HS_parcel.value_counts()))

    # Identify parcels smaller than 2000 sqft
    building_groupby_parcel['small_parcel'] = False
    small_parcel_idx = (building_groupby_parcel.ACRES * SQUARE_FEET_PER_ACRE) < 2000
    building_groupby_parcel.loc[small_parcel_idx, 'small_parcel'] = True
    logger.info("Small parcel (<2000 sqft) statistics: \n {}".format(building_groupby_parcel.small_parcel.value_counts()))

    # Calculate parcel's investment-land ratio
    building_groupby_parcel['ILR'] = building_groupby_parcel['improvement_value'] / building_groupby_parcel['LAND_VALUE']
    building_groupby_parcel.loc[building_groupby_parcel['LAND_VALUE'] == 0, 'ILR'] = 'n/a'


    # join to raw capacity dataframe
    capacity_with_building = pd.merge(left=capacity_raw, 
                                      right=building_groupby_parcel,
                                      how="left", 
                                      on="PARCEL_ID")

    # Identify under-built parcels and calculate the net units capacity for under-built parcels
    new_units = (capacity_with_building['zoned_du_' + boc_source] - 
                 capacity_with_building['residential_units']       - 
                 capacity_with_building['non_residential_sqft'] / SQUARE_FEET_PER_DU).clip(lower=0)
    ratio = (new_units / capacity_with_building['residential_units']).replace(np.inf, 1)
    capacity_with_building['is_under_built_' + boc_source] = ratio > 0.5
    logger.info('Under_built parcel statistics  ({}): \n {}'.format(boc_source, 
            (capacity_with_building['is_under_built_' + boc_source].value_counts())))


    # Calculate existing capactiy to zoned capacity ratio
    # ratio of existing res units to zoned res units
    capacity_with_building['res_zoned_existing_ratio_' + boc_source] = \
            (capacity_with_building['residential_units'] / capacity_with_building['zoned_du_' + boc_source]).replace(np.inf, 1).clip(lower=0)
    # ratio of existing non-res sqft to zoned non-res sqft
    capacity_with_building['nonres_zoned_existing_ratio_' + boc_source] = \
            (capacity_with_building['non_residential_sqft'] / capacity_with_building['zoned_sqft_' + boc_source]).replace(np.inf, 1).clip(lower=0)


    # calculate net capacity by different criteria
    # 1. only of vacant parcels
    for capacity_type in ["zoned_du", "zoned_sqft", "zoned_Ksqft", "job_spaces"]:
        capacity_with_building[capacity_type+'_vacant_'+boc_source] = capacity_with_building[capacity_type+'_'+boc_source]
        capacity_with_building.loc[capacity_with_building.parcel_vacant == False,
                                   capacity_type+'_vacant_'+boc_source] = 0

    # 2. only of under-built parcels
    for capacity_type in ["zoned_du", "zoned_sqft", "zoned_Ksqft", "job_spaces"]:
        capacity_with_building[capacity_type+'_underbuild_'+boc_source] = capacity_with_building[capacity_type+'_'+boc_source]
        capacity_with_building.loc[capacity_with_building['is_under_built_' + boc_source] == False,
                                   capacity_type+'_underbuild_'+boc_source] = 0

    # 3. of under-built but no protected parcels (with old building or single-family parcel < 0.5 acre)
    for capacity_type in ["zoned_du", "zoned_sqft", "zoned_Ksqft", "job_spaces"]:
        capacity_with_building[capacity_type+'_underbuild_noProt_'+boc_source] = capacity_with_building[capacity_type+'_'+boc_source]
        capacity_with_building.loc[(capacity_with_building['is_under_built_' + boc_source] == False) |
                                   (capacity_with_building.has_old_building == True) |
                                   (capacity_with_building.small_HS_parcel == True) |
                                   (capacity_with_building.small_parcel == True),
                                   capacity_type+'_underbuild_noProt_'+boc_source] = 0

    keep_cols = ['PARCEL_ID'] + net_pass_thru_cols + \
    [
        "zoned_du_vacant_"             + boc_source,
        "zoned_sqft_vacant_"           + boc_source,
        "zoned_Ksqft_vacant_"          + boc_source,
        "job_spaces_vacant_"           + boc_source,

        "zoned_du_underbuild_"         + boc_source,
        "zoned_sqft_underbuild_"       + boc_source,
        "zoned_Ksqft_underbuild_"      + boc_source,
        "job_spaces_underbuild_"       + boc_source,

        "zoned_du_underbuild_noProt_"      + boc_source,
        "zoned_sqft_underbuild_noProt_"    + boc_source,
        "zoned_Ksqft_underbuild_noProt_"   + boc_source,
        "job_spaces_underbuild_noProt_"    + boc_source,

        "parcel_vacant",
        "has_old_building",
        'small_HS_parcel',
        "ILR",

        'is_under_built_'              + boc_source,
        'res_zoned_existing_ratio_'    + boc_source,
        'nonres_zoned_existing_ratio_' + boc_source        
    ]

    return capacity_with_building[keep_cols]


if __name__ == '__main__':

    """    
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

    logger.info("BOX_dir         = {}".format(BOX_dir))
    logger.info("data_output_dir = {}".format(data_output_dir))
    """


    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument('hybrid_zoning',    metavar="hybrid_zoning.csv",  help="Input hybrid zoning")
    parser.add_argument('hybrid_version',   metavar="hybrid_version",     help="Version of input hybrid zoning")
    parser.add_argument('capacity_output',  metavar="capacity_output",    help="Capacity output folder")
    #parser.add_argument('upzoning_zmods',   metavar='zoningmods.csv',     help='Zoningmods for upzoning')
    parser.add_argument('upzoning_scenario',metavar='upzoning_scenario',   help="Scenario of input upzoning zoning")

    args = parser.parse_args()
    print(" {:15}: {}".format('hybrid_zoning',    args.hybrid_zoning))
    print(" {:15}: {}".format('hybrid_version',   args.hybrid_version))
    print(" {:15}: {}".format('capacity_output',  args.capacity_output))
    #print(" {:15}: {}".format('upzoning',         args.upzoning_zmods))
    print(" {:15}: {}".format('upzoning_scenario',args.upzoning_scenario))

    zoning_to_capacity(args.hybrid_zoning, args.hybrid_version, args.capacity_output)


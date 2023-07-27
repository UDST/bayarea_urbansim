USAGE = """

Tools to process BAUS input, interim output, final output for summarization, visualization, and comparison.

"""


import pandas as pd
import numpy as np
import os, argparse, time, logging


def load_parcel_hh_emp_data(file_name):
    """
    Load parcel data with the needed columns.
    """

    df = pd.read_csv(file_name, usecols=["parcel_id", "tothh", "totemp"])
    print("load {} rows of parcel data {}".format(df.shape[0], file_name))
    for col in ["tothh", "totemp"]:
        df[col].fillna(0, inplace=True)
    return df


def load_housing_data_from_parcel(parcel_file, res_cols):
    """
    Load residential-related columns of BAUS output parcel_data.

    Arguments:
        - parcel_file: file directory of [runID]_parcel_data_[year].csv
        - res_cols: list of residentil-related columns to load, could include:
                    'tothh', 'zoned_du_underbuild', 'zoned_du', 'zoned_du_underbuild_nodev',
                    'residential_units', 'deed_restricted_units',
                    'preserved_units', 'inclusionary_units', 'subsidized_units'
    """
    parcel_data = pd.read_csv(parcel_file, usecols=["parcel_id"] + res_cols)
    print("load {} rows of parcel data {}".format(parcel_data.shape[0], parcel_file))
    for col in res_cols:
        parcel_data[col].fillna(0, inplace=True)

    return parcel_data


def tag_parcel(parcel_df, crosswalk):
    """
    Add geographies tagging (e.g., PDA, TRA, TAZ, MAZ) or other tagging to parcels.

    Input:
        - parcel_df: parcel-level data
        - crosswalk: parcel_level parcel-to-geography crosswalk
    """

    parcel_tagged = parcel_df.merge(crosswalk, on="parcel_id", how="left")
    return parcel_tagged


def summarize_parcel_by_group(parcel_df, metrics_cols, groupby_col):
    """
    Summarize BAUS parcel-level output by certain category, e.g., TAZ, SD (superdistrict).

    Inputs:
        - parcel_df: parcel-level BAUS output
        - metrics_cols: a list of columns specifying which metrics to summarize, e.g. tothh (total households)
        - groupby_col: a column that tags parcels by geography designation,
                    e.g. column 'hra_id' tags parcels by 'hra' and 'non-hra'

    """

    parcel_summary = parcel_df.groupby(geo_col)[metrics_cols].sum()
    return parcel_summary


def load_feasibility(filename):
    """
    Load BAUS interim table 'feasibility'.
    """
    df = pd.read_csv(filename, header=[0, 1, 2]).reset_index()
    return df


def get_feasibility_by_useType(
    feasibility_df, dev_type, keep_all_cols=True, keep_only_allowed_parcels=False
):
    """
    Extract the feasbility table of one use type (development type).
    The 'feasibility' table has two levels of column indexes - level 1 is 'use' represents development type,
    including ______; level 2 contains info on factors that affect development feasibility, as well as
    calculated feasible sqft.

    """

    feasibility_df_reindex = feasibility_df.rename(
        columns={"Unnamed: 0_level_0": "parcel_id", "Unnamed: 0_level_1": "parcel_id"}
    )
    feasibility_devType = feasibility_df_reindex[["parcel_id", dev_type]]
    res_cols = feasibility_devType.columns.droplevel([0, 2])
    feasibility_devType.columns = res_cols

    if not keep_all_cols:
        res_cols_keep = [
            "parcel_id",
            "total_residential_units",  # from "buildings" table, indicating number of existing units
            "max_profit_far",  # representing development capacity (?)
            "max_profit",
            "residential_sales_price_sqft",
            "max_dua",
            "residential_sqft",  # representing development capacity
        ]

        feasibility_devType = feasibility_devType[res_cols_keep]

    if keep_only_allowed_parcels:
        # drop NA rows - parcels where the given development type is not allowed
        feasibility_devType = feasibility_devType.loc[
            feasibility_devType.max_profit.notnull()
        ]

    return feasibility_devType


def analyze_neighborhood_vars(neighborhood_nodes_df):
    return None


def compare_df_fields(df1, df2, common_id, compare_cols):
    if df1.shape[0] != df2.shape[0]:
        print("different number of rows")
    else:
        df1.columns = [x + "_1" for x in list(df1)]
        df2.columns = [x + "_2" for x in list(df2)]
        compare = df1.merge(
            df2, left_on=common_id + "_1", right_on=common_id + "_2", how="outer"
        )
        for i in compare_cols:
            print("compare ", i)
            if compare.loc[compare[i + "_1"] != compare[i + "_2"]].shape[0] > 0:
                print("column {} is not identical".format(i))

        print("finish comparison")


def summarize_rsh_simulate_result_by_parcel(
    rsh_simulate_result_file, building_data_file
):
    """
    Inputs:
    rsh_simulate_result_file: unit-level data containing unit_id (unique identifier),
                              building_id, unit_residential_price
    building_data_file: building-level data containing building_id (unique identifier),
                        parcel_id

    Output:
    parcel-level data with the following fields: parcel_id (unique identifier),
                                                 unit_res_price_min,
                                                 unit_res_price_max,
                                                 unit_res_price_mean,
                                                 unit_cnt

    """

    # read rsh_simulate result
    rsh_simulate_df = pd.read_csv(
        rsh_simulate_result_file,
        usecols=["unit_id", "unit_residential_price", "building_id"],
    )
    print(
        "read {} rows of unit-level rsh_simulate result {}".format(
            rsh_simulate_df.shape[0], rsh_simulate_result_file
        )
    )

    # add 'parcel_id' field from building data
    building_data_df = pd.read_csv(
        building_data_file, usecols=["building_id", "parcel_id"]
    )
    print(
        "read {} rows of building_data {}".format(
            building_data_df.shape[0], building_data_file
        )
    )

    rsh_simulate_df = rsh_simulate_df.merge(
        building_data_df, on="building_id", how="left"
    )

    # get parcel-level unit_residential_price values (max, min, median) and unit count
    # the model assumes all units of the same building have the same price
    rsh_simulate_parcel_df = (
        rsh_simulate_df.groupby(["parcel_id"])
        .agg({"unit_residential_price": ["min", "max", "mean"], "unit_id": "count"})
        .reset_index()
    )
    rsh_simulate_parcel_df.columns = [
        "parcel_id",
        "unit_res_price_min",
        "unit_res_price_max",
        "unit_res_price_mean",
        "unit_cnt",
    ]
    return rsh_simulate_parcel_df


def summarize_rrh_simulate_result_by_parcel(
    rrh_simulate_result_file, building_data_file
):
    """
    Inputs:
    rrh_simulate_result_file: unit-level data containing unit_id (unique identifier),
                              building_id, unit_residential_rent
    building_data_file: building-level data containing building_id (unique identifier),
                        parcel_id

    Output:
    parcel-level data with the following fields: parcel_id (unique identifier),
                                                 unit_res_rent_min,
                                                 unit_res_rent_max,
                                                 unit_res_rent_mean,
                                                 unit_cnt

    """

    # read rsh_simulate result
    rrh_simulate_df = pd.read_csv(
        rrh_simulate_result_file,
        usecols=["unit_id", "unit_residential_rent", "building_id"],
    )
    print(
        "read {} rows of unit-level rrh_simulate result {}".format(
            rrh_simulate_df.shape[0], rrh_simulate_result_file
        )
    )

    # add 'parcel_id' field from building data
    building_data_df = pd.read_csv(
        building_data_file, usecols=["building_id", "parcel_id"]
    )
    print(
        "read {} rows of building_data {}".format(
            building_data_df.shape[0], building_data_file
        )
    )

    rrh_simulate_df = rrh_simulate_df.merge(
        building_data_df, on="building_id", how="left"
    )

    # get parcel-level unit_residential_rent values (max, min, median) and unit count
    # the model assumes all units of the same building have the same price
    rrh_simulate_parcel_df = (
        rrh_simulate_df.groupby(["parcel_id"])
        .agg({"unit_residential_rent": ["min", "max", "mean"], "unit_id": "count"})
        .reset_index()
    )
    rrh_simulate_parcel_df.columns = [
        "parcel_id",
        "unit_res_rent_min",
        "unit_res_rent_max",
        "unit_res_rent_mean",
        "unit_cnt",
    ]
    return rrh_simulate_parcel_df


def summarize_parcel_output_by_parcel(parcel_output_file, sum_cols):
    """
    Inputs:
        parcel_output_file: building-level data containing a variety of info
        sum_cols: building attributes which can be summed up to parcel-level
    """
    parcel_output_df = pd.read_csv(parcel_output_file, usecols=["parcel_id"] + sum_cols)
    print(
        "read {} rows of parcel_output data {}, with {} unique parcel_id".format(
            parcel_output_df.shape[0],
            parcel_output_file,
            len(parcel_output_df.parcel_id.unique()),
        )
    )

    parcel_output_parcel_df = (
        parcel_output_df.groupby("parcel_id")[sum_cols].sum().reset_index()
    )
    return parcel_output_parcel_df


def summarize_units_by_source_from_blg(building_file):
    """ """
    building_df = pd.read_csv(
        building_file,
        usecols=[
            "building_id",
            "parcel_id",
            "residential_units",
            "unit_price",
            "source",
        ],
    )
    print(
        "read {} building_data {}, with {} unique parcel_id".format(
            building_df.shape[0], building_file, len(building_df.parcel_id.unique())
        )
    )

    # select buildings built by the residential developer model
    df_dev = building_df.loc[building_df.source == "developer_model"]
    # rename to reflect the source
    df_dev.rename(
        columns={"residential_units": "resUnits_dev", "unit_price": "unit_price_dev"},
        inplace=True,
    )
    # aggregate by parcel_id
    df_parcel_dev = (
        df_dev.groupby("parcel_id")
        .agg({"resUnits_dev": "sum", "unit_price_dev": "mean"})
        .reset_index()
    )
    df_parcel_dev.columns = ["parcel_id", "resUnits_dev", "unit_price_mean_dev"]

    # do the same for buildings not built by the developer model
    df_nondev = building_df.loc[building_df.source != "developer_model"]
    df_nondev.rename(
        columns={
            "residential_units": "resUnits_nondev",
            "unit_price": "unit_price_nondev",
        },
        inplace=True,
    )
    df_parcel_nondev = (
        df_nondev.groupby("parcel_id")
        .agg({"resUnits_nondev": "sum", "unit_price_nondev": "mean"})
        .reset_index()
    )
    df_parcel_nondev.columns = [
        "parcel_id",
        "resUnits_nondev",
        "unit_price_mean_nondev",
    ]

    # merge
    df_parcel_all = df_parcel_dev.merge(df_parcel_nondev, on="parcel_id", how="outer")

    return df_parcel_all


def compare_variables_between_scenarios(
    df, identifer, suffix_1, suffix_2, compare_cols, keep_cols=None
):
    """
    Compares variables between two scenarios (e.g. NoProject and Project).
    Arguments:
        - df: a dataframe containing one unique identifier (e.g. 'parcel_id')
              and its attributes in both scenarios, e.g. 'residential_unit', 'unit_price'.
              Field names contain scenario-suffix, e.g. 'residential_unit_NP'and
              'residential_unit_P', 'unit_price_NP' and 'unit_price_P'.
        - suffix_1, suffix_2: string representing scenario-suffix, e.g. 'NP', 'P'.
        - compare_cols: a list of attribute names, without the suffix, e.g.
              ['residential_unit', 'unit_price'].
        - keep_cols: a list of attributes shared by the two scenarios and to be
                included in the final dataframe, e.g. ['TAZ', 'SD'].

    Output:
        - a dataframe including all records (e.g. all parcels), but only attributes that
          differ between the two scenarios.
    """
    diff_cols = []

    for col in compare_cols:
        # rows with col_suffix_1 != col_suffix_2 and at least one of the scenarios is not na
        df_col_diff = df.loc[
            (df[col + "_" + suffix_1] != df[col + "_" + suffix_2])
            & (df[col + "_" + suffix_1].notnull() | df[col + "_" + suffix_2].notnull())
        ]

        if df_col_diff.shape[0] > 0:
            # check if all of these rows are all NA (also !=)
            # if (df_col_diff[col+'_'+suffix_1].isna().sum() == df_col_diff.shape[0]) \
            #    & (df_col_diff[col+'_'+suffix_2].isna().sum() == df_col_diff.shape[0]):
            #    print('NA for all rows in both {} and {}'.format(
            #     suffix_1, suffix_2))
            # else:
            print(
                "compare {}: not identical between {} and {}".format(
                    col, suffix_1, suffix_2
                )
            )
            diff_cols.append(col)
        else:
            print(
                "compare {}: identical between {} and {}".format(
                    col, suffix_1, suffix_2
                )
            )

    if len(diff_cols) > 0:
        # keep only different cols and keep_cols if any
        df_diff_cols = df[
            [identifer]
            + keep_cols
            + sorted(
                [x + "_" + suffix_1 for x in diff_cols]
                + [x + "_" + suffix_2 for x in diff_cols]
            )
        ]
        return df_diff_cols
    else:
        return None

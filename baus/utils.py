from __future__ import print_function

import pandas as pd
import numpy as np
import orca
import os
import sys
from urbansim_defaults.utils import _remove_developed_buildings
from urbansim.developer.developer import Developer as dev
import itertools as it
# for urbanforecast.com visualizer
if "URBANSIM_SLACK" in os.environ:
    import boto3
    import time
    import requests
    import json


#####################
# UTILITY FUNCTIONS
#####################


# This is really simple and basic (hackish) code to solve a very important
# problem.  I'm calling an orca step and want to serialize the tables that
# are passed in so that I can add code to this orca step and test it quickly
# without running all the models that came before, which takes many minutes.
#
# If the passed in outhdf does not exist, all the DataFarmeWrappers which
# have been passed have their DataFrames saved to the hdf5.  If the file
# does exist they all get restored, and as a byproduct their types are
# changed from DataFrameWrappers to DataFrames.  This type change is a
# major drawback of the current code, as is the fact that other non-
# DataFrame types are not serialized, but it's a start.  I plan to bring
# this problem up on the UrbanSim coders call the next time we meet.
#
#    Sample code:
#
#    d = save_and_restore_state(locals())
#    for k in d.keys():
#        locals()[k].local = d[k]
#
def save_and_restore_state(in_d, outhdf="save_state.h5"):
    if os.path.exists(outhdf):
        # the state exists - load it back into the locals
        store = pd.HDFStore(outhdf)
        out_d = {}
        for table_name in store:
            print("Restoring", table_name)
            out_d[table_name[1:]] = store[table_name]
        return out_d

    # the state doesn't exist and thus needs to be saved
    store = pd.HDFStore(outhdf, "w")
    for table_name, table in in_d.items():
        try:
            table = table.local
        except Exception as e:
            # not a dataframe wrapper
            continue
        print("Saving", table_name)
        store[table_name] = table
    store.close()
    sys.exit(0)


# similar to the function in urbansim_defaults, except it assumes you want
# to use your own pick function
def add_buildings(buildings, new_buildings,
                  remove_developed_buildings=True):

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]

    if remove_developed_buildings:
        unplace_agents = ["households", "jobs"]
        old_buildings = \
            _remove_developed_buildings(old_buildings, new_buildings,
                                        unplace_agents)

    all_buildings = dev.merge(old_buildings, new_buildings)

    orca.add_table("buildings", all_buildings)


# assume df1 and df2 each have 2 float columns specifying x and y
# in the same order and coordinate system and no nans.  returns the indexes
# from df1 that are closest to each row in df2
def nearest_neighbor(df1, df2):
    from sklearn.neighbors import KDTree
    kdt = KDTree(df1.as_matrix())
    indexes = kdt.query(df2.as_matrix(), k=1, return_distance=False)
    return df1.index.values[indexes]


# need to reindex from geom id to the id used on parcels
def geom_id_to_parcel_id(df, parcels):
    s = parcels.geom_id  # get geom_id
    s = pd.Series(s.index, index=s.values)  # invert series
    df["new_index"] = s.loc[df.index]  # get right parcel_id for each geom_id
    df = df.dropna(subset=["new_index"])
    df["new_index"] = df.new_index.astype('int')
    df = df.set_index("new_index", drop=True)
    df.index.name = "parcel_id"
    return df


def parcel_id_to_geom_id(s):
    parcels = orca.get_table("parcels")
    g = parcels.geom_id  # get geom_id
    return pd.Series(g.loc[s.values].values, index=s.index)


# This is best described by example. Imagine s is a series where the
# index is parcel ids and the values are cities, while counts is a
# series where the index is cities and the values are counts.  You
# want to end up with "counts" many parcel ids from s (like 40 from
# Oakland, 60 from SF, and 20 from San Jose).  This can be
# thought of as grouping the dataframe "s" came from and sampling
# count number of rows from each group.  I mean, you group the
# dataframe and then counts gives you the count you want to sample
# from each group.
def groupby_random_choice(s, counts, replace=True):
    if counts.sum() == 0:
        return pd.Series()

    return pd.concat([
        s[s == grp].sample(cnt, replace=replace)
        for grp, cnt in counts[counts > 0].iteritems()
    ])


# pick random indexes from s without replacement
def random_indexes(s, num, replace=False):
    return np.random.choice(
        np.repeat(s.index.values, s.values),
        num,
        replace=replace)


# This method takes a series of floating point numbers, rounds to
# integers (e.g. to while number households), while making sure to
# meet the given target for the sum.  We're obviously going to lose
# some resolution on the distrbution implied by s in order to meet
# the target exactly
def round_series_match_target(s, target, fillna=np.nan):
    if target == 0 or s.sum() == 0:
        return s
    r = s.fillna(fillna).round().astype('int')
    # handles rare cases where all values round to 0
    if r.sum() == 0:
        r = np.ceil(s).astype('int')
    diff = int(np.subtract(target, r.sum()))
    # diff = int(target - r.sum())
    if diff > 0:
        # replace=True allows us to add even more than we have now
        indexes = random_indexes(r, abs(diff), replace=True)
        r = r.add(pd.Series(indexes).value_counts(), fill_value=0)
    elif diff < 0:
        # this makes sure we can only subtract until all rows are zero
        indexes = random_indexes(r, abs(diff))
        r = r.sub(pd.Series(indexes).value_counts(), fill_value=0)

    assert r.sum() == target
    return r


# scales (floating point ok) so that the sum of s if equal to
# the specified target - pass check_close to verify that it's
# within a certain range of the target
def scale_by_target(s, target, check_close=None):
    ratio = float(target) / s.sum()
    if check_close:
        assert 1.0-check_close < ratio < 1.0+check_close
    return s * ratio


def constrained_normalization(marginals, constraint, total):
    # this method increases the marginals to match the total while
    # also meeting the matching constraint.  marginals should be
    # scaled up proportionally.  it is possible that this method
    # will fail if the sum of the constraint is less than the total

    assert constraint.sum() >= total

    while 1:

        # where do we exceed the total
        constrained = marginals >= constraint
        exceeds = marginals > constraint
        unconstrained = ~constrained

        num_constrained = len(constrained[constrained is True])
        num_exceeds = len(exceeds[exceeds is True])

        print("Len constrained = %d, exceeds = %d" %
              (num_constrained, num_exceeds))

        if num_exceeds == 0:
            return marginals

        marginals[constrained] = constraint[constrained]

        # scale up where unconstrained
        unconstrained_total = total - marginals[constrained].sum()
        marginals[unconstrained] *= \
            unconstrained_total / marginals[unconstrained].sum()

        # should have scaled up
        assert np.isclose(marginals.sum(), total)


# this should be fairly self explanitory if you know ipf
# seed_matrix is your best bet at the totals, col_marginals are
# observed column marginals and row_marginals is the same for rows
def simple_ipf(seed_matrix, col_marginals, row_marginals, tolerance=1, cnt=0):
    assert np.absolute(row_marginals.sum() - col_marginals.sum()) < 5.0

    # most numpy/pandas combinations will perform this conversion
    # automatically, but explicit is safer - see PR #99
    if isinstance(col_marginals, pd.Series):
        col_marginals = col_marginals.values

    # first normalize on columns
    ratios = col_marginals / seed_matrix.sum(axis=0)

    seed_matrix *= ratios
    closeness = np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum()
    assert np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum() < .01
    print("row closeness", closeness)
    if closeness < tolerance:
        return seed_matrix

    # first normalize on rows
    ratios = np.array(row_marginals / seed_matrix.sum(axis=1))
    ratios[row_marginals == 0] = 0
    seed_matrix = seed_matrix * ratios.reshape((ratios.size, 1))
    assert np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum() < .01
    closeness = np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum()
    print("col closeness", closeness)
    if closeness < tolerance:
        return seed_matrix

    if cnt >= 50:
        return seed_matrix

    return simple_ipf(seed_matrix, col_marginals, row_marginals,
                      tolerance, cnt+1)


"""
BELOW IS A SET OF UTITLIES TO COMPARE TWO SUMMARY DATAFRAMES, MAINLY LOOKING
FOR PCT DIFFERENCE AND THEN FORMATTING INTO A DESCRIPTION OR INTO AN EXCEL
OUTPUT FILE WHICH IS COLOR CODED TO HIGHLIGHT THE DIFFERENCES
"""

# for labels and cols in df1, find value in df2, and make sure value is
# within pctdiff - if not return dataframe col, row and values in two frames
# pctdiff should be specified as a number between 1 and 100


def compare_dfs(df1, df2):

    df3 = pd.DataFrame(index=df1.index, columns=df1.columns)

    # for each row
    for label, row in df1.iterrows():

        # assume row exists in comparison
        rowcomp = df2.loc[label]

        # for each value
        for col, val in row.iteritems():

            val2 = rowcomp[col]

            df3.loc[label, col] = \
                int(abs(val - val2) / ((val + val2 + .01) / 2.0) * 100.0)

    return df3


# identify small values as a boolean T/F for each column
def small_vals(df):

    df = df.copy()

    for col in df.columns:
        df[col] = df[col] < df[col].mean() - .5*df[col].std()

    return df


def compare_dfs_excel(df1, df2, excelname="out.xlsx"):
    import palettable
    import xlsxwriter

    writer = pd.ExcelWriter(excelname, engine='xlsxwriter')

    df1.reset_index().to_excel(writer, index=False, sheet_name='df1')
    df2.reset_index().to_excel(writer, index=False, sheet_name='df2')
    writer.sheets['df1'].set_zoom(150)
    writer.sheets['df2'].set_zoom(150)

    df3 = compare_dfs(df1, df2)

    df3.reset_index().to_excel(writer, index=False, sheet_name='comparison')

    workbook = writer.book
    worksheet = writer.sheets['comparison']
    worksheet.set_zoom(150)

    color_range = "B2:Z{}".format(len(df1)+1)

    reds = palettable.colorbrewer.sequential.Blues_5.hex_colors
    reds = {
        i: workbook.add_format({'bg_color': reds[i]})
        for i in range(len(reds))
    }

    blues = palettable.colorbrewer.sequential.Oranges_5.hex_colors
    blues = {
        i: workbook.add_format({'bg_color': blues[i]})
        for i in range(len(blues))
    }

    def apply_format(worksheet, df, format, filter):

        s = df.stack()

        for (lab, col), val in filter(s).iteritems():

            rowind = df.index.get_loc(lab)+2
            colind = df.columns.get_loc(col)+1
            colletter = xlsxwriter.utility.xl_col_to_name(colind)

            worksheet.write(colletter+str(rowind), val, format)

    for i in range(5):
        apply_format(worksheet, df3, reds[i], lambda s: s[s > i*10+10])

    df3[small_vals(df1)] = np.nan
    for i in range(5):
        apply_format(worksheet, df3, blues[i], lambda s: s[s > i*10+10])

    writer.save()


# compare certain columns of two dataframes for differences above a certain
# amount and return a string describing the differences
def compare_summary(df1, df2, index_names=None, pctdiff=10,
                    cols=["tothh", "totemp"], geog_name="Superdistrict"):

    if cols:
        df1, df2 = df1[cols], df2[cols]

    df3 = compare_dfs(df1, df2)
    df3[small_vals(df1)] = np.nan
    s = df3[cols].stack()

    buf = ""
    for (lab, col), val in s[s > 10].iteritems():
        lab = index_names.loc[lab]
        buf += "%s '%s' is %d%% off in column '%s'\n" % \
            (geog_name, lab, val, col)

    return buf


def ue_config(run_num, host):
    data = {
        'taz_url': ('https://landuse.s3.us-west-2.amazonaws.com/'
                    'run{}_simulation_output.json'.format(run_num)),
        'parcel_url': ('https://landuse.s3.us-west-2.amazonaws.com/'
                       'run{}_parcel_output.csv'.format(run_num)),
        'timestamp': time.time(),
        'name': 'Simulation run {}, Machine {}'.format(run_num, host)
    }

    r = requests.post(
            'https://forecast-feedback.firebaseio.com/simulations.json',
            json.dumps(data))

    return r.text


def ue_files(run_num):
    s3 = boto3.client('s3')
    resp1 = s3.upload_file(
        'runs/run{}_simulation_output.json'.format(run_num),
        'landuse',
        'run{}_simulation_output.json'.format(run_num),
        ExtraArgs={'ACL': 'public-read'})
    resp2 = s3.upload_file(
        'runs/run{}_parcel_output.csv'.format(run_num),
        'landuse',
        'run{}_parcel_output.csv'.format(run_num),
        ExtraArgs={'ACL': 'public-read'})
    return resp1, resp2


# MIGRATED FROM OUTPUT_CSV_UTILS.PY

geography = 'taz'


# loosely borrowed from https://gist.github.com/haleemur/aac0ac216b3b9103d149
def format_df(df, formatters=None, **kwargs):
    formatting_columns = list(set(formatters.keys()).intersection(df.columns))
    df_copy = df[formatting_columns].copy()
    na_rep = kwargs.get('na_rep') or ''
    for col, formatter in formatters.items():
        try:
            df[col] = df[col].apply(lambda x: na_rep if pd.isnull(x)
                                    else formatter.format(x))
        except KeyError:
            print('{} does not exist in the dataframe.'.format(col)) +\
                'Ignoring the formatting specifier'
    return df


def get_base_year_df(base_run_year=2010):
    geography_id = 'zone_id' if geography == 'taz' else geography
    df = pd.read_csv(
        'output/baseyear_{}_summaries_{}.csv'.format(geography, base_run_year),
        index_col=geography_id)
    df = df.fillna(0)
    return df


def get_outcome_df(run, year=2040):
    geography_id = 'zone_id' if geography == 'taz' else geography
    df = pd.read_csv(
        'http://urbanforecast.com/runs/run' +
        '%(run)d_%(geography)s_summaries_%(year)d.csv'
        % {"run": run, "year": year, "geography": geography},
        index_col=geography_id)
    df = df.fillna(0)
    return df


def write_outcome_csv(df, run, geography, year=2040):
    geography_id = 'zone_id' if geography == 'taz' else geography
    f = 'runs/run%(run)d_%(geography)s_summaries_%(year)d.csv' \
        % {"run": run, "year": year, "geography": geography}
    df = df.fillna(0)
    df.to_csv(f)


def compare_series(base_series, outcome_series, index):
    s = base_series
    s1 = outcome_series
    d = {
        'Count': s1,
        'Share': s1 / s1.sum(),
        'Percent_Change': 100 * (s1 - s) / s,
        'Share_Change': (s1 / s1.sum()) - (s / s.sum())
    }
    # there must be a less verbose way to do this:
    columns = ['Count', 'Share', 'Percent_Change',
               'Share_Change']
    df = pd.DataFrame(d, index=index, columns=columns)
    return df


def compare_outcome(run, base_series, formatters):
    df = get_outcome_df(run)
    s = df[base_series.name]
    df = compare_series(base_series, s, df.index)
    df = format_df(df, formatters)
    return df


def remove_characters(word, characters=b' _aeiou'):
    return word.translate(None, characters)


def make_esri_columns(df):
    df.columns = [str(x[0]) + str(x[1]) for x in df.columns]
    df.columns = [remove_characters(x) for x in df.columns]
    return df
    df.to_csv(f)


def to_esri_csv(df, variable, runs):
    f = 'compare/esri_' +\
        '%(variable)s_%(runs)s.csv'\
        % {"variable": variable,
           "runs": '-'.join(str(x) for x in runs)}
    df = make_esri_columns(df)
    df.to_csv(f)


def write_bundle_comparison_csv(df, variable, runs):
    df = make_esri_columns(df)
    if variable == "tothh" or variable == "TOTHH":
        headers = ['hh10', 'hh10_shr', 'hh40_0', 'hh40_0_shr',
                   'pctch40_0', 'Shrch40_0', 'hh40_3', 'hh40_3_shr',
                   'pctch40_3', 'shrch40_3', 'hh40_1', 'hh40_1_shr',
                   'pctch40_1', 'shrch40_1', 'hh40_2', 'hh40_2_shr',
                   'pctch40_2', 'shrch40_2', '3_40_0_40_rat',
                   '1_40_0_40_rat', '2_40_0_40_rat']
        df.columns = headers
        df = df[[
            'hh10', 'hh10_shr', 'hh40_0', 'hh40_0_shr',
            'pctch40_0', 'Shrch40_0', 'hh40_3', 'hh40_3_shr', 'pctch40_3',
            'shrch40_3', '3_40_0_40_rat', 'hh40_1', 'hh40_1_shr', 'pctch40_1',
            'shrch40_1', '1_40_0_40_rat', 'hh40_2', 'hh40_2_shr', 'pctch40_2',
            'shrch40_2', '2_40_0_40_rat']]
    elif variable == "totemp" or variable == "TOTEMP":
        headers = [
            'emp10', 'emp10_shr', 'emp40_0',
            'emp40_0_shr', 'pctch40_0', 'Shrch40_0', 'emp40_3',
            'emp40_3_shr', 'pctch40_3', 'shrch40_3', 'emp40_1',
            'emp40_1_shr', 'pctch40_1', 'shrch40_1', 'emp40_2',
            'emp40_2_shr', 'pctch40_2', 'shrch40_2', '3_40_0_40_rat',
            '1_40_0_40_rat', '2_40_0_40_rat']
        df.columns = headers
        df = df[[
            'emp10', 'emp10_shr', 'emp40_0',
            'emp40_0_shr', 'pctch40_0', 'Shrch40_0', 'emp40_3',
            'emp40_3_shr', 'pctch40_3', 'shrch40_3', '3_40_0_40_rat',
            'emp40_1', 'emp40_1_shr', 'pctch40_1', 'shrch40_1',
            '1_40_0_40_rat', 'emp40_2', 'emp40_2_shr', 'pctch40_2',
            'shrch40_2', '2_40_0_40_rat']]
    cut_variable_name = variable[3:]
    f = 'compare/' + \
        '%(geography)s_%(variable)s_%(runs)s.csv'\
        % {"geography": geography,
           "variable": cut_variable_name,
           "runs": '_'.join(str(x) for x in runs)}
    df.to_csv(f)


def write_csvs(df, variable, runs):
    f = 'compare/' +\
        '%(variable)s_%(runs)s.csv'\
        % {"variable": variable,
           "runs": '-'.join(str(x) for x in runs)}
    write_bundle_comparison_csv(df, variable, runs)


def divide_series(a_tuple, variable):
    s = get_outcome_df(a_tuple[0])[variable]
    s1 = get_outcome_df(a_tuple[1])[variable]
    s2 = s1 / s
    s2.name = str(a_tuple[1]) + '/' + str(a_tuple[0])
    return s2


def get_combinations(nparray):
    return pd.Series(list(it.combinations(np.unique(nparray), 2)))


def compare_outcome_for(variable, runs, set_geography):
    global geography
    geography = set_geography
    # empty list to build up dataframe from other dataframes
    base_year_df = get_base_year_df()
    df_lst = []
    s = base_year_df[variable]
    s1 = s / s.sum()
    d = {'Count': s, 'Share': s1}

    df = pd.DataFrame(d, index=base_year_df.index)
    if geography == 'superdistrict':
        formatters = {
            'Count': '{:.0f}',
            'Share': '{:.2f}'}
        df = pd.DataFrame(d, index=base_year_df.index)
        df = format_df(df, formatters)
        df_lst.append(df)
        more_formatters = {
            'Count': '{:.0f}',
            'Share': '{:.2f}',
            'Percent_Change': '{:.0f}',
            'Share_Change': '{:.3f}'}
        for run in runs:
            df_lst.append(compare_outcome(run, s, more_formatters))
    else:
        formatters = {
            'Count': '{:.4f}',
            'Share': '{:.6f}'}
        df = pd.DataFrame(d, index=base_year_df.index)
        df = format_df(df, formatters)
        df_lst.append(df)
        more_formatters = {
            'Count': '{:.4f}',
            'Share': '{:.6f}',
            'Percent_Change': '{:.6f}',
            'Share_Change': '{:.6f}'}
        for run in runs:
            df_lst.append(compare_outcome(run, s, more_formatters))

    # build up dataframe of ratios of run count variables to one another
    if len(runs) > 1:
        ratios = pd.DataFrame()
        combinations = get_combinations(runs)
        # just compare no no project right now
        s2 = divide_series((runs[0], runs[1]), variable)
        ratios[s2.name] = s2
        s2 = divide_series((runs[0], runs[2]), variable)
        ratios[s2.name] = s2
        s2 = divide_series((runs[0], runs[3]), variable)
        ratios[s2.name] = s2
    df_rt = pd.DataFrame(ratios)
    formatters = {}
    for column in df_rt.columns:
        formatters[column] = '{:.2f}'
    df_rt = format_df(df_rt, formatters)
    df_lst.append(df_rt)

    # build up summary names to the first level of the column multiindex
    keys = ['', 'BaseRun2010']
    run_column_shortnames = ['r' + str(x) + 'y40' for x in runs]
    keys.extend(run_column_shortnames)
    keys.extend(['y40Ratios'])

    df2 = pd.concat(df_lst, axis=1, keys=keys)

    write_csvs(df2, variable, runs)


def subtract_base_year_urban_footprint(run_number):
    base_year_filename = \
        'runs/run{}_urban_footprint_summary_summaries_{}.csv'.\
        format(run_number, 2010)
    bdf = pd.read_csv(base_year_filename, index_col=0)
    outcome_year_filename = \
        'runs/run{}_urban_footprint_summary_summaries_{}.csv'.\
        format(run_number, 2040)
    odf = pd.read_csv(outcome_year_filename, index_col=0)
    sdf = odf - bdf
    sdf.to_csv(
        'runs/run{}_urban_footprint_subtracted_summaries_{}.csv'
        .format(run_number, 2040))

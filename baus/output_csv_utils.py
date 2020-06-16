import pandas as pd
import numpy as np
import itertools as it

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

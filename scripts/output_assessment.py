import pandas as pd
import numpy as np
import itertools as it

VARIABLES = ['tothh', 'totemp']
RUNS = [547, 540]

# loosely borrowed from https://gist.github.com/haleemur/aac0ac216b3b9103d149


def format_df(df, formatters=None):
    formatting_columns = list(set(formatters.keys()).intersection(df.columns))
    df_copy = df[formatting_columns].copy()
    for col, formatter in formatters.items():
        try:
            df[col] = df[col].apply(lambda x: na_rep if pd.isnull(x)
                                    else formatter.format(x))
        except KeyError:
            print('{} does not exist in the dataframe.'.format(col)) +\
                'Ignoring the formatting specifier'
    return df


def get_base_year_df(geography='superdistrict'):
    geography_id = 'zone_id' if geography == 'taz' else geography
    df = pd.read_csv('data/run0_{}_summaries_2009.csv'.format(geography),
                     index_col=geography_id)
    df = df.fillna(0)
    return df


def get_outcome_df(run, geography='superdistrict', year=2040):
    geography_id = 'zone_id' if geography == 'taz' else geography
    df = pd.read_csv(
         'runs/run%(run)d_%(geography)s_summaries_%(year)d.csv' \
         % {"run": run, "year": year, "geography": geography},
         index_col=geography_id)
    df = df.fillna(0)
    return df

def write_outcome_csv(df, run, geography, year=2040):
    geography_id = 'zone_id' if geography == 'taz' else geography
    f = 'runs/run%(run)d_%(geography)s_summaries_%(year)d_acre_fix.csv' \
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


def compare_outcome(run, base_series):
    df = outcome_df(run)
    s = df[base_series.name]
    df = compare_series(base_series, s, df.index)
    formatters1 = {'Count': '{:.0f}',
                   'Share': '{:.2f}',
                   'Percent_Change': '{:.2f}',
                   'Share_Change': '{:.3f}'}

    df = format_df(df, formatters1)
    return df


def get_superdistrict_names_df():
    df = pd.read_csv('data/superdistrict_names.csv',
                     index_col='number')
    df.index.name = 'superdistrict'
    return df


def remove_characters(word, characters=b' _aeiou'):
    return word.translate(None, characters)


def make_esri_columns(df):
    df.columns = [str(x[0]) + str(x[1]) for x in df.columns]
    df.columns = [remove_characters(x) for x in df.columns]
    return df
    df.to_csv(f)


def to_esri_csv(df, variable, runs=RUNS):
    f = 'compare/esri_' +\
        '%(variable)s_%(runs)s.csv'\
        % {"variable": variable,
           "runs": '-'.join(str(x) for x in runs)}
    df = make_esri_columns(df)
    df.to_csv(f)


def write_csvs(df, variable, runs=RUNS):
    f = 'compare/' +\
        '%(variable)s_%(runs)s.csv'\
        % {"variable": variable,
           "runs": '-'.join(str(x) for x in runs)}
    df.to_csv(f)
    to_esri_csv(df, variable, runs)


def divide_series(a_tuple, variable):
    s = outcome_df(a_tuple[0])[variable]
    s1 = outcome_df(a_tuple[1])[variable]
    s2 = s / s1
    s2.name = str(a_tuple[0]) + '/' + str(a_tuple[1])
    return s2


def get_combinations(nparray):
    return pd.Series(list(it.combinations(np.unique(nparray), 2)))


def compare_outcome_for(variable):
    # empty list to build up dataframe from other dataframes
    df_lst = []
    df1 = get_superdistrict_names_df()
    df_lst.append(df1)

    s = base_year_df[variable]
    s1 = s / s.sum()
    d = {
        'Count': s,
        'Share': s1
    }
    formatters = {'Count': '{:.0f}',
                  'Share': '{:.2f}'}
    df = pd.DataFrame(d, index=base_year_df.index)
    df = format_df(df, formatters)
    df_lst.append(df)

    for run in RUNS:
        df_lst.append(compare_outcome(run, s))

    # build up dataframe of ratios of run count variables to one another
    if len(RUNS) > 1:
        ratios = pd.DataFrame()
        combinations = get_combinations(RUNS)
        for combination in combinations:
            s2 = divide_series(combination, variable)
            ratios[s2.name] = s2
    df_rt = pd.DataFrame(ratios)
    formatters = {}
    for column in df_rt.columns:
        formatters[column] = '{:.2f}'
    df_rt = format_df(df_rt, formatters)
    df_lst.append(df_rt)

    # build up summary names to the first level of the column multiindex
    keys = ['', 'r0y09']
    run_column_shortnames = ['r' + str(x) + 'y40' for x in RUNS]
    keys.extend(run_column_shortnames)
    keys.extend(['y40Ratios'])

    df2 = pd.concat(df_lst, axis=1, keys=keys)

    write_csvs(df2, variable, RUNS)


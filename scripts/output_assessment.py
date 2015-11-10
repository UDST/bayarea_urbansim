import pandas as pd
import numpy as np
import itertools as it

VARIABLES = ['tothh', 'totemp']
RUNS = [547, 540, 37]

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


def base_df(variables=VARIABLES, geography='superdistrict'):
    variables.append(geography)
    df = pd.read_csv('data/superdistrict_summaries_2009.csv',
                     index_col='superdistrict',
                     usecols=variables)
    df = df.fillna(0)
    return df


def outcome_df(run, variables=VARIABLES,
               geography='superdistrict', year=2040):
    variables.append(geography)
    df = pd.read_csv(
        'runs/run%(run)d_%(geography)s_summaries_%(year)d.csv'
        % {"run": run, "year": year, "geography": geography},
        index_col=geography,
        usecols=variables)
    df = df.fillna(0)
    return df


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
               'Share_Change', 'Ratio']
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


def sd_names():
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
    s2 = s/s1
    import pdb; pdb.set_trace
    s2.name = str(a_tuple[0]) + '/' + str(a_tuple[1])
    return s2

def get_combinations(nparray):
    return pd.Series(list(it.combinations(np.unique(nparray),2)))

def compare_outcome_for(variable):
    s = base_df[variable]
    s1 = s / s.sum()

    d = {
        'Count': s,
        'Share': s1
    }
    df_lst = []

    df1 = sd_names()
    df_lst.append(df1)

    formatters = {'Count': '{:.0f}',
                  'Share': '{:.2f}'}

    df = pd.DataFrame(d, index=base_df.index)
    df = format_df(df, formatters)

    df_lst.append(df)

    for run in RUNS:
        df_lst.append(compare_outcome(run, s))

    if len(RUNS)>1:
        ratios = []
        combinations = get_combinations(RUNS)
        for combination in combinations:
            s2 = divide_series(combination, variable)
            ratios.append(s2)
    df_rt = pd.DataFrame(ratios)

    # 'Ratio': '{:.4f}'}
    # 'Ratio': (s1 / s)

    keys = ['', 'r0y09']

    run_column_shortnames = ['r'+str(x)+'y40' for x in RUNS]

    keys.extend(run_column_shortnames)

    df2 = pd.concat(df_lst, axis=1, keys=keys)

    write_csvs(df2, variable, RUNS)

    return df_rt

base_df = base_df()
df_rt = compare_outcome_for('totemp')
compare_outcome_for('tothh')

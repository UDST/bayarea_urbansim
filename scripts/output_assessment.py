import pandas as pd
import numpy as np

# this script template is borrowed from vmt_compare.py

#PR_RUN = 547  # vmt on

BASE_RUN = 540
YEARS = [2010,2040]
COMP_RUN = 547

import pandas as pd

#from https://gist.github.com/haleemur/aac0ac216b3b9103d149
def to_csv_with_formatting(df, path_or_buf, formatters=None, **kwargs):
    """
    Writes a pandas dataframe to csv with specified formatting.
        note: I've chosen to make formatters a keyword only argument because DataFrame.to_csv method
        accepts way too many arguments for them to be legibly read later if positional arguments are 
        used. Just my opinion.
    :param df: Pandas DataFrame that is to be written to a csv file.
    :param formatters: A dictionary where (key: value) represents (column_name: new_style_formatting_string)
    :param kwargs: additional keyword arguments are passed through to the DataFrame.to_csv method.
    """
    # just call the dataframe's to_csv method if no formatters are specified
    if formatters is None:
        df.to_csv(path_or_buf, **kwargs)
        return
    
    # copy dataframe columns to be formatted to preserve their datatypes
    formatting_columns = list(set(formatters.keys()).intersection(df.columns))
    df_copy = df[formatting_columns].copy()
    
    # get the na_rep value passed to the fuction as a keyword or default to ''
    na_rep = kwargs.get('na_rep') or ''
    for col, formatter in formatters.items():
        try:
            df[col] = df[col].apply(lambda x: na_rep if pd.isnull(x) else formatter.format(x))
        except KeyError:
            print('{} does not exist in the dataframe. Ignoring the formatting specifier'.format(col))
    df.to_csv(path_or_buf, **kwargs)
    df[formatting_columns] = df_copy[formatting_columns]

def construct_panel(base_run,comparison_runs,years):
    d2={}
    all_runs = np.concatenate([[base_run],[comparison_runs]])
    for run in all_runs:
        d = {}
        for year in years:
            df = pd.read_csv(
                'runs/run%(run)d_superdistrict_summaries_%(year)d.csv' \
                % {"run": run, "year": year},
                index_col='superdistrict',
                usecols=['tothh','totemp','superdistrict'])
            df = df.fillna(0)
            d[year] = df
        p = pd.Panel.from_dict(d,orient='minor')
        d2[run]=p
    p4d = pd.Panel4D(d2)
    return p4d

def compare_variable_across_year(run,year,variable,p4d):
    d = {
        'count_10' : p4d[run,variable,:,year],
        'shr_10' : p4d[run,variable,:,year]/p4d[run,variable,:,year].sum(),
        'count_40' : p4d[run,variable,:,2040],
        'shr_40' : p4d[run,variable,:,2040]/p4d[run,variable,:,2040].sum(),
        'prcnt_chng_10_40' : p4d[run,variable,:,:].pct_change(axis=1)[2040],
        'shr_ch_40' : (p4d[run,variable,:,2040]/p4d[run,variable,:,2040].sum())\
                      / p4d[run,variable,:,year]/p4d[run,variable,:,year].sum()
    }
    df = pd.DataFrame(d,p4d.major_axis)
    return df


def compare_variable_across_run(base_run,comparison_run,variable,p4d,year=2040):
    run = comparison_run
    df = p4d[:, variable, :, year]
    d = {
        'count' : df[run],
        'shr_of_ttl' : df[run]/df[run].sum(),
        'prct_dffrnc_frm_base' : (df[run] - df[base_run])/df[base_run],
        'shr_dffrnc_frm_base' : 100*((df[run]/df[run].sum()) -
                                (df[base_run]/df[base_run].sum())),
        'div_by_base' : (df[run]/df[base_run])
    }
    df = pd.DataFrame(d, index=p4d.major_axis)
    return df

sd_names_df = pd.read_csv('data/superdistrict_names.csv',
                                   index_col='number')

def add_names(sd_names, df):
    df = pd.concat([df,sd_names], axis=1)
    return df

def pretty_run_comparison(base_run,comparison_run,sd_names,df,variable):
    formatters1 = {'count': '{:.0f}',
                  'shr_of_ttl': '{:.2f}',
                  'prct_dffrnc_frm_base': '{:.2f}',
                  'shr_dffrnc_frm_base': '{:.3f}',
                  'div_by_base': '{:.4f}'}
    f = 'compare/'+\
    '%(variable)s_in_run_%(comparison)d_to_run_%(base)d.csv'\
        % {"base": base_run, "comparison": comparison_run, "variable": variable}
    df = add_names(df,sd_names)
    to_csv_with_formatting(df, f, formatters=formatters1, index=True, na_rep='NULL')

def pretty_year_comparison(run,year_one,year_two,sd_names,df,variable):
    formatters2 = {'count_10': '{:.0f}',
                  'shr_10': '{:.2f}',
                  'count_40': '{:.0f}',
                  'shr_40': '{:.2f}',
                  'prcnt_chng_10_40': '{:.2f}',
                  'shr_ch_40': '{:.3f}'}
    f = 'compare/'+\
        '%(variable)s_in_year_%(year_one)d_to_%(year_two)d_in_run_%(run)d.csv'\
        % {"run": run, "year_one": year_one, 
           "year_two": year_two, "variable": variable}
    df = add_names(df,sd_names)
    to_csv_with_formatting(df, f, formatters=formatters2, index=True, na_rep='NULL')

p4d1 = construct_panel(BASE_RUN,COMP_RUN,YEARS)

df1 = compare_variable_across_run(BASE_RUN,COMP_RUN,'tothh',p4d1)
pretty_run_comparison(BASE_RUN,COMP_RUN,sd_names_df,df1,'tothh')

df2 = compare_variable_across_run(BASE_RUN,COMP_RUN,'totemp',p4d1)
pretty_run_comparison(BASE_RUN,COMP_RUN,sd_names_df,df2,'totemp')

df3 = compare_variable_across_year(BASE_RUN,2010,'tothh',p4d1)
pretty_year_comparison(BASE_RUN,2010,2040,sd_names_df,df3,'tothh')

df4 = compare_variable_across_year(BASE_RUN,2010,'totemp',p4d1)
pretty_year_comparison(BASE_RUN,2010,2040,sd_names_df,df4,'totemp')

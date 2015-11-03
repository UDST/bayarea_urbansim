import pandas as pd
import numpy as np

# this script template is borrowed from vmt_compare.py

#PR_RUN = 547  # vmt on

BASE_RUN = 540
YEARS = [2010,2040]
COMP_RUN = 547

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
        'prct_chng_10_40' : p4d[run,variable,:,:].pct_change(axis=1)[2040]
    }
    df = pd.DataFrame(d,p4d.major_axis)
    return df


def compare_variable_across_run(comparison_run,variable,p4d,base_run,year=2040):
    run = comparison_run
    run_string = str(run)
    cname1 = run_string + 'count'
    cname2 = run_string + '_shr_of_ttl'
    cname3 = run_string + '_prct_dffrnc_frm_base'
    cname4 = run_string + '_shr_dffrnc_frm_base'
    cname5 = run_string + '_div_by_base'

    df = p4d[:, variable, :, year]
    d = {
        cname1 : df[run],
        cname2 : df[run]/df[run].sum(),
        cname3 : (df[run] - df[base_run])/df[base_run],
        cname4 : (df[run]/df[run].sum()) -
                 (df[base_run]/df[base_run].sum()),
        cname5 : (df[run]/df[base_run])
    }
    df = pd.DataFrame(d, index=p4d.major_axis)
    return df

sd_names = pd.read_csv('data/superdistrict_names.csv',
                                   index_col='number')

def compare_across_runs(comparison_run,p4d,base_run):
    d1 = {
        'tothh' : compare_variable_across_run(comparison_run,'tothh',p4d1,base_run),
        'totemp' : compare_variable_across_run(comparison_run,'totemp',p4d1,base_run)
    }
    p1 = pd.Panel(d1)
    p1.items.name='source variable'
    p1.minor_axis.name='summary'
    df = p1.to_frame().unstack()
    return df

def compare_across_years(run,p4d):
    d2 = {
        'tothh' : compare_variable_across_year(540,2010,'tothh',p4d),
        'totemp' : compare_variable_across_year(540,2010,'totemp',p4d)
    }
    p2 = pd.Panel(d2)
    p2.items.name='source variable'
    p2.minor_axis.name='summary'
    df = p2.to_frame().unstack()
    return df 

def add_names(sd_names, df):
    df = pd.concat([df,sd_names], axis=1)
    return df

p4d1 = construct_panel(BASE_RUN,COMP_RUN,YEARS)
df1 = compare_across_years(BASE_RUN,p4d1)
df2 = compare_across_runs(COMP_RUN,p4d1,BASE_RUN)
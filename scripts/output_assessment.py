import pandas as pd
import numpy as np

# this script template is borrowed from vmt_compare.py

#PR_RUN = 547  # vmt on

BASE_RUN = 540
YEARS = [2010,2040]
comp_runs = [547]

def construct_panel(base_run,comparison_runs,years):
    d2={}
    all_runs = np.concatenate([[base_run],comparison_runs])
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

def compare_variable_across_years(run,variable,p4d):
    d = {
        'hh10' : p4d[run,variable,:,2010],
        'shr10' : p4d[run,variable,:,2010]/p4d[run,variable,:,2010].sum(),
        'hh40' : p4d[run,variable,:,2040],
        'shr40' : p4d[run,variable,:,2040]/p4d[run,variable,:,2040].sum(),
        'prct_chng_10_40' : p4d[run,variable,:,:].pct_change(axis=1)[2040]
    }
    df = pd.DataFrame(d,p4d.major_axis)
    return df



def compare_variable_across_runs(comparison_run,variable,p4d,base_run,year=2040):
    run = comparison_run
    run_string = str(run)
    cname1 = run_string + '_' + variable
    cname2 = run_string + '_shr_of_ttl'
    cname3 = run_string + '_prct_dffrnc_frm_base'
    cname4 = run_string + '_shr_dffrnc_frm_base'

    df = p4d[:, variable, :, year]
    d = {
        cname1 : df[run],
        cname2 : df[run]/df[run].sum(),
        cname3 : (df[run] - df[base_run])/df[base_run],
        cname4 : (df[run]/df[run].sum()) -
                 (df[base_run]/df[base_run].sum())
    }
    df = pd.DataFrame(d, index=p4d.major_axis)
    return df

p4d1 = construct_panel(BASE_RUN,comp_runs,YEARS)

sd_names = pd.read_csv('data/superdistrict_names.csv',
                                   index_col='number')

dct1 = {
    'tothh' : compare_variable_across_runs(547,'tothh',p4d1,BASE_RUN),
    'totemp' : compare_variable_across_runs(547,'totemp',p4d1,BASE_RUN)
}

dfA = compare_variable_across_years(540,'tothh',p4d1)
dfB = pd.concat([sd_names,dfA],axis=1)

panel_comparison = pd.Panel(dct1)

out_file1 = 'runs/scenario_comparison.csv'
out_file2 = 'runs/single_scenario_summary.csv'

panel_comparison.to_frame().unstack().to_csv(out_file1, index_label='superdistrict')

dfB.to_csv(out_file2, index_label='superdistrict')

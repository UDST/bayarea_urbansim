import pandas as pd

# this script template is borrowed from vmt_compare.py

NP_RUN = 540  # vmt off
PR_RUN = 547  # vmt on

np_df_2010 = pd.read_csv(
    'runs/run%d_superdistrict_summaries_2010.csv'
    % NP_RUN, index_col='superdistrict')
np_df_2040 = pd.read_csv(
    'runs/run%d_superdistrict_summaries_2040.csv'
    % NP_RUN, index_col='superdistrict')

superdistrict_names = pd.read_csv('data/superdistrict_names.csv',
                                  index_col='number')

########################
# Compare Outcomes w/in
# No Project Simulation
########################

hh10 = np_df_2010["tothh"]
shr10 = hh10/hh10.sum()

hh40 = np_df_2040["tothh"]
shr40 = hh40/hh40.sum()

prct_chng_10_40 = (hh40-hh10)/hh10
shr_chng_10_40 = shr40-shr10

########################
# Compare Outcomes of
# No Project & Project
# Simulation
########################

pr_df_2040 = pd.read_csv(
    'runs/run%d_superdistrict_summaries_2040.csv'
    % PR_RUN, index_col='superdistrict')

hh40_pr = pr_df_2040["tothh"]
shr40_pr = hh40_pr/hh40_pr.sum()

prct_chng_np_pr_40 = (hh40_pr-hh40)/hh40
shr_chng_np_pr_40 = shr40_pr-shr40

d = {
    'hh10': hh10,
    'shr10': shr10,
    'hh40': hh40,
    'shr40': shr40,
    'prct_chng_10_40': prct_chng_10_40,
    'shr_chng_10_40': shr_chng_10_40,
    'hh40_pr': hh40_pr,
    'shr40_pr': shr40_pr,
    'prct_chng_np_pr_40': prct_chng_np_pr_40,
    'shr_chng_np_pr_40': shr_chng_np_pr_40,
    'sd_name': superdistrict_names['name']
}

summary_df = pd.DataFrame(data=d, index=np_df_2010.index)

# pandas doesn't seem to order the columns as specified...
summary_df = summary_df.loc[:, ['sd_name', 'hh10', 'shr10', 'hh40', 'shr40',
                                'prct_chng_10_40', 'shr_chng_10_40',
                                'hh40_pr', 'shr40_pr', 'prct_chng_np_pr_40',
                                'shr_chng_np_pr_40']]

out_file = 'runs/SD_output_assessment_%(np_run)d_%(pr_run)d.csv' \
    % {"np_run": NP_RUN, "pr_run": PR_RUN}

summary_df.to_csv(out_file, index_label='superdistrict')

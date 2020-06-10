import pandas as pd
import os
import random

# this script takes multiple runs and averages them in order to reduce the
# impacts of stochasticity - this also runs at different sample sizes to
# find out how many runs we need to do in order to reduce stochasticity

run_nums = range(334, 550)
SAMPLE_SIZES = range(1, 45, 5)
NUM_SAMPLES = 2

flds = ("TOTHH,TOTEMP").split(',')


def get_2040_taz_summaries(run_nums):
    dfs = []
    for run in run_nums:
        fname = "runs/run{}_taz_summaries_2040.csv".format(run)
        if os.path.exists(fname):
            dfs.append(pd.read_csv(fname, index_col="zone_id")[flds])
    return dfs


# pass a list of dataframes of taz summaries
def average_taz_summaries(dfs):
    return reduce(lambda x, y: x + y, dfs) / len(dfs)


# select N dataframes from the list of dataframes and average them
def average_random_N_dfs(dfs, N):
    dfs = random.sample(dfs, N)
    return average_taz_summaries(dfs)


# do N samples of size M and return the stddev of the stdevs
def variability_measure(dfs, N, M):
    assert N * M < len(dfs)
    # take samples of the datafarmes and do an average
    average_dfs = [average_taz_summaries(dfs[i*M:(i+1)*M])
                   for i in range(N)]

    # stack the dataframes so that every cell is now a column
    # with rows corresponding to each sample
    average_dfs = pd.DataFrame([df.stack() for df in average_dfs])

    pct_diff = (average_dfs.iloc[0] - average_dfs.iloc[1]).abs() /\
        average_dfs.mean() * 100
    return pct_diff.unstack().fillna(0).quantile(.95)


dfs = get_2040_taz_summaries(run_nums)
print "Total sims = {}".format(len(dfs))

# need to test different sample sizes
for sample_size in SAMPLE_SIZES:
    print sample_size
    print variability_measure(dfs, NUM_SAMPLES, sample_size)

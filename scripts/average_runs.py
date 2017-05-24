import pandas as pd
import os
import random

# this script takes multiple runs and averages them in order to reduce the
# impacts of stochasticity - this also runs at different sample sizes to
# find out how many runs we need to do in order to reduce stochasticity

run_nums = range(334, 420)
SAMPLE_SIZES = range(5, 19)
NUM_SAMPLES = 3

flds = ("AGREMPN,FPSEMPN,HEREMPN,RETEMPN,MWTEMPN,OTHEMPN,HHINCQ1," +
        "HHINCQ2,HHINCQ3,HHINCQ4").split(',')


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

    # now we take a measure of the variability - we take the stddev by column
    # and then the stddev of the stddevs to boil it down to one number
    return average_dfs.std().mean()


dfs = get_2040_taz_summaries(run_nums)
print "Total sims = {}".format(len(dfs))

# need to test different sample sizes
for sample_size in SAMPLE_SIZES:
    print sample_size, variability_measure(dfs, NUM_SAMPLES, sample_size)

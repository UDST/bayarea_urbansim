import pandas as pd
import sys

args = sys.argv[1:]

if len(args) == 0:
    print "Checks growth exceeds RHNA targets for given out year"
    print "usage: <runnum> <outyear>"
    sys.exit(0)

runnum = int(args[0])
out_year = int(args[1])

controls_df = pd.read_csv("data/rhna_by_juris.csv", index_col="juris")

juris_df = pd.read_csv(
    "runs/run{}_juris_summaries_{}.csv".format(runnum, 2010),
    index_col="juris")
juris_df2 = pd.read_csv(
    "runs/run{}_juris_summaries_{}.csv".format(runnum, out_year),
    index_col="juris")

s = juris_df2.tothh - juris_df.tothh - controls_df.rhna14to22

print s[s < 0]

import pandas as pd
import os

BASE = "/var/www/html/runs/"


def readfile(year):
    fname = BASE + "run{}_parcel_output.csv".format(runnum)
    return pd.read_csv(fname, low_memory=False) \
        if os.path.isfile(fname) else pd.DataFrame()


df = pd.concat([
    readfile(runnum)
    for runnum in range(1338, 1344)
], axis=0)

df = df.query("(residential_units > 150 or job_spaces > 300) and SDEM != True")

grps = df.groupby('geom_id')

df = grps.first()
df["occurences"] = grps.size()

print len(df), df.occurences.describe()

df.to_csv("large_projects.csv")

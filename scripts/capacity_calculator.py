import pandas as pd
import orca
import sys
import argparse
from baus import models

parser = argparse.ArgumentParser(description='Run capacity calculator.')

parser.add_argument('-s', action='store', dest='scenario',
                    help='specify which scenario to run')

options = parser.parse_args()

if options.scenario:
    orca.add_injectable("scenario", options.scenario)

parcels = orca.get_table("parcels_zoning_calculations")
parcels_geography = orca.get_table("parcels_geography")

df = parcels.to_frame([
	"total_residential_units",
	"zoned_du",
    "zoned_du_underbuild",
    "zoned_du_underbuild_nodev"
])

# add the city name
df["juris_name"] = parcels_geography.juris_name

df_filt = df.query("zoned_du_underbuild_nodev > 0")
print "Number of parcels with value > 0 = %d" % len(df_filt)

df = df.groupby(["juris_name"]).sum().fillna(0).astype('int')

df.to_csv("output/city_capacity_scenario_{}.csv".format(scenario))
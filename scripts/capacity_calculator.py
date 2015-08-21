import pandas as pd
import orca
import sys
sys.path.append(".")
import models

# make sure to use the baseline zoning, even if set differently for urbansim
orca.get_injectable("settings").update(scenario="baseline")

parcels = orca.get_table("parcels")
parcels_geography = orca.get_table("parcels_geography")

df = parcels.to_frame(["total_residential_units", "zoned_du", "zoned_du_underbuild", "zoned_du_underbuild_nodev"])

print "Number of parcels with value > 0 = %d" % len(df.query("zoned_du_underbuild_nodev > 0"))

df["juris_name"] = parcels_geography.juris_name
df["juris_id"] = parcels_geography.jurisdiction

df.query("juris_name == 'Berkeley' and zoned_du_underbuild_nodev > 0").to_csv('output/berkeley_parcels.csv')

df = df.groupby(["juris_name", "juris_id"]).sum()
df["total_residential_units"] = df.total_residential_units.fillna(0).astype('int')

df.to_csv("output/city_capacity.csv")

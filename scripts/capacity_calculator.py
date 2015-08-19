import pandas as pd
import orca
import sys
sys.path.append(".")
import models

# make sure to use the baseline zoning, even if set differently for urbansim
settings = orca.get_injectable("settings")
settings["scenario"] = "baseline"
orca.add_injectable("settings", settings)

parcels = orca.get_table("parcels")
parcels_geography = orca.get_table("parcels_geography")

df = parcels.to_frame(["zoned_du", "zoned_du_underbuild"])

print "Number of parcels with value > 0 = %d" % len(df.query("zoned_du > 0"))

df = df.groupby(parcels_geography.jurisdiction).sum()

df = df.reset_index().dropna(subset=['jurisdiction'])
df['jurisdiction'] = df.jurisdiction.astype('int')

df.to_csv("output/city_capacity.csv", index=False)
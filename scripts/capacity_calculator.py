import pandas as pd
import orca
import sys
sys.path.append(".")
import models

# make sure to use the baseline zoning, even if set differently for urbansim
orca.get_injectable("settings").update(scenario="baseline")

parcels = orca.get_table("parcels")
parcels_geography = orca.get_table("parcels_geography")

df = parcels.to_frame(["zoned_du", "zoned_du_underbuild"])

print "Number of parcels with value > 0 = %d" % len(df.query("zoned_du > 0"))

df = df.groupby(parcels_geography.juris_name).sum()

df.to_csv("output/city_capacity.csv")
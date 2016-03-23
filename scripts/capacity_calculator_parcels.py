import pandas as pd
import orca
import sys
sys.path.append(".")
import models

# make sure to use the baseline zoning, even if set differently for urbansim
orca.get_injectable("settings").update(scenario="baseline")

parcels = orca.get_table("parcels_zoning_calculations")
parcels_geography = orca.get_table("parcels_geography")

df = parcels.to_frame(["geom_id", "total_residential_units", "zoned_du",
                       "zoned_du_underbuild", "zoned_du_underbuild_nodev", 
                       "effective_max_dua","effective_max_office_far",
                       # "office_allowed","retail_allowed","industrial_allowed",
                       # "cat_r"
                       # "office_high","office_medium","office_low",
                       "non_res_categories"
                       ])
df.to_csv("output/parcel_zoning_capacity.csv")
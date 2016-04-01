import sys
import orca
sys.path.append(".")
import baus.models
import pandas as pd
import numpy as np

fnames = [
    "nodev",
    "manual_nodev",
    "oldest_building_age",
    "sdem",
    "apn",
    "parcel_id",
    "geom_id",
    "total_job_spaces",
    "total_sqft",
    "total_non_residential_sqft",
    "total_residential_units",
    "juris",
    "county",
    "pda",
    "zoned_du",
    "zoned_du_underbuild",
    "parcel_size",
    "parcel_acres",
    "oldest_building",
    "first_building_type_id",
    "general_type",
    "height"
]

df = orca.get_table("parcels").to_frame(fnames)
df = df.join(orca.get_table("parcels_zoning_by_scenario").to_frame())

zoning = pd.read_csv('data/2015_10_06_zoning_parcels.csv', index_col="geom_id")
df["zoning_id"] = zoning.zoning_id.loc[df.geom_id].values

settings = orca.get_injectable("settings")

# filter buildings as urbansim does
# f = settings["feasibility"]["parcel_filter"]
# df = df.query(f)

# get building types
df["building_type"] = \
    df.first_building_type_id.map(settings["building_type_map2"])

df["oldest_building"][df.oldest_building > 2200] = np.nan

# after filter can drop a few fields
df = df.drop(["nodev", "oldest_building_age", 
    "manual_nodev", "first_building_type_id"], axis=1)

df.to_csv("parcels.csv")

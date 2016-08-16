import sys
import orca
sys.path.append(".")
import baus.models
import pandas as pd
import numpy as np

orca.add_injectable("scenario", "4")

orca.run([
    "neighborhood_vars",            # local accessibility vars
    "regional_vars",                # regional accessibility vars

    "rsh_simulate",                 # residential sales hedonic
    "nrh_simulate",                 # non-residential rent hedonic
    "price_vars"
], iter_vars=[2012])

fnames = [
    "nodev",
    "manual_nodev",
    "oldest_building_age",
    "sdem",
    "apn",
    "parcel_id",
    "geom_id",
    "total_sqft",
    "total_non_residential_sqft",
    "total_residential_units",
    "total_job_spaces",
    "juris",
    "county",
    "pda",
    "max_dua",
    "max_far",
    "parcel_size",
    "parcel_acres",
    "oldest_building",
    "first_building_type_id",
    "general_type",
    "height",
    "land_cost",
    "residential_sales_price_sqft",
    "x",
    "y"
]

df = orca.get_table("parcels").to_frame(fnames)

df["land_cost"] = df.land_cost.astype("int")

for use in ["retail", "residential", "office", "industrial"]:
    df[use+"_is_allowed"] = orca.get_injectable("parcel_is_allowed_func")(use)

settings = orca.get_injectable("settings")

# filter buildings as urbansim does
f = settings["feasibility"]["parcel_filter"]
df = df.query(f)

# get building types
df["building_type"] = \
    df.first_building_type_id.map(settings["building_type_map2"])

df["oldest_building"][df.oldest_building > 2200] = np.nan

# after filter can drop a few fields
#df = df.drop(["nodev", "oldest_building_age", 
#    "manual_nodev", "first_building_type_id"], axis=1)

df.to_csv("parcels.csv")

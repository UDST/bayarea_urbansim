import sys
import orca
from baus import models
import pandas as pd
import numpy as np

orca.add_injectable("scenario", "baseline")
orca.get_injectable("settings")["dont_build_most_dense_building"] = False

'''
orca.run([
    "neighborhood_vars",            # local accessibility vars
    "regional_vars",                # regional accessibility vars

    "rsh_simulate",                 # residential sales hedonic
    "nrh_simulate",                 # non-residential rent hedonic
    "price_vars"
], iter_vars=[2012])
'''

fnames = [
    "nodev",
    "manual_nodev",
    "oldest_building_age",
    "sdem",
    "parcel_id",
    "total_sqft",
    "first_building_type_id",
    "total_non_residential_sqft",
    "total_residential_units",
    "juris",
    "county",
    "pda",
    "vmt_res_cat",
    "vmt_nonres_cat",
    "max_dua",
    "max_far",
    "parcel_size",
    "parcel_acres",
    "oldest_building",
    "general_type",
    "x",
    "y"
]

df = orca.get_table("parcels").to_frame(fnames)

df["zoningmodcat"] = orca.get_table("parcels_geography").zoningmodcat
df["tpp"] = orca.get_table("parcels_geography").tpp_id

for use in ["retail", "residential"]:
    df[use+"_is_allowed"] = orca.get_injectable("parcel_is_allowed_func")(use)

settings = orca.get_injectable("settings")

# filter buildings as urbansim does
f = settings["feasibility"]["parcel_filter"]
df = df.query(f)

# get building types
df["building_type"] = \
    df.first_building_type_id.map({v: k for k, v in
                                  settings["building_type_map2"].items()})

df["oldest_building"][df.oldest_building > 2200] = np.nan

columns = {
    "total_sqft": "building_sqft",
    "total_residential_units": "residential_units",
    "total_non_residential_sqft": "non_residential_sqft",
    "oldest_building": "year_built"
}

# after filter can drop a few fields
df = df.drop(["first_building_type_id", "sdem", "nodev", "parcel_acres",
              "oldest_building_age", "manual_nodev"], axis=1).\
    rename(index=str, columns=columns)

df.to_csv("parcels.csv")

import orca
import pandas as pd
import numpy as np
from urbansim.utils import misc
from baus import models, utils
import itertools
import random

# this first part is the city level controls from abag
targets = pd.read_csv("data/juris_controls.csv",
                      index_col="Jurisdiction").Households

print "Sum of cities", targets.sum()

households = orca.get_table("households")
buildings = orca.get_table("buildings")
parcels = orca.get_table("parcels")

households_df = households.local

households_df["parcel_id"] = misc.reindex(buildings.parcel_id,
                                          households_df.building_id)
households_df["juris"] = misc.reindex(parcels.juris,
                                      households_df.parcel_id)
households_df["zone_id"] = misc.reindex(parcels.zone_id,
                                        households_df.parcel_id)

buildings_ids_by_juris = households_df.building_id.groupby(
    households_df.juris).unique()

household_ids_by_juris = pd.Series(households_df.index).groupby(
    households_df.juris.values).unique()

# turns out we need 4 more households
new_households = households_df.sample(4).reset_index()
# keep unique index
new_households.index += len(households_df)
households_df = households_df.append(new_households)

print "len households", len(households_df)

actual = households_df.juris.value_counts()

# now compare the two
diff = (actual - targets).sort_values(ascending=False)
print diff.describe()
print diff[diff > 0].sum()
print diff[diff < 0].sum()

overfull = diff[diff > 0]
underfull = diff[diff < 0] * -1

hh_ids = np.concatenate([
    np.random.choice(household_ids_by_juris.loc[juris],
                     size=value, replace=False)
    for juris, value in overfull.iteritems()])

print(diff - households_df.loc[hh_ids].juris.value_counts()).describe()

# unplaced
movers = households_df[households_df.building_id == -1].index

movers = pd.concat([pd.Series(hh_ids), pd.Series(movers)])


# this is a list of city names repeated the amount of
# households we're over the target
def make_city_list(overfull):

    move_hhs = sum([[x]*y for (x, y) in
                   zip(list(overfull.index), list(overfull.values))],
                   [])
    move_hhs = pd.Series(move_hhs)
    move_hhs = move_hhs.sample(len(move_hhs))  # random reorder
    return move_hhs


juris_destinations = make_city_list(underfull)
building_destinations = [
    np.random.choice(buildings_ids_by_juris.loc[d])
    for d in juris_destinations]

print len(building_destinations)

# putting the unplaced in advantageous positions
s = pd.Series(building_destinations, index=movers)
s.index.name = "household_id"

pd.DataFrame({"building_id": s}).\
    to_csv("household_building_id_overrides.csv")

print s.describe()

orig_zone_id_counts = households_df.zone_id.value_counts()

households_df.loc[s.index, "building_id"] = s.values

# go get the juris and zone_id columns again
parcels = orca.get_table("parcels")
buildings = orca.get_table("buildings")
households_df["parcel_id"] = misc.reindex(buildings.parcel_id,
                                          households_df.building_id)
households_df["juris"] = misc.reindex(parcels.juris,
                                      households_df.parcel_id)
households_df["zone_id"] = misc.reindex(parcels.zone_id,
                                        households_df.parcel_id)

diff = households_df.juris.value_counts() - targets
print diff.describe()
print diff[diff > 0].sum()
print diff[diff < 0].sum()

print(households_df.zone_id.value_counts() -
      orig_zone_id_counts).describe()

import pandas as pd
import os

max_dua = None
max_far = None

for runnum in range(1300, 1600):

    if not os.path.exists("runs/run%d_parcel_output.csv" % runnum):
        continue

    log = open("runs/run%d.log" % runnum).read()

    if "Current Scenario :  4" not in log:
        continue

    print(runnum)

    # now we know it's scenario 4
    df = pd.read_csv("runs/run%d_parcel_output.csv" % runnum).\
        set_index("parcel_id")

    dua = df.residential_units / (df.parcel_size / 43560.0)

    dua = dua.dropna()
    dua = dua[dua > 0]
    dua = dua.round()

    if max_dua is None:
        max_dua = dua
    else:
        max_dua = pd.concat([max_dua, dua], axis=1).max(axis=1)

    far = df.non_residential_sqft / df.parcel_size

    far = far.dropna()
    far = far[far > 0]
    far = far.round(1)

    if max_far is None:
        max_far = far
    else:
        max_far = pd.concat([max_far, far], axis=1).max(axis=1)

print(max_dua.describe())
print(max_far.describe())

pd.DataFrame({
    "max_built_dua": max_dua,
    "max_built_non_res_far": max_far
}).to_csv("max_building_sizes.csv")

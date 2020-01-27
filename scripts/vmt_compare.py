import pandas as pd

# this script compares the residential unit counts for two scenarios with
# vmt fees on and off - presumably we want there to be more units built
# in low-vmt areas

RUN1 = 547  # vmt on
RUN2 = 540  # vmt off
vmt_order = ["S", "M", "MH", "H", "VH"]

for runnum in [RUN1, RUN2]:
    print("VMT summary for run =", runnum)

    df = pd.read_csv('runs/run%d_parcel_output.csv' % runnum,
                     low_memory=False)

    df = df[df.form == "residential"]

    total_units = df.net_units.sum()
    print("Total units =", total_units)

    s = df.groupby("vmt_res_cat").net_units.sum().loc[vmt_order]

    print(s / total_units)

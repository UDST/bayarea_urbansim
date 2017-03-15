import pandas as pd
import sys

args = sys.argv[1:]

if len(args) != 2:
    print "Identify buildings which are not in the baserun"
    print "Usage <scenario runnum> <base runrum>"
    sys.exit()

scen = pd.read_csv("runs/run%d_parcel_output.csv" % int(args[0]),
                   low_memory=False)
base = pd.read_csv("runs/run%d_parcel_output.csv" % int(args[1]),
                   low_memory=False)

rows = []
cnt1 = cnt2 = cnt3 = 0
for ind, row in scen.iterrows():

    if row.SDEM is True:
        continue

    prevrow = base[base.parcel_id == row.parcel_id]

    if len(prevrow) == 0:

        # new parcels get added
        rows.append(row)
        cnt1 += 1

    else:

        prevrow = prevrow.iloc[0]

        def close(v1, v2, tol):
            return abs(v1-v2) < tol

        if not close(row.residential_units,
                     prevrow.residential_units, 2.5) or \
                not close(row.non_residential_sqft,
                          prevrow.non_residential_sqft, 2500):

            rows.append(row)
            cnt2 += 1

        else:
            cnt3 += 1

print "%d new developments and %d changed developments (%d unchanged)" % \
  (cnt1, cnt2, cnt3)

base = pd.DataFrame(rows).\
  to_csv("runs/run%d_parcel_output_diff.csv" % int(args[0]))

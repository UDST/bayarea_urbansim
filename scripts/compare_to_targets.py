import pandas as pd
import sys
import orca

RUNNUM = sys.argv[1]

if RUNNUM == "zoned":
    sys.path.append(".")
    import models
    parcels = orca.get_table('parcels')
    modeled = parcels.zoned_du_underbuild.groupby(parcels.pda).sum()

else:
    RUNNUM = int(RUNNUM)
    # reading modeled
    df = pd.read_csv("runs/run%d_parcel_output.csv" % RUNNUM)
    print "Total modeled units = ", df.net_units.sum()
    print "Total modeled units in all pdas = ", \
        df.dropna(subset=["pda"]).net_units.sum()
    # aggregating net units for modeled
    modeled = df.groupby("pda").net_units.sum()

targets = pd.read_csv("data/pdatargets.csv", sep="\t")
targets.index = targets.Key.str.lower()
targets = targets.Households2 - targets.Households1
# print "Warning, halving targets for 15 year simulation"

# something is wrong with the targets - they're too large
# see email with mike about this, as this is only a temp solution
print "Total target units in pdas = ", targets.sum()

ratio = (modeled / targets).reindex(targets.index).fillna(0)

print ratio.describe()

pd.DataFrame({
    "modeled": modeled,
    "targets": targets,
    "ratio": ratio
}, index=targets.index).to_csv("runs/pda_model_results.csv", index_label="pda")

import pandas as pd

RUNNUM = 41

# reading modeled
df = pd.read_csv("runs/run%d_parcel_output.csv" % RUNNUM)
print "Total modeled units = ", df.net_units.sum()
print "Total modeled units in all pdas = ", \
	df.dropna(subset=["pda"]).net_units.sum()
# aggregating net units for modeled
modeled = df.groupby("pda").net_units.sum()

targets = pd.read_csv("data/pdatargets.csv").set_index("pda").hu40pr
print "Warning, halving targets for 15 year simulation"

# something is wrong with the targets - they're too large
# see email with mike about this, as this is only a temp solution
targets = targets/2.0*.6/2.0
print "Total target units in pdas = ", targets.sum()

print modeled.head()
print targets.head()

ratio = (modeled / targets).reindex(targets.index).fillna(0)

print ratio.describe()

print ratio

pd.DataFrame({
    "modeled": modeled,
    "targets": targets,
    "ratio": ratio
}, index=targets.index).to_csv("pda_model_results.csv", index_label="pda")

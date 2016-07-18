import pandas as pd
import sys

CHECK_QUANTILE = .95

rows = []

for i in range(1532, 1552):

	df = pd.read_csv("http://urbanforecast.com/runs/run{}_targets_comparison_2040.csv".\
		format(i)).set_index("pda_fill_juris")

	cnt = tot = 0

	d = {
		"runnum": i,
		"quantile": CHECK_QUANTILE,
                "combination_factor": (i-1532)*.05
	}

	for typ in ["households", "jobs"]:
		for geo in ["juris", "pda_fill_juris"]:

			col1 = "abag_{}_{}".format(geo, typ)
			col2 = "baus_{}_{}".format(geo, typ)

			pct_diff = (df[col1] - df[col2]).abs() / df[col1]

			pct_diff = pct_diff.dropna().order()

			q = pct_diff.quantile(CHECK_QUANTILE)

			cnt += 1

			tot += q

			d[geo+"_"+typ] = q

	rows.append(d)

pd.DataFrame(rows).to_csv("output.csv")

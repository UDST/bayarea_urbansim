import pandas as pd
import sys

args = sys.argv[1:]

runnum = int(args[0])

controls_df = pd.read_csv("data/employment_controls.csv", index_col="year")

for year in range(2010, 2045, 5):

    print(year)
    taz_df = pd.read_csv("runs/run{}_taz_summaries_{}.csv".
                         format(runnum, year))
    juris_df = pd.read_csv("runs/run{}_juris_summaries_{}.csv".
                           format(runnum, year))

    for col in ["AGREMPN", "FPSEMPN", "HEREMPN", "RETEMPN",
                "TOTEMP", "MWTEMPN", "OTHEMPN"]:

        # assert taz and juris summaries are the same
        assert taz_df[col].sum() == juris_df[col.lower()].sum()

        # assert that the totals equal the control totals
        if col != "TOTEMP":
            assert taz_df[col].sum() == controls_df.loc[year, col]
        else:
            assert taz_df[col].sum() == controls_df.loc[year].sum()

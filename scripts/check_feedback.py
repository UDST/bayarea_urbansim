import pandas as pd
import sys

def check_feedback(newrunnum, baserunnum=5080):
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/1Mocmry8CcmEABiSS1otSjYSpps765bgIXA67QIUpygs/pub?gid=0&single=true&output=csv")

    baserun = pd.read_csv("runs/run{}_juris_summaries_2040.csv".format(baserunnum),
                          index_col="juris")
    newrun = pd.read_csv("runs/run{}_juris_summaries_2040.csv".format(newrunnum),
                         index_col="juris")

    expected = []
    not_expected = []

    for ind, row in df.iterrows():

        juris = row.juris

        col = {"households": "tothh", "jobs": "totemp"}[row.res_or_nonres]

        diff = newrun[col].loc[juris] - baserun[col].loc[juris]
        pct = float(diff) / baserun[col].loc[juris]
        pct = round(pct * 100, 1)

        direc = row.direction

        if direc == "high" and diff > 0:
            s = "{} have increased by {} ({}%) for {} which is NOT the expectation\n".\
                 format(row.res_or_nonres, diff, pct, juris)
            not_expected.append(s)
        if direc == "low" and diff < 0:
            s = "{} have decreased by {} ({}%) for {} which is NOT the expectation\n".\
                format(row.res_or_nonres, diff, pct, juris)
            not_expected.append(s)
        if direc == "high" and diff < 0:
            s = "{} have decreased by {} ({}%) for {} which is the expectation\n".\
                format(row.res_or_nonres, diff, pct, juris)
            expected.append(s)
        if direc == "low" and diff > 0:
            s = "{} have decreased by {} ({}%) for juris {} which isthe expectation\n".\
                format(row.res_or_nonres, diff, pct, juris)
            expected.append(s)

    buf = "\nTHESE ARE EXPECTED\n\n"
    for s in expected:
        buf += s + "\n"

    buf += "\nTHESE ARE NOT EXPECTED\n\n"
    for s in not_expected:
        buf += s + "\n"
    
    return buf

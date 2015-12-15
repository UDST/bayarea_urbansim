import pandas as pd
import numpy as np
import sys

RUN_NUMBER = int(sys.argv[1])
SIMULATION_YEAR = int(sys.argv[2])

from output_csv_utils import get_outcome_df
from output_csv_utils import get_base_year_df
from output_csv_utils import write_outcome_csv

def scaled_ciacre(base_year_df, outcome_df, acre_df):
    abgc = acre_df["ciacre10_abag"]
    mtcc = base_year_df["CIACRE"]
    us_outc = outcome_df["CIACRE"]
    totacre = outcome_df["TOTACRE"]
    sim_difference = [us_outc - mtcc][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgc + sim_difference
    overtotal = combined_acres[combined_acres > totacre]
    return (combined_acres, overtotal)


def scaled_resacre(base_year_df, outcome_df, acre_df):
    abgr = acre_df["resacre10_abag"]
    mtcr = base_year_df["RESACRE"]
    us_outr = outcome_df["RESACRE"]
    totacre = outcome_df["TOTACRE"]
    sim_difference = [us_outr - mtcr][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgr + sim_difference
    overtotal = combined_acres[combined_acres > totacre]
    return (combined_acres, overtotal)

base_year_df = get_base_year_df()
outcome_df = get_outcome_df(RUN_NUMBER, year=SIMULATION_YEAR)
acre_df = pd.read_csv('data/zone_forecast_inputs.csv',
                      index_col='zone_id',
                      usecols=['resacre10_abag', 'ciacre10_abag', 'zone_id'])
acre_df = acre_df.fillna(1)

ciacre, c_overtotal = scaled_ciacre(base_year_df, outcome_df, acre_df)
resacre, r_overtotal = scaled_resacre(base_year_df, outcome_df, acre_df)

print "acre adjustment summary checks"
print "Year:{}".format(SIMULATION_YEAR)
print "-------------"

print "abag base year c/i acres:"
print acre_df["ciacre10_abag"].sum()

print "sum of simulation c/i  acres:"
print outcome_df['CIACRE'].sum()

print "adjusted sum of c/i acres:"
print ciacre.sum()

print "abag base year res acres:"
print acre_df["resacre10_abag"].sum()

print "sum of simulation res  acres:"
print outcome_df['RESACRE'].sum()

print "adjusted sum of res acres:"
print resacre.sum()

print "TAZ TOTPOP == HHPOP+GQPOP"
print (outcome_df.TOTPOP==(outcome_df.HHPOP+outcome_df.GQPOP)).value_counts()

#adf = print outcome_df[['AGE0004', 'AGE0519', 'AGE2044', 'AGE4564', 'AGE65UP', 'TOTPOP']]

#print outcome_df[(outcome_df.TOTHH==(outcome_df.HHPOP-outcome_df.GQPOP))==False]

# assign
outcome_df['CIACRE'] = ciacre
outcome_df['RESACRE'] = resacre
outcome_df['abag_adjusted'] = 'yes'

write_outcome_csv(outcome_df, RUN_NUMBER, 'taz', year=SIMULATION_YEAR)

#################
# Optional Tools
#################

def write_outlier_csv(df, run, geography, landtype, year=SIMULATION_YEAR):
    geography_id = 'zone_id' if geography == 'taz' else geography
    f = 'runs/run%(run)d_%(geography)s_summaries_%(year)d_acre_fix_outliers_%(landtype)s.csv' \
        % {"run": run, "year": year, "geography": geography, "landtype": landtype}
    df = df.fillna(0)
    df.to_csv(f, header=True)

# write_outlier_csv(c_overtotal, RUN_NUMBER, 'taz', 'commercial', year=SIMULATION_YEAR)
# write_outlier_csv(r_overtotal, RUN_NUMBER, 'taz', 'residential', year=SIMULATION_YEAR)

def report_outliers_in_base_year_data_differences(acre_df, base_year_df):
    abgr = acre_df["resacre10_abag"]
    mtcr = base_year_df["RESACRE"]

    abgc = acre_df["ciacre10_abag"]
    mtcc = base_year_df["CIACRE"]
    us_outc = outcome_df["CIACRE"]

    r = abgr - mtcr
    c = abgc - mtcc

    # c.describe()
    # r.describe()

    c_outliers = c[c < c.quantile(.05)]
    c_outliers = c_outliers.append(c[c > c.quantile(.95)])
    r_outliers = r[r < r.quantile(.05)]
    r_outliers = r_outliers.append(r[r > r.quantile(.95)])

    r_outliers.to_csv('r_acre_outliers.csv')
    c_outliers.to_csv('c_acre_outliers.csv')

    r = (abgr - mtcr) / abgr
    c = (abgc - mtcc) / abgr

    c.describe()
    r.describe()

    c_outliers = c[c < c.quantile(.05)]
    c_outliers = c_outliers.append(c[c > c.quantile(.95)])
    r_outliers = r[r < r.quantile(.05)]
    r_outliers = r_outliers.append(r[r > r.quantile(.95)])

    r_outliers.to_csv('r_acre_outliers_prcnt.csv')
    c_outliers.to_csv('c_acre_outliers_prcnt.csv')


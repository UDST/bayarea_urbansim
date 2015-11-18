import pandas as pd
import numpy as np

# base_year_df = get_base_year_df()
# compare_outcome_for('totemp')
# compare_outcome_for('tothh')

# resacre10_abag and and ciacre10_abag are now included in zone_forecast_inputs.csv. Change the the way resacre and ciacre are calculated in the travel model output file. We want to grow the values provided by the base year ABAG numbers by the percent growth we seen in our measurements:

# 1) compute the % change in residential acres using the parcels method (ie acres of parcels with residential building on them in 2040 / same for 2010). 
# 2) compute the output variable by increasing resacre10_abag by this percentage
# 3) same for ciacre
from output_assessment import get_outcome_df
from output_assessment import get_base_year_df
from output_assessment import write_outcome_csv

base_year_df = get_base_year_df(geography='taz')
outcome_df = get_outcome_df(540,geography='taz')
acre_df = pd.read_csv('data/zone_forecast_inputs.csv',
                 index_col='zone_id',
                 usecols=['resacre10_abag','ciacre10_abag','zone_id'])
acre_df = acre_df.fillna(1)

# same as output_assessment, but fillna with 1, since using
# ratio to decide change in outcome


def scaled_ciacre(base_year_df, outcome_df, acre_df):
    abgc = acre_df["ciacre10_abag"]
    mtcc = base_year_df["CIACRE"]
    us_outc = outcome_df["CIACRE"]
    totacre = outcome_df["TOTACRE"]

    #replace 0's with 1's, because otherwise infinite values below
    abgc = abgc.replace(0,1)
    mtcc = mtcc.replace(0,1)

    prcnt_chng = (us_outc - mtcc) / mtcc
    combined_acres = abgc*(1 + prcnt_chng)

    overtotal = combined_acres[combined_acres > 2*totacre]-totacre[combined_acres > 2*totacre]
    overtotal.name = 'amnt_ovr_ttcre'

    combined_acres[combined_acres > 2*totacre] = 2*totacre

    # we do the following because some base year acreage numbers
    # are absurdly different and if we model forward on them  
    # the results are absurd. see this commit for details:
    # da895ca0d828c274f248a34d6edbb4c4868f7d7d
    # very_big_difference = ((mtcc-abgc)/abgc)>1.1
    # sim_difference = [us_outc - mtcc][0]
    # combined_acres[very_big_difference==True] = abgc + sim_difference

    # # set 83 TAZ's with negative values after the above to 0
    # combined_acres[combined_acres<0]=0
    return (combined_acres, overtotal)

def scaled_resacre(base_year_df, outcome_df, acre_df):
    abgr = acre_df["resacre10_abag"]
    mtcr = base_year_df["RESACRE"]
    us_outr = outcome_df["RESACRE"]
    totacre = outcome_df["TOTACRE"]

    #replace 0's with 1's, because otherwise infinite values below
    abgr = abgr.replace(0,1)
    mtcr = mtcr.replace(0,1)

    prcnt_chng = (us_outr - mtcr) / mtcr
    combined_acres = abgr*(1 + prcnt_chng)

    overtotal = combined_acres[combined_acres > 2*totacre]
    overtotal.name = 'amnt_ovr_ttcre'

    combined_acres[combined_acres > 2*totacre] = 2*totacre

    # we do the following because some base year acreage numbers
    # are absurdly different and if we model forward on them  
    # the results are absurd. see this commit for details:
    # da895ca0d828c274f248a34d6edbb4c4868f7d7d
    # very_big_difference = ((abgr-mtcr)/mtcr)>1.1
    # sim_difference = [us_outr - mtcr][0]
    # combined_acres[very_big_difference==True] = abgr + sim_difference

    # # set ~300 TAZ's with negative values after the above to 0
    # combined_acres[combined_acres<0]=0

    return (combined_acres, overtotal)

ciacre, c_overtotal = scaled_ciacre(base_year_df, outcome_df, acre_df)
resacre, r_overtotal = scaled_resacre(base_year_df, outcome_df, acre_df)


# print (outcome_df['RESACRE']-resacre).describe()
# print (outcome_df['CIACRE']-ciacre).describe()
# assign
outcome_df['CIACRE'] = ciacre
outcome_df['RESACRE'] = resacre

def write_outlier_csv(df, run, geography, landtype, year=2040):
    geography_id = 'zone_id' if geography == 'taz' else geography
    f = 'runs/run%(run)d_%(geography)s_summaries_%(year)d_acre_fix_outliers_%(landtype)s.csv' \
         % {"run": run, "year": year, "geography": geography, "landtype": landtype}
    df = df.fillna(0)
    df.to_csv(f,header=True)

write_outlier_csv(c_overtotal, 540, 'taz', 'commercial', year=2040)
write_outlier_csv(r_overtotal, 540, 'taz', 'residential', year=2040)
# print outcome_df.CIACRE.describe()
# print outcome_df.RESACRE.describe()

write_outcome_csv(outcome_df, 540, 'taz', year=2040)

def report_outliers_in_base_year_data_differences(acre_df,base_year_df):
    abgr = acre_df["resacre10_abag"]
    mtcr = base_year_df["RESACRE"]

    abgc = acre_df["ciacre10_abag"]
    mtcc = base_year_df["CIACRE"]
    us_outc = outcome_df["CIACRE"]


    r = abgr-mtcr
    c = abgc-mtcc

    # c.describe()
    # r.describe()

    c_outliers = c[c<c.quantile(.05)]
    c_outliers = c_outliers.append(c[c>c.quantile(.95)])
    r_outliers = r[r<r.quantile(.05)]
    r_outliers = r_outliers.append(r[r>r.quantile(.95)])

    r_outliers.to_csv('r_acre_outliers.csv')
    c_outliers.to_csv('c_acre_outliers.csv')

    r = (abgr-mtcr)/abgr
    c = (abgc-mtcc)/abgr

    c.describe()
    r.describe()

    c_outliers = c[c<c.quantile(.05)]
    c_outliers = c_outliers.append(c[c>c.quantile(.95)])
    r_outliers = r[r<r.quantile(.05)]
    r_outliers = r_outliers.append(r[r>r.quantile(.95)])

    r_outliers.to_csv('r_acre_outliers_prcnt.csv')
    c_outliers.to_csv('c_acre_outliers_prcnt.csv')

# write_outcome_csv(outcome_df, 540, 'taz', year=2040)
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
from output_assessment import write_outcome_csv

def get_base_year_df(geography='superdistrict'):
    geography_id = 'zone_id' if geography == 'taz' else geography
    df = pd.read_csv('data/run0_{}_summaries_2009.csv'.format(geography),
                     index_col=geography_id)
    df = df.fillna(1)
    return df

base_year_df = get_base_year_df(geography='taz')
outcome_df = get_outcome_df(540,geography='taz')
acre_df = pd.read_csv('data/zone_forecast_inputs.csv',
                 index_col='zone_id',
                 usecols=['resacre10_abag','ciacre10_abag','zone_id'])

# same as output_assessment, but fillna with 1, since using
# ratio to decide change in outcome


def scaled_ciacre(base_year_df, outcome_df, acre_df):
    s = base_year_df['CIACRE']
    s1 = outcome_df[s.name]
    s2 = acre_df['ciacre10_abag']
    s3 = (s1 - s) / s
    s4 = s2*(1 + s3)
    s4[s3 == np.inf]=s1[s3 == np.inf]
    return s4

def scaled_resacre(base_year_df, outcome_df, acre_df):
    s = base_year_df['RESACRE']
    s1 = outcome_df[s.name]
    s2 = acre_df['resacre10_abag']
    s3 = (s1 - s) / s
    s4 = s2*(1 + s3)
    s4[s3 == np.inf]=s1[s3 == np.inf]
    return s4

ciacre = scaled_ciacre(base_year_df, outcome_df, acre_df)
resacre = scaled_resacre(base_year_df, outcome_df, acre_df)

print (ciacre-outcome_df['CIACRE']).describe()
# count       1451.000000
# mean       -3236.698121
# std        37579.293901
# min     -1281870.699553
# 25%         -496.281697
# 50%          -68.497138
# 75%           -1.762445
# max        20182.306218

# check on the (negative) 1 million acre value


#check
(outcome_df['RESACRE']/ciacre).describe()
print (outcome_df['CIACRE']-ciacre).describe()
(outcome_df['RESACRE']/ciacre).value_counts()
print (outcome_df['CIACRE']/ciacre).value_counts()

# compare the base year data
# some ~100 taz are 0/na 
# in one or the other datasource
sr = base_year_df['RESACRE']
sr2 = acre_df['resacre10_abag']
(sr/sr2).describe()
((sr/sr2)==0).value_counts()
((sr/sr2)==np.inf).value_counts()

#assign
# outcome_df['CIACRE'] = ciacre
# outcome_df['RESACRE'] = resacre

write_outcome_csv(outcome_df, 540, 'taz', year=2040)

abgc = acre_df["ciacre10_abag"]
mtcc = base_year_df["CIACRE"]
us_outc = outcome_df["CIACRE"]

(abgc == mtcc).value_counts()
print (mtcc/abgc).describe()
print ((mtcc/abgc)==0).value_counts()
print ((mtcc/abgc)==np.inf).value_counts()


# mtcc = base_year_df['CIACRE']

# s3 = (us_outc - mtcc) / mtcc

# #this method works fine, except for cases in which mtcc is 0
# s4 = abgc*(1 + s3)

# #there are ~100 base year taz's with ciacre=0
# (mtcc==0).value_counts()

# #in the outcome year, these are their values. 
# us_outc[(mtcc==0)]

# #why fill values here? 
# s4[s3 == np.inf]=us_outc[s3 == np.inf]
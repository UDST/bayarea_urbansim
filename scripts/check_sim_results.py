# do some error checking of the results of a simulation
# this is designed to run on the parcel output that is used by urbansim_explorer

import sys
import pandas as pd
import orca
sys.path.append(".")
import models

args = sys.argv[1:]
runnum = int(args[0])

devs = pd.read_csv('runs/run%d_parcel_output.csv' % runnum, low_memory=False)

devs["building_ratio"] = devs.building_sqft / devs.total_sqft.replace(0, 1)

df = devs[devs.building_ratio < 1.5]

if len(df):

    print "Found %d devs that redev only slightly larger buildings (they change uses)" % len(df)
    #print df

    # write out some more detailed info on these buildings
    '''
    b = orca.get_table('buildings').local
    df2 = b[b.parcel_id.isin(df.parcel_id)]
    print df2.building_type_id.value_counts()
    df2.to_csv('out.csv')
    df[df.parcel_id.isin(df2.parcel_id)].to_csv('out2.csv')
    '''

devs["parcel_id_counts"] = devs.parcel_id.value_counts().loc[devs.parcel_id].values

df = devs.query("parcel_id_counts > 1 and SDEM == False")

if len(df):

    print "Found %d devs that redev a parcel that's already built on" % len(df)
    print df


jdf10 = pd.read_csv('runs/run%d_juris_summaries_2010.csv' % runnum, index_col='juris')
jdf40 = pd.read_csv('runs/run%d_juris_summaries_2040.csv' % runnum, index_col='juris')

empdiff = jdf40.loc['San Francisco'].totemp - jdf10.loc['San Francisco'].totemp
pctdiff = float(empdiff) / jdf10.loc['San Francisco'].totemp * 100
print "San Francisco employment goes up by %d jobs (%.1f pct)" % (empdiff, pctdiff)

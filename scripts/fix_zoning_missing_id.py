import pandas as pd
import orca
import sys
sys.path.append(".")
import datasources

zb = orca.get_table("zoning_baseline")
zl = orca.get_table("zoning_lookup")
z = orca.get_table("zcsv")

zdf = z.to_frame()

null_df = zdf.loc[zdf.zoning_id.isnull(),:]
print "there are " + str(len(null_df.index)) + " empty zoning ids"
print "number of parcels with null values by city:"
print null_df.tablename.value_counts()

zl_df = zl.to_frame()

zlcn = orca.get_table("zoning_table_city_lookup")
zlcndf = zlcn.to_frame()
zl_df['zoning_lookup_table_id'] = zl_df.index
#zlcndf['zoning_lookup_table_id'] = zlcndf.index
zldf_tbl_nm = pd.merge(zl_df,zlcndf,how='left',left_on='city',right_on='city_name')
zl_df = zldf_tbl_nm

null_df['geom_id'] = null_df.index
mdf = pd.merge(null_df,zl_df,how='inner', right_on=['name','tablename'], left_on=['zoning','tablename'])
mdf = mdf.set_index(mdf.geom_id)

print "replaced " + str(len(mdf.index)) + " empty zoning ids"
zdf.loc[mdf.index,'zoning_id'] = mdf['zoning_lookup_table_id']

null_df = zdf.loc[zdf.zoning_id.isnull(),:]
print "there are " + str(len(null_df.index)) + " empty zoning ids"

print "number of parcels with null values by city:"
print null_df.tablename.value_counts()

zdf.to_csv('data/2015_12_15_zoning_parcels.csv')


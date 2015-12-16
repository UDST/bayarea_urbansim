import pandas as pd
import datasources
import orca

zb = orca.get_table("zoning_baseline")
zl = orca.get_table("zoning_lookup")
z = orca.get_table("zcsv")

# zid_s = zb['zoning_id']
# znd_s = zb['nodev']

# z_name = zb['zoning']
# z_geom_id = zb['geom_id']

# z_name == 'RS 7'

# zdf.ix[5986988702188]

# zbdf = zb.to_frame()

# bk_zoning = zbdf[zbdf.tablename=='berkeleyzoning']

# bk_zoning[bk_zoning.zoning=='C-DMU Core']

zdf = z.to_frame()
bk_zoning_rawcsv = zdf[zdf.tablename=='berkeleyzoning']
bk_zoning_rawcsv[bk_zoning_rawcsv.zoning=='C-DMU Core']

null_df = bk_zoning_rawcsv.loc[bk_zoning_rawcsv.zoning_id.isnull(),:]

zldf = zl.to_frame()

zlcn = orca.get_table("zoning_table_city_lookup")
zlcndf = zlcn.to_frame()
zldf_tbl_nm = pd.merge(zldf,zlcndf,how='left',left_on='city',right_on='city_name')
zl_df = zldf_tbl_nm

zl_df['zoning_lookup_table_id'] = zl_df.index
null_df['geom_id'] = null_df.index
mdf = pd.merge(null_df,zldf_tbl_nm,how='inner', right_on=['name','tablename'], left_on=['zoning','tablename'])
mdf = mdf.set_index(mdf.geom_id)

bk_zoning_rawcsv.loc[mdf.index,'zoning_id'] = mdf['zoning_lookup_table_id']


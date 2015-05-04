import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex import TableLoader
from spandex.io import exec_sql,  df_to_db

#Connect to the database
loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

def read_baydata_csv(relative_filepath):
    zoning_xref_dir = loader.get_path(relative_filepath)
    return pd.read_csv(zoning_xref_dir)

# Reading zoning csv's
zoning_parcel_xref = read_baydata_csv('juris/loc/zoning/zoning_parcels.csv')
zoning = read_baydata_csv('juris/loc/zoning/zoning_id_lookup_table.csv')

# Getting parcels
parcels = db_to_df('select * from parcel;')
parcels = parcels[['parcel_id', 'geom_id']]

# Processing the xref
zoning_parcel_xref = zoning_parcel_xref.set_index('geom_id')
zoning_parcel_xref = pd.merge(zoning_parcel_xref, parcels, left_index = True, right_on = 'geom_id')
zoning_parcel_xref.index.name = 'idx'
df_to_db(zoning_parcel_xref, 'zoning_parcel_xref', schema=loader.tables.public)

# Zoning table to db
zoning = zoning.rename(columns = {'id':'zoning_id'})
zoning = zoning.set_index('zoning_id')
df_to_db(zoning, 'zoning', schema=loader.tables.public)

# Update zoning_id on the database
exec_sql("update parcel set zoning_id = a.zoning_id from zoning_parcel_xref a where parcel.geom_id = a.geom_id;")


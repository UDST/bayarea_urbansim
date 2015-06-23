import pandas as pd
from spandex import TableLoader
import pandas.io.sql as sql

loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

## Export to HDF5-  get path to output file
h5_path = loader.get_path('out/regeneration/summaries/bayarea_v3.h5')  ## Path to the output file

#Buildings
buildings = db_to_df('select * from building').set_index('building_id')
if 'id' in buildings.columns:
    del buildings['id']
buildings['building_type_id'] = 0
buildings.building_type_id[buildings.development_type_id == 1] = 1
buildings.building_type_id[buildings.development_type_id == 2] = 3
buildings.building_type_id[buildings.development_type_id == 5] = 12
buildings.building_type_id[buildings.development_type_id == 7] = 10
buildings.building_type_id[buildings.development_type_id == 9] = 5
buildings.building_type_id[buildings.development_type_id == 10] = 4
buildings.building_type_id[buildings.development_type_id == 13] = 8
buildings.building_type_id[buildings.development_type_id == 14] = 7
buildings.building_type_id[buildings.development_type_id == 15] = 9
buildings.building_type_id[buildings.development_type_id == 13] = 8
buildings.building_type_id[buildings.development_type_id == 17] = 6
buildings.building_type_id[buildings.development_type_id == 24] = 16

#Parcels
parcels = db_to_df('select * from parcel').set_index('parcel_id')
parcels['shape_area'] = parcels.acres * 4046.86
if 'id' in parcels.columns:
    del parcels['id']
if 'geom' in parcels.columns:
    del parcels['geom']
if 'centroid' in parcels.columns:
    del parcels['centroid']

#Jobs
jobs = db_to_df('select * from jobs').set_index('job_id')
if 'id' in jobs.columns:
    del jobs['id']

#Households
hh = db_to_df('select * from households').set_index('household_id')
if 'id' in hh.columns:
    del hh['id']
hh = hh.rename(columns = {'hinc':'income'})
for col in hh.columns:
    hh[col] = hh[col].astype('int32')
    
#Zones
zones_path = loader.get_path('juris/reg/zones/zones.csv')
zones = pd.read_csv(zones_path).set_index('zone_id')

#Putting tables in the HDF5 file
store = pd.HDFStore(h5_path)
store['parcels'] = parcels # http://urbansim.org/Documentation/Parcel/ParcelTable
store['buildings'] = buildings # http://urbansim.org/Documentation/Parcel/BuildingsTable
store['households'] = hh # http://urbansim.org/Documentation/Parcel/HouseholdsTable
store['jobs'] = jobs # http://urbansim.org/Documentation/Parcel/JobsTable
store['zones'] = zones # http://urbansim.org/Documentation/Parcel/ZonesTable
store.close()
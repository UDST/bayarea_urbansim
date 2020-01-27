import pandas as pd

parcels = pd.read_hdf('./bayarea_urbansim/data/2015_09_01_bayarea_v3.h5',
                      key='parcels', mode='r')
parcels.head()

tract_zone_xwalk = pd.read_csv('./tract2000_zone_sd.csv')
tract_zone_xwalk.head()

tracts = []
for i in parcels.index:
    zone = parcels['zone_id'][i]
    tract = tract_zone_xwalk.query('rtaz1==@zone')["tract"].item()
    tracts.append(tract)

parcels_clp = parcels[['zone_id']]
parcels_clp['census_tract'] = tracts
parcels_clp

parcels_clp.to_csv('parcel_tract_xwalk.csv')

# checks
print((len(parcels_clp['census_tract'].unique())))
print((len(parcels_clp.index)))

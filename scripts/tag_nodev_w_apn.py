import pandas as pd
import geopandas as gpd
from baus import utils, datasources
from shapely import wkt
from shapely.geometry import Point


def read_geocsv(*args, **kwargs):
    df = pd.read_csv(*args, **kwargs)
    df.loc[df.geometry.notnull(), 'geometry'] = \
        [wkt.loads(s) for s in df.loc[df.geometry.notnull(), 'geometry']]
    gdf = gpd.GeoDataFrame(df)
    gdf.crs = {'init': 'epsg:4326'}
    return gdf
gpd.read_geocsv = read_geocsv

nodev = pd.read_csv('data/nodev_locations.csv')

geometry = [Point(xy) for xy in zip(nodev.x, nodev.y)]
crs = {'init': 'epsg:4326'}
nodev = gpd.GeoDataFrame(nodev, crs=crs, geometry=geometry)

parcels = gpd.read_geocsv('data/parcels_geography.csv')

pcl_second_match = parcels[['x', 'y', 'apn']].set_index('apn')

parcels = parcels[['geometry', 'apn']]

parcels = parcels[parcels.is_valid]

nodev = nodev[nodev.is_valid]

nodev_apn = gpd.sjoin(
    nodev, parcels, how="left", op="intersects")

nodev_apn.loc[nodev_apn.apn.isnull(),
              'apn'] = nearest_neighbor(pcl_second_match[['x', 'y']],
                                        nodev_apn.loc[nodev_apn.apn.isnull(),
                                                      ['x', 'y']])

del nodev_apn['geometry']
del nodev_apn['index_right']

nodev_apn.to_csv('data/nodev_locations.csv', index=False)

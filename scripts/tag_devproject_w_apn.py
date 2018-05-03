import pandas as pd
import geopandas as gpd
from baus import utils
import orca
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

parcels = gpd.read_geocsv('data/parcels_geography.csv')

pcl_second_match = parcels[['x', 'y', 'apn']].set_index('apn')

parcels = parcels[['geometry', 'apn']]

parcels = parcels[parcels.is_valid]

dev = pd.read_csv('data/development_projects.csv')

geometry = [Point(xy) for xy in zip(dev.x, dev.y)]
crs = {'init': 'epsg:4326'}
dev = gpd.GeoDataFrame(dev, crs=crs, geometry=geometry)

dev = dev[dev.is_valid]

dev_apn = gpd.sjoin(
    dev, parcels, how="left", op="intersects")

dev_unmatched = dev_apn.loc[dev_apn.apn.isnull(), ['x', 'y']]

dev_apn.loc[dev_apn.apn.isnull(),
            'apn'] = utils.nearest_neighbor(pcl_second_match[['x', 'y']],
                                            dev_apn.loc[dev_apn.apn.isnull(),
                                            ['x', 'y']])

del dev_apn['geometry']
del dev_apn['index_right']

dev_apn.to_csv('data/development_projects.csv', index=False)

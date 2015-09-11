import pandas as pd
import geopandas as gpd

df = pd.read_csv('/var/www/html/scratchpad/bayarea_softsites.csv')
df.set_index('geom_id', inplace=True)

gdf = gpd.GeoDataFrame.from_file('/home/ubuntu/data/sfr.shp')

gdf.set_index('GEOM_ID', inplace=True)
gdf = gdf.to_crs(epsg=4326)

for col in df.columns:
    gdf[col] = df[col]

gdf = gdf[gdf.zoned_du > 0]

print len(gdf)

gdf = gdf.reset_index()
gdf["GEOM_ID"] = gdf.GEOM_ID.astype('int')

open('out.json', 'w').write(gdf.to_json())


# gdf[gdf.COUNTY_ID == 75].to_file('/home/ubuntu/data/sfr.shp')

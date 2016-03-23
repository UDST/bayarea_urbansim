
# phase one just gets rid of a bunch of extra field names and writes to geojson
'''
import geopandas as gpd
from fiona.crs import from_epsg
gdf = gpd.GeoDataFrame.from_file('/home/ubuntu/data/avgload5period.shp')
gdf.crs = from_epsg(3740)
gdf = gdf.to_crs(epsg=4326)

gdf = gdf[["A", "B", "CTIMAM", "CTIMEA", "CTIMEV", "CTIMMD",
           "CTIMPM", "geometry"]]

print gdf.columns

open("out.json", "w").write(gdf.to_json())
'''

# phase 2 reads in the json as json and converts to the expected hdf format
# docs for format http://udst.github.io/pandana/tutorial.html
import pandas as pd
import json

store = pd.HDFStore('tmnet.h5')

nodes = {}
edges = []

for feature in json.load(open('out.json'))['features']:

    edge = feature["properties"]
    edge["from"] = edge["A"]
    edge["to"] = edge["B"]
    del edge["A"]
    del edge["B"]
    edges.append(edge)

    p1 = feature["geometry"]["coordinates"][0]
    p2 = feature["geometry"]["coordinates"][1]

    nodes[edge["from"]] = {"x": p1[0], "y": p1[1]}
    nodes[edge["to"]] = {"x": p2[0], "y": p2[1]}

store["nodes"] = pd.DataFrame(nodes.values(), index=nodes.keys())
store["edges"] = pd.DataFrame(edges)

print store["nodes"].describe()
print store["nodes"].index
print store["edges"].describe()

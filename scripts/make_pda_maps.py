import folium
import pandas as pd

htmldir = "/var/www/html/scratchpad/"

pda_data = pd.read_csv('data/pdatargets.csv')
pda_data["pda"] = pda_data.pda.str.upper()

map = folium.Map(location=[37.7792, -122.1191], 
                 zoom_start=10,
                 tiles='Stamen Toner')

map.geo_json(geo_path='pdas.json', 
             data=pda_data, 
             data_out='data.json',
             columns=['pda', 'hu40pr'],
             key_on='feature.properties.id_1',
             fill_color='YlGn', 
             fill_opacity=0.7, 
             line_opacity=1.0,
             legend_name='Target Unit Growth')

map.create_map(path=htmldir+'targets.html')

# this is weird how folium does this
import os
os.rename("data.json", htmldir+"data.json")

map = folium.Map(location=[37.7792, -122.1191], 
                 zoom_start=10,
                 tiles='Stamen Toner')

results = pd.read_csv('pda_model_results.csv').set_index("pda")

import geopandas
gdf = geopandas.GeoDataFrame.from_file(htmldir+"pdas.json").set_index("id_1")
centroids = gdf.centroid

for pda, centroid in centroids.iteritems():
    try:
        result = results.loc[pda.lower()]
        text = str(result).replace("\n", "<br>")
        ratio = result.ratio
    except:
        print "PDA not found", pda
        text = "Not found"
        ratio = 0
    radius = max(result.targets / 4.0, 200)
    if ratio > .5 and ratio < 2.0: color = 'green'
    elif ratio <= .5: color = 'yellow'
    else: color = 'red'
    map.circle_marker(location=[centroid.y, centroid.x], radius=radius,
                  popup=text, line_color='black',
                  fill_color=color, fill_opacity=0.6)

map.create_map(path=htmldir+'results.html')

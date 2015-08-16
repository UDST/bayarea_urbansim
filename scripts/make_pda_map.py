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

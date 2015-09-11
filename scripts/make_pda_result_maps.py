import folium
import pandas as pd
import geopandas
import math
import sys

args = sys.argv[1:]
suffix = ''
if len(args) > 0:
    suffix = "_"+args[0]

htmldir = "/var/www/html/scratchpad/"

gdf = geopandas.GeoDataFrame.from_file(htmldir+"pdas.json").set_index("id_1")
centroids = gdf.centroid

results = pd.read_csv('runs/pda_model_results.csv').set_index("pda")


def make_map(outname, map_funcs):
    map = folium.Map(location=[37.7792, -122.1191],
                     zoom_start=10,
                     tiles='Stamen Toner')

    pda_d = {}
    for pda, centroid in centroids.iteritems():

        if pda in pda_d:
            # for some reason pdas occur multiple times in the shapefile
            continue
        pda_d[pda] = 1

        result = None
        try:
            result = results.loc[pda.lower()]
        except:
            print "PDA not found", pda

        text = map_funcs['text'](result)
        color = map_funcs['color'](result)
        radius = map_funcs['radius'](result)

        map.circle_marker(
            location=[centroid.y, centroid.x], radius=radius,
            popup=text, line_color='black',
            fill_color=color, fill_opacity=0.6)

    map.create_map(path=outname)


def get_text(r):
    return str(r).replace("\n", "<br>") \
        if r is not None else ''


def get_radius(r):
    return max(math.sqrt(r.targets)*10, 200) \
        if r is not None else 0


def get_color(r):
    ratio = r.ratio if r is not None else 0
    if ratio > .5 and ratio < 2.0:
        color = 'green'
    if 2.0 < ratio < 4.0:
        color = 'yellow'
    if ratio > 4.0:
        color = 'red'
    if .25 < ratio < .5:
        color = 'blue'
    if ratio < .25:
        color = 'purple'
    return color

outname = htmldir+'results%s.html' % suffix
map_funcs = {
    'text': get_text,
    'radius': get_radius,
    'color': get_color
}
make_map(outname, map_funcs)


def get_radius(r):
    return max(math.sqrt(r.fillna(0).modeled)*10, 200) \
        if r is not None else 0

outname = htmldir+'results_flipped%s.html' % suffix
map_funcs['radius'] = get_radius
make_map(outname, map_funcs)

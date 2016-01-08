from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import geopandas
import json
import time
import fiona
from string import join
from shapely.geometry import shape

MONGO = True
JURIS = None
FEASIBILITY = True

cid = "ZC7yyAyA8jkDFnRtf" # parcels
csvname = "output/parcels.csv"

if FEASIBILITY:
    cid = "hMm5FqbDCPa4ube6Y" # feasibility
    csvname = "output/feasibility.csv"

if MONGO:
    client = MongoClient()
    #client.drop_database("baus")
    db = client.togethermap
else:
    outf = open("parcels.json", "w")

df = pd.read_csv(csvname, index_col="geom_id")

cnt = 0
features = []

print time.ctime()


def add_bbox(p):
    bounds = shape(p['geometry']).bounds
    minx, miny, maxx, maxy = bounds
    poly = {
        "type": "Polygon",
        "coordinates": [
            [ [minx, miny], [minx, maxy], [maxx, maxy],
              [maxx, miny], [minx, miny] ]
        ]
    }
    p['bbox'] = poly
    return p


def export_features(features):
    global MONGO, db, outf
    if MONGO:
         db.places.insert_many(features)
    else:
         outf.write(join([json.dumps(f) for f in features], "\n"))


with fiona.drivers():
    with fiona.open('/home/ubuntu/data/parcels4326.shp') as shp:
        
        for f in shp:

             cnt += 1
             if cnt % 10000 == 0:
                 print "Done reading rec %d" % cnt

             if len(features) == 10000:

                 print "Exporting 10k recs"
                 export_features(features)
                 print "Done exporting 10k recs"

                 features = []

             geom_id = int(f["properties"]["GEOM_ID"])
             try:
                 rec = df.loc[geom_id]
             except:
                 # don't need to keep it, it's not in parcels.csv
                 continue
    
             if JURIS and rec["juris"] != JURIS:
                 continue

             f["properties"] = rec.to_dict()
             f["properties"]["geom_id"] = geom_id
             del f["id"]

             f["creatorUID"] = "ceTir2NKMN87Gq7wj"
             f["creator"] = "Fletcher Foti"
             f["createDate"] = "2015-08-29T05:10:00.446Z"
             f["updateDate"] = "2015-08-29T05:10:00.446Z"
             f["collectionId"] = cid
             f['_id'] = str(ObjectId())
             f["post_count"] = 0

             f = add_bbox(f)

             features.append(f)
                 
if len(features):
    export_features(features)

print time.ctime()

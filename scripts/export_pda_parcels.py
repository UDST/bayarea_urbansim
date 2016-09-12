from pymongo import MongoClient
import sys
import json
from bson import json_util
import pandas as pd

parcels = pd.read_csv("parcels.csv").dropna(subset=["pda"]).fillna("na")

parcels["nodev"] = parcels.nodev.astype('int')

# use as index and as a column
parcels.index = parcels.parcel_id

pdas = parcels.pda

db = MongoClient('localhost', 27017).togethermap

cid = "ZC7yyAyA8jkDFnRtf"

def fix(p):
    # add current data from csv file and just keep shapes
    pid = p["properties"]["parcel_id"]
    for k in p.keys():
        if k not in ["geometry", "type"]:
            del p[k]
    p["properties"] = parcels.loc[pid].to_dict()
    return p

for pda in pdas.unique():

    print "Exporting: ", pda

    with open("output/%s.json" % pda, "w") as f:

        pids = parcels[pdas == pda].parcel_id

        features = [fix(p) for p in db['places'].find(
            {
                "collectionId": cid,
                "properties.parcel_id": {"$in": pids.tolist()}
            }
        )]

        print "Writing %d features" % len(features)
        f.write(json.dumps({
            "type": "FeatureCollection",
            "features": features
        }, default=json_util.default))

import sys
import json
from flask import Flask, jsonify
from flask.ext.cors import CORS
import pandas as pd
import orca

sys.path.append(".")
import models

app = Flask(__name__)
# app.debug = True
cors = CORS(app)

MAX_PARCELS_RETURNED = 5000

print "Loading"

store = pd.HDFStore('data/bayarea_v3.h5')

flds = ['parcel_id', 'parcel_acres', 'total_residential_units',
        'total_job_spaces', 'x', 'y', 'pda']
parcels = orca.get_table('parcels').to_frame(flds)
flds = ['building_id', 'residential_units', 'job_spaces',
        'building_type_id', 'parcel_id', 'building_sqft']
buildings = orca.get_table('buildings').to_frame(flds)
# households = orca.get_table('households').to_frame()
# jobs = orca.get_table('jobs').to_frame()
flds = ['parcel_id', 'max_dua', 'max_far', 'max_height', 'type1',
        'type2', 'type3', 'type4']
zoning = orca.get_table('zoning_baseline').to_frame(flds)

print "Ready"


@app.route('/extract/<query>')
def get_data(query):

    print "Got query:", query

    # global parcels, buildings, households, jobs, zoning_baseline
    global parcels, buildings, zoning_baseline

    p = parcels.reset_index().query(query).set_index('parcel_id')

    print "Len parcels:", len(p)

    if len(p) > MAX_PARCELS_RETURNED:
        return jsonify(**{
            "status": "Error: can only ask for %d parcels" %
                       MAX_PARCELS_RETURNED
                       })

    d = {}
    d['parcels'] = json.loads(p.to_json(orient='index'))

    z = zoning[zoning.index.isin(p.index)]
    d['zoning'] = json.loads(z.to_json(orient='index'))

    b = buildings[buildings.parcel_id.isin(p.index)]
    d['buildings'] = json.loads(b.to_json(orient='index'))

    # h = households[households.building_id.isin(b.index)]
    # d['households'] = json.loads(h.to_json(orient='index'))

    # j = jobs[jobs.building_id.isin(b.index)]
    # d['jobs'] = json.loads(j.to_json(orient='index'))

    return jsonify(**{
        "status": "Success",
        "data": d
    })


if __name__ == '__main__':

    # from tornado.wsgi import WSGIContainer
    # from tornado.httpserver import HTTPServer
    # from tornado.ioloop import IOLoop

    # http_server = HTTPServer(WSGIContainer(app))
    # http_server.listen(5000)
    # IOLoop.instance().start()
    app.run('0.0.0.0', 1984)

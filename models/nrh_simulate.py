from __future__ import print_function

import copy
import os
import sys
import time

import numpy as np
import pandas as pd
import statsmodels.api as sm
from patsy import dmatrix

from urbansim.urbanchoice import interaction
from urbansim.urbanchoice import mnl
from urbansim.urbanchoice import nl
from urbansim.utils import misc

def nrh_simulate(dset, year=None, show=True):
    assert "hedonicmodel" == "hedonicmodel"  # should match!
    returnobj = {}
    t1 = time.time()

    # TEMPLATE configure table
    buildings = dset.building_filter(residential=0)
    # ENDTEMPLATE

    # TEMPLATE merge
    t_m = time.time()
    buildings = pd.merge(buildings, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying in %f seconds" % (time.time() - t1))
    t1 = time.time()

    simrents = []
    # TEMPLATE creating segments
    segments = buildings.groupby(['general_type'])
    # ENDTEMPLATE
    
    for name, segment in segments:
        name = str(name)
        outname = "nrh" if name is None else "nrh_" + name

        # TEMPLATE computing vars
        print("WARNING: using patsy, ind_vars will be ignored")
        est_data = dmatrix("I(year_built < 1940) + I(year_built > 2005) + np.log1p(stories) + ave_income + poor + jobs + sfdu + renters", data=segment, return_type='dataframe')
        # ENDTEMPLATE
        print("Generating rents on %d %s" %
            (est_data.shape[0], "buildings"))
        vec = dset.load_coeff(outname)
        vec = np.reshape(vec,(vec.size,1))
        rents = est_data.dot(vec).astype('f4')
        rents = rents.apply(np.exp)
        simrents.append(rents[rents.columns[0]])
        returnobj[name] = misc.pandassummarytojson(rents.describe())
        rents.describe().to_csv(
            os.path.join(misc.output_dir(), "nrh_simulate.csv"))
        
    simrents = pd.concat(simrents)

    dset.buildings["nonresidential_rent"] = simrents.reindex(dset.buildings.index)
    dset.store_attr("nonresidential_rent", year, simrents)

    print("Finished executing in %f seconds" % (time.time() - t1))
    return returnobj
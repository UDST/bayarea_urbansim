from __future__ import print_function

import copy
import os
import string
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

def rrh_simulate(dset, year=None, show=True):
    assert "hedonicmodel" == "hedonicmodel"  # should match!
    returnobj = {}
    t1 = time.time()

    # TEMPLATE configure table
    units = dset.building_filter(residential=1)
    # ENDTEMPLATE

    # TEMPLATE merge
    t_m = time.time()
    units = pd.merge(units, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying in %f seconds" % (time.time() - t1))
    t1 = time.time()

    simrents = []
    segments = [(None, units)]
    
    for name, segment in segments:
        name = str(name)
        outname = "rrh" if name is None else "rrh_" + name

        # TEMPLATE computing vars
        print("WARNING: using patsy, ind_vars will be ignored")
        patsy = "np.log1p(unit_sqft) + sum_residential_units + ave_unit_sqft + ave_lot_sqft + ave_income + poor + jobs + sfdu + renters"
        est_data = dmatrix(patsy, data=segment, return_type='dataframe')
        # ENDTEMPLATE
        print("Generating rents on %d %s" %
            (est_data.shape[0], "units"))
        vec = dset.load_coeff(outname)
        vec = np.reshape(vec,(vec.size,1))
        rents = est_data.dot(vec).astype('f4')
        rents = rents.apply(np.exp)
        simrents.append(rents[rents.columns[0]])
        returnobj[name] = misc.pandassummarytojson(rents.describe())
        rents.describe().to_csv(
            os.path.join(misc.output_dir(), "rrh_simulate.csv"))
        
    simrents = pd.concat(simrents)

    dset.buildings["res_rent"] = simrents.reindex(dset.buildings.index)
    dset.store_attr("res_rent", year, simrents)

    print("Finished executing in %f seconds" % (time.time() - t1))
    return returnobj
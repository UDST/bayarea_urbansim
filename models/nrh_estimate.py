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

def nrh_estimate(dset, year=None, show=True):
    assert "hedonicmodel" == "hedonicmodel"  # should match!
    returnobj = {}
    t1 = time.time()

    # TEMPLATE configure table
    buildings = dset.costar
    # ENDTEMPLATE

    # TEMPLATE merge
    t_m = time.time()
    buildings = pd.merge(buildings, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying in %f seconds" % (time.time() - t1))
    t1 = time.time()

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
        # TEMPLATE dependent variable
        depvar = segment["averageweightedrent"]
        depvar = depvar.apply(np.log)
        # ENDTEMPLATE
        
        if name:
            print("Estimating hedonic for %s with %d observations" %
                (name, len(segment.index)))
        if show:
            print(est_data.describe())
        dset.save_tmptbl("EST_DATA", est_data)  # in case we want to explore via urbansimd

        model = sm.OLS(depvar, est_data)
        results = model.fit()
        if show:
            print(results.summary())

        misc.resultstocsv(
            (results.rsquared, results.rsquared_adj),
            est_data.columns,
            zip(results.params, results.bse, results.tvalues),
            outname + "_estimate.csv", hedonic=1, tblname=outname)

        d = {}
        d['rsquared'] = results.rsquared
        d['rsquared_adj'] = results.rsquared_adj
        d['columns'] =  est_data.columns.tolist()
        d['est_results'] =  zip(results.params, results.bse, results.tvalues)
        returnobj[name] = d

        dset.store_coeff(outname, results.params.values, results.params.index)
        
    print("Finished executing in %f seconds" % (time.time() - t1))
    return returnobj
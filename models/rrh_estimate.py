from __future__ import print_function

import copy
import os
import sys
import time

import numpy as np
import pandas as pd
import statsmodels.api as sm
from patsy import dmatrix

from urbansim.urbanchoice import *
from urbansim.utils import misc

def rrh_estimate(dset, year=None, show=True):
    assert "hedonicmodel" == "hedonicmodel"  # should match!
    returnobj = {}
    t1 = time.time()

    # TEMPLATE configure table
    units = dset.apartments
    # ENDTEMPLATE

    # TEMPLATE merge
    t_m = time.time()
    units = pd.merge(units, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying in %f seconds" % (time.time() - t1))
    t1 = time.time()

    segments = [(None, units)]
    
    for name, segment in segments:
        name = str(name)
        outname = "rrh" if name is None else "rrh_" + name

        # TEMPLATE computing vars
        est_data = dmatrix("np.log1p(unit_sqft) + sum_residential_units + ave_unit_sqft + ave_lot_sqft + ave_income + poor + jobs + sfdu + renters", data=segment, return_type='dataframe')
        # ENDTEMPLATE
        # TEMPLATE dependent variable
        depvar = segment["rent"]
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
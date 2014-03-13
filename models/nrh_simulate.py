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

def nrh_simulate(dset, year=None, show=True):
    assert "hedonicmodel" == "hedonicmodel"  # should match!
    returnobj = {}
    t1 = time.time()

    # TEMPLATE configure table
    jobs = dset.building_filter(residential=0)
    # ENDTEMPLATE

    # TEMPLATE merget_m = time.time()
    jobs = pd.merge(jobs, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying in %f seconds" % (time.time() - t1))
    t1 = time.time()

    simrents = []
    # TEMPLATE creating segments
    segments = jobs.groupby(['general_type'])
    # ENDTEMPLATE
    
    for name, segment in segments:
        name = str(name)
        outname = "nrh" if name is None else "nrh_" + name

        # TEMPLATE computing vars
        est_data = pd.DataFrame(index=segment.index)
        
        # start off this sequence of elif statements with a no-op
        if False:
            pass
        else:
            est_data["accessibility"] = (segment.nets_all_regional1_30.apply(np.log1p)).astype('float')
            est_data["reliability"] = (segment.nets_all_regional2_30.apply(np.log1p)).astype('float')
            est_data["ln_stories"] = (segment.stories.apply(np.log1p)).astype('float')
        est_data = sm.add_constant(est_data, prepend=False)
        est_data = est_data.fillna(0)
        # ENDTEMPLATE
        print("Generating rents on %d %s" %
            (est_data.shape[0], "jobs"))
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
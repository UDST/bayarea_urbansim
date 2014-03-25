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

def jobs_run(dset,year=None,show=True):
    assert "transitionmodel2" == "transitionmodel2"  # should match!
    t1 = time.time()

    # keep original indexes
    if "job_id" not in dset.nets.columns:
        dset.nets = dset.nets.reset_index()

    # TEMPLATE configure table
    jobs = dset.nets
    # ENDTEMPLATE

    # TEMPLATE growth rate
    newdf = jobs[np.random.sample(len(jobs.index)) < 0.05]
    # ENDTEMPLATE

    # TEMPLATE fields to zero out
    newdf["building_id"] = -1
    # ENDTEMPLATE

    print("Adding %d rows" % len(newdf.index))
    if show:
        print(newdf.describe())

    # concat and make index unique again
    dset.nets = pd.concat([dset.nets, newdf], axis=0).reset_index(drop=True)
    if show:
        print(dset.nets.describe())

    dset.store_attr(
        "job_id", year,
        copy.deepcopy(dset.nets.job_id))
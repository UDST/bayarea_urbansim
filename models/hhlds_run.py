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

def hhlds_run(dset,year=None,show=True):
    assert "transitionmodel2" == "transitionmodel2"  # should match!
    t1 = time.time()

    # keep original indexes
    if "household_id" not in dset.households.columns:
        dset.households = dset.households.reset_index()

    # TEMPLATE configure table
    households = dset.households
    # ENDTEMPLATE

    # TEMPLATE growth rate
    newdf = households[np.random.sample(len(households.index)) < 0.05]
    # ENDTEMPLATE

    # TEMPLATE fields to zero out
    newdf["building_id"] = -1
    # ENDTEMPLATE

    print("Adding %d rows" % len(newdf.index))
    if show:
        print(newdf.describe())

    # concat and make index unique again
    dset.households = pd.concat([dset.households, newdf], axis=0).reset_index(drop=True)
    if show:
        print(dset.households.describe())

    dset.store_attr(
        "household_id", year,
        copy.deepcopy(dset.households.household_id))
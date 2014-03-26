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

from urbansim.utils import networks

def networks2_run(dset, year=None, show=True):
    assert "run" == "run"

    if not networks.NETWORKS:
        networks.NETWORKS = networks.Networks(
            [os.path.join(misc.data_dir(), x) for x in ['osm_bayarea.jar']],
            factors=[1.0],
            maxdistances=[2000],
            twoway=[1],
                    impedances=None)
        
    t1 = time.time()

    
    NETWORKS = networks.NETWORKS # make available for the variables
    _tbl_ = pd.DataFrame(
        index=pd.MultiIndex.from_tuples(networks.NETWORKS.nodeids,
                                        names=['_graph_id', '_node_id']))
    
    # start off this sequence of elif statements with a no-op
    if False:
        pass
    else:
        _tbl_["ave_residential_price"] = (NETWORKS.accvar(dset.buildings,1500,vname='res_sales_price',agg='AGG_AVE',decay='DECAY_FLAT')).astype('float')
        _tbl_["ave_retail_rent"] = (NETWORKS.accvar(dset.buildings[dset.buildings.general_type=='Retail'],1500,vname='nonresidential_rent',agg='AGG_AVE',decay='DECAY_FLAT')).astype('float')
        _tbl_["ave_office_rent"] = (NETWORKS.accvar(dset.buildings[dset.buildings.general_type=='Office'],1500,vname='nonresidential_rent',agg='AGG_AVE',decay='DECAY_FLAT')).astype('float')
        _tbl_["ave_industrial_rent"] = (NETWORKS.accvar(dset.buildings[dset.buildings.general_type=='Industrial'],1500,vname='nonresidential_rent',agg='AGG_AVE',decay='DECAY_FLAT')).astype('float')
    _tbl_ = _tbl_.fillna(0)

    if show:
        print(_tbl_.describe())
        _tbl_ = _tbl_.reset_index().set_index(networks.NETWORKS.external_nodeids)
    dset.save_tmptbl("nodes_prices", _tbl_)
    
    _tbl_.to_csv(os.path.join(misc.data_dir(), "nodes_prices.csv"), index_label='node_id', float_format="%.3f")
    
    print("Finished executing in %f seconds" % (time.time() - t1))
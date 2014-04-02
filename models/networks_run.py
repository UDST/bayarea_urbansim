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

def networks_run(dset, year=None, show=True):
    assert "run" == "run"

    if not networks.NETWORKS:
        networks.NETWORKS = networks.Networks(
            [os.path.join(misc.data_dir(), x) for x in ['osm_bayarea.jar']],
            factors=[1.0],
            maxdistances=[2000],
            twoway=[1],
                    impedances=None)
        
    t1 = time.time()

    dset.households = dset.add_xy(dset.households)
    dset.nets = dset.add_xy(dset.nets)
    
    NETWORKS = networks.NETWORKS # make available for the variables
    _tbl_ = pd.DataFrame(
        index=pd.MultiIndex.from_tuples(networks.NETWORKS.nodeids,
                                        names=['_graph_id', '_node_id']))
    
    # start off this sequence of elif statements with a no-op
    if False:
        pass
    else:
        _tbl_["sum_residential_units"] = (NETWORKS.accvar(dset.buildings,500,vname='residential_units').apply(np.log1p)).astype('float')
        _tbl_["sum_nonresidential_units"] = (NETWORKS.accvar(dset.buildings,500,vname='non_residential_units').apply(np.log1p)).astype('float')
        _tbl_["ave_unit_sqft"] = (NETWORKS.accvar(dset.buildings,500,vname='unit_sqft',agg='AGG_AVE',decay='DECAY_FLAT').apply(np.log1p)).astype('float')
        _tbl_["ave_lot_sqft"] = (NETWORKS.accvar(dset.buildings,500,vname='unit_lot_size',agg='AGG_AVE',decay='DECAY_FLAT').apply(np.log1p)).astype('float')
        _tbl_["ave_income"] = (NETWORKS.accvar(dset.households,500,vname='income',agg='AGG_AVE',decay='DECAY_FLAT').apply(np.log1p)).astype('float')
        _tbl_["population"] = (NETWORKS.accvar(dset.households,500,vname='persons').apply(np.log1p)).astype('float')
        _tbl_["poor"] = (NETWORKS.accvar(dset.households[dset.households.income<40000],500,vname='persons').apply(np.log1p)).astype('float')
        _tbl_["hhsize"] = (NETWORKS.accvar(dset.households,500,vname='persons',agg='AGG_AVE',decay='DECAY_FLAT')).astype('float')
        _tbl_["jobs"] = (NETWORKS.accvar(dset.nets,500,vname='emp11').apply(np.log1p)).astype('float')
        _tbl_["sfdu"] = (NETWORKS.accvar(dset.households[dset.households.building_type_id==1],500).apply(np.log1p)).astype('float')
        _tbl_["renters"] = (NETWORKS.accvar(dset.buildings[dset.buildings.tenure==2],500).apply(np.log1p)).astype('float')
    _tbl_ = _tbl_.fillna(0)

    if show:
        print(_tbl_.describe())
        _tbl_ = _tbl_.reset_index().set_index(networks.NETWORKS.external_nodeids)
    dset.save_tmptbl("nodes", _tbl_)
    
    _tbl_.to_csv(os.path.join(misc.data_dir(), "nodes.csv"), index_label='node_id', float_format="%.3f")
    
    print("Finished executing in %f seconds" % (time.time() - t1))
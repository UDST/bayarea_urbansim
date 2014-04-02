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

SAMPLE_SIZE=100

def elcm_simulate(dset, year=None, show=True):
    returnobj = {}
    t1 = time.time()
    # TEMPLATE configure table
    jobs = dset.nets
    # ENDTEMPLATE

    # TEMPLATE dependent variable
    depvar = "building_id"
    # ENDTEMPLATE

    # TEMPLATE computing relocations
    movers = jobs[np.random.sample(len(jobs.index)) < 0.1].index
    print("Count of movers = %d" % len(movers))
    jobs["building_id"].loc[movers] = -1
    # add current unplaced
    movers = jobs[jobs["building_id"] == -1]
    # ENDTEMPLATE
    
    print("Total new agents and movers = %d (out of %d %s)" %
        (len(movers.index),len(jobs.index),"jobs"))

    # TEMPLATE specifying alternatives
    alternatives = dset.building_filter(residential=0)
    # ENDTEMPLATE

    # TEMPLATE computing supply constraint
    vacant_units = dset.building_filter(residential=0).non_residential_units.sub(dset.nets.groupby('building_id').size(),fill_value=0)
    vacant_units = vacant_units[vacant_units>0].order(ascending=False)
    alternatives = alternatives.loc[np.repeat(vacant_units.index,vacant_units.values.astype('int'))].reset_index()
    print("There are %s empty units in %s locations total in the region" %
        (vacant_units.sum(),len(vacant_units)))
    # ENDTEMPLATE
    # TEMPLATE merge
    t_m = time.time()
    alternatives = pd.merge(alternatives, dset.nodes, **{'right_index': True, 'left_on': '_node_id'})
    print("Finished with merge in %f" % (time.time() - t_m))
    # ENDTEMPLATE
    
    print("Finished specifying model in %f seconds" % (time.time() - t1))

    t1 = time.time()

    pdf = pd.DataFrame(index=alternatives.index)
    # TEMPLATE creating segments
    segments = movers.groupby(['naics11cat'])
    # ENDTEMPLATE
    
    for name, segment in segments:
        segment = segment.head(1)

        name = str(name)
        outname = "elcm" if name is None else "elcm_" + name

        SAMPLE_SIZE = alternatives.index.size  # don't sample
        sample, alternative_sample, est_params = \
            interaction.mnl_interaction_dataset(segment, alternatives, SAMPLE_SIZE, chosenalts=None)
        # TEMPLATE computing vars
        print("WARNING: using patsy, ind_vars will be ignored")
        patsy = "np.log1p(stories) + ave_income + poor + sum_nonresidential_units + jobs + sfdu + renters + np.log1p(nonresidential_rent) - 1"
        data = dmatrix(patsy, data=alternative_sample, return_type='dataframe')
        # ENDTEMPLATE
        data = data.as_matrix()

        coeff = dset.load_coeff(outname)
        probs = interaction.mnl_simulate(data, coeff, numalts=SAMPLE_SIZE, returnprobs=1)
        pdf['segment%s'%name] = pd.Series(probs.flatten(), index=alternatives.index)

    print("Finished creating pdf in %f seconds" % (time.time() - t1))
    if len(pdf.columns) and show:
        print(pdf.describe())
    returnobj["elcm"] = misc.pandasdfsummarytojson(pdf.describe(), ndigits=10)
    pdf.describe().to_csv(os.path.join(misc.output_dir(), "elcm_simulate.csv"))

    t1 = time.time()
    # draw from actual units
    new_homes = pd.Series(np.ones(len(movers.index)) * -1, index=movers.index)
    mask = np.zeros(len(alternatives.index), dtype='bool')
    for name, segment in segments:
        name = str(name)
        print("Assigning units to %d agents of segment %s" %
            (len(segment.index), name))
        p=pdf['segment%s' % name].values

        mask, new_homes = dset.choose(p, mask, alternatives, segment, new_homes)
        
    new_homes = pd.Series(alternatives["building_id"].loc[new_homes].values, index=new_homes.index)
    
    build_cnts = new_homes.value_counts()
    print("Assigned %d agents to %d locations with %d unplaced" %
        (new_homes.size,build_cnts.size,build_cnts.get(-1,0)))

    # need to go back to the whole dataset
    table = dset.nets
    table["building_id"].loc[new_homes.index] = new_homes.values
    dset.store_attr("firms_building_ids", year, copy.deepcopy(table["building_id"]))
    print("Finished assigning agents in %f seconds" % (time.time() - t1))
    return returnobj


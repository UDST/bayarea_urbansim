import numpy as np
import pandas as pd
import os
import random
from urbansim.utils import misc
import urbansim.sim.simulation as sim

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


BUILDING_TYPE_MAP = {
    1: "Residential",
    2: "Residential",
    3: "Residential",
    4: "Office",
    5: "Hotel",
    6: "School",
    7: "Industrial",
    8: "Industrial",
    9: "Industrial",
    10: "Retail",
    11: "Retail",
    12: "Residential",
    13: "Retail",
    14: "Office"
}

FORM_TO_BTYPE = {
    'residential': [1, 2, 3],
    'industrial': [7, 8, 9],
    'retail': [10, 11],
    'office': [4],
    'mixedresidential': [12],
    'mixedoffice': [14],
}

STORE_NAME = 'sanfran.h5'
STORE = pd.HDFStore(os.path.join(misc.data_dir(), STORE_NAME), mode="r")
SCENARIO = "baseline"


@sim.table('nodes')
def nodes():
    # default will fetch off disk unless networks have already been run
    print "WARNING: fetching precomputed nodes off of disk"
    df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes.csv'), index_col='node_id')
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    return df


@sim.table('nodes_prices')
def nodes_prices():
    # default will fetch off disk unless networks have already been run
    print "WARNING: fetching precomputed nodes_prices off of disk"
    df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes_prices.csv'), index_col='node_id')
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    return df


@sim.table('building_sqft_per_job')
def building_sqft_per_job():
    return pd.read_csv(os.path.join(misc.data_dir(), 'building_sqft_job.csv'),
                       index_col='building_type_id')


@sim.table('jobs')
def jobs():
    nets = STORE['nets']
    # go from establishments to jobs
    df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)].reset_index()
    df.index.name = 'job_id'
    return df


@sim.table('buildings')
def buildings():
    df = STORE['buildings']
    for col in ["residential_sales_price", "residential_rent", "non_residential_rent"]:
        df[col] = np.nan
    return df


@sim.table('households')
def households():
    df = STORE['households']
    df["building_id"][df.building_id == -1] = np.nan
    return df


@sim.table('parcels')
def parcels():
    return STORE['parcels']


@sim.table('homesales')
def homesales():
    df = STORE['homesales']
    return df.reset_index(drop=True)


@sim.table('costar')
def costar():
    df = STORE['costar']
    return df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]


@sim.table('zoning_for_parcels')
def zoning_for_parcels():
    df = STORE['zoning_for_parcels']
    return df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')


@sim.table('zoning')
def zoning():
    return STORE['zoning']


@sim.table('zoning_baseline')
def zoning_baseline(zoning, zoning_for_parcels):
    return pd.merge(zoning_for_parcels.to_frame(),
                    zoning.to_frame(),
                    left_on='zoning',
                    right_index=True)


@sim.table('zoning_test')
def zoning_test():
    parcels_to_zoning = pd.read_csv(os.path.join(misc.data_dir(), 'parcels_to_zoning.csv'))
    scenario_zoning = pd.read_excel(os.path.join(misc.data_dir(), 'zoning_scenario_test.xls'),
                                    sheetname='zoning_lookup')
    df = pd.merge(parcels_to_zoning,
                  scenario_zoning,
                  on=['jurisdiction', 'pda', 'tpp', 'expansion'],
                  how='left')
    df = df.set_index(df.parcel_id)
    return df


def merge_nodes(df):
    return pd.merge(df, sim.get_table("nodes"), left_on="_node_id", right_index=True)


def random_type(form):
    return random.choice(FORM_TO_BTYPE[form])
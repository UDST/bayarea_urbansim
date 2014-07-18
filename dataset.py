import numpy as np
import pandas as pd
import os
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


# default will fetch off disk unless networks have already been run
df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes.csv'), index_col='node_id')
df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
sim.add_table("nodes", df)


df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes_prices.csv'), index_col='node_id')
df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
sim.add_table("nodes_prices", df)


df = pd.read_csv(os.path.join(misc.data_dir(), 'building_sqft_job.csv'), index_col='building_type_id')
sim.add_table("building_sqft_per_job", df)


nets = STORE['nets']
# go from establishments to jobs
df = nets.loc[np.repeat(nets.index.values, nets.emp11.values)].reset_index()
df.index.name = 'job_id'
sim.add_table("jobs", df)


df = STORE['buildings']
for col in ["residential_sales_price", "residential_rent", "non_residential_rent"]:
    df[col] = np.nan
sim.add_table("buildings", df)


df = STORE['households']
df["building_id"][df.building_id == -1] = np.nan
sim.add_table("households", df)


sim.add_table("parcels", STORE['parcels'])


df = STORE['homesales'].reset_index(drop=True)
sim.add_table("homesales", df)


df = STORE['costar']
df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]
sim.add_table("costar", df)


df = STORE['zoning_for_parcels']
df = df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')
sim.add_table("zoning_for_parcels", df)


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
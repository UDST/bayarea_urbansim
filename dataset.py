import numpy as np
import pandas as pd
import os
from urbansim.utils import misc, dataset

import warnings
warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)


class BayAreaDataset(dataset.Dataset):

    def __init__(self, filename):
        self.scenario = "baseline"
        super(BayAreaDataset, self).__init__(filename)

    def add_zone_id(self, df):
        return self.join_for_field(df, 'buildings', 'building_id', 'zone_id')

    def fetch_jobs(self):
        return self.nets

    @staticmethod
    def fetch_nodes():
        # default will fetch off disk unless networks have already been run
        print "WARNING: fetching precomputed nodes off of disk"
        df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes.csv'), index_col='node_id')
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        return df

    @staticmethod
    def fetch_nodes_prices():
        # default will fetch off disk unless networks have already been run
        print "WARNING: fetching precomputed nodes_prices off of disk"
        df = pd.read_csv(os.path.join(misc.data_dir(), 'nodes_prices.csv'), index_col='node_id')
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        return df

    @staticmethod
    def fetch_building_sqft_per_job():
        return pd.read_csv(os.path.join(misc.data_dir(), 'building_sqft_job.csv'),
                           index_col='building_type_id')

    def fetch_zoning_for_parcels(self):
        df = self.store['zoning_for_parcels']
        return df.reset_index().drop_duplicates(cols='parcel').set_index('parcel')

    def fetch_zoning_baseline(self):
        assert self.zoning_for_parcels.index.is_unique
        return pd.merge(self.zoning_for_parcels, self.zoning, left_on='zoning', right_index=True)

    @staticmethod
    def fetch_zoning_test():
        parcels_to_zoning = pd.read_csv(os.path.join(misc.data_dir(), 'parcels_to_zoning.csv'))
        scenario_zoning = pd.read_excel(os.path.join(misc.data_dir(), 'zoning_scenario_test.xls'),
                                        sheetname='zoning_lookup')
        df = pd.merge(parcels_to_zoning, scenario_zoning,
                      on=['jurisdiction', 'pda', 'tpp', 'expansion'], how='left')
        df = df.set_index(df.parcel_id)
        return df

    def set_scenario(self, scenario):
        assert scenario in ["baseline", "test"]
        self.scenario = scenario

    def merge_nodes(self, df):
        return pd.merge(df, self.nodes, left_on="_node_id", right_index=True)

    def fetch_object(self, name):
        objects = {
            "buildings": Buildings,
            "households": Households,
            "jobs": Jobs
        }
        df = objects[name](self).build_df()
        df['x'] = self.fetch(name)['x']
        df['y'] = self.fetch(name)['y']
        return df


class Buildings(dataset.CustomDataFrame):

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

    def __init__(self, dset, addprices=True):
        """
        Parameters
        ----------
        dset : Dataset
            Pass in the dataset object
        addprices : boolean
            Set addprices to true to add the simulated prices to the dataframe - set to false
            when simulating those same prices
        """
        self.dset = dset
        self.df = dset.buildings
        self.flds = ["year_built", "unit_lot_size", "unit_sqft", "_node_id", "general_type",
                     "stories", "residential_units", "non_residential_units", "building_type_id"]
        if addprices:
            self.flds += ["residential_sales_price", "residential_rent", "non_residential_rent"]

    @property
    def building_type_id(self):
        return self.df.building_type_id

    @property
    def general_type(self):
        return self.building_type_id.map(self.BUILDING_TYPE_MAP)

    @property
    def residential_sales_price(self):
        return self.df.residential_sales_price

    @property
    def residential_rent(self):
        return self.df.residential_rent

    @property
    def non_residential_rent(self):
        return self.df.non_residential_rent

    @property
    def year_built(self):
        return self.df.year_built

    @property
    def stories(self):
        return self.df.stories

    @property
    def total_sqft(self):
        return self.df.building_sqft

    @property
    def residential_units(self):
        return self.df.residential_units

    @property
    def unit_sqft(self):
        return self.total_sqft / self.residential_units

    @property
    def unit_lot_size(self):
        return self.df.lot_size / self.residential_units

    @property
    def non_residential_sqft(self):
        return self.df.non_residential_sqft

    @property
    def non_residential_units(self):
        sqft_per_job = misc.reindex(self.dset.building_sqft_per_job.sqft_per_job,
                                    self.building_type_id.fillna(-1))
        return (self.non_residential_sqft/sqft_per_job).fillna(0).astype('int')

    @property
    def _node_id(self):
        return self.df._node_id


class CoStar(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        self.df = dset.costar[dset.costar.PropertyType.isin(["Office", "Retail", "Industrial", "Flex"])]
        self.flds = ["rent", "stories", "_node_id", "year_built"]

    @property
    def year_built(self):
        return self.df.year_built

    @property
    def rent(self):
        return self.df.averageweightedrent

    @property
    def stories(self):
        return self.df.number_of_stories

    @property
    def _node_id(self):
        return self.df._node_id


class Apartments(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        self.df = dset.apartments.dropna()
        self.flds = ["_node_id", "rent", "unit_sqft"]

    @property
    def _node_id(self):
        return misc.reindex(self.dset.parcels._node_id, self.df.parcel_id)

    @property
    def rent(self):
        return (self.df.MinOfLowRent+self.df.MaxOfHighRent)/2.0/self.unit_sqft

    @property
    def unit_sqft(self):
        return self.df.AvgOfSquareFeet


class Households(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        self.dset.households["building_id"][self.dset.households.building_id == -1] = np.nan
        self.df = self.dset.households
        self.flds = ["income", "income_quartile", "building_id", "tenure", "persons"]

    @property
    def income(self):
        return self.df.income

    @property
    def income_quartile(self):
        return pd.Series(pd.qcut(self.df.income, 4).labels, index=self.df.index)

    @property
    def building_id(self):
        return self.df.building_id

    @property
    def tenure(self):
        return self.df.tenure

    @property
    def persons(self):
        return self.df.persons


class Jobs(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        # go from establishments to jobs
        jobs = dset.nets.loc[np.repeat(dset.nets.index.values, dset.nets.emp11.values)].reset_index()
        jobs.index.name = 'job_id'
        dset.jobs = self.df = jobs
        self.flds = ["building_id"]

    @property
    def building_id(self):
        return self.df.building_id


class HomeSales(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        self.df = self.dset.homesales
        self.flds = ["sale_price_flt", "year_built", "unit_lot_size", "unit_sqft", "_node_id"]

    @property
    def sale_price_flt(self):
        return self.df.Sale_price.str.replace('$', '').str.replace(',', '').astype('f4') / \
            self.unit_sqft

    @property
    def year_built(self):
        return self.df.Year_built

    @property
    def unit_lot_size(self):
        return self.df.Lot_size

    @property
    def unit_sqft(self):
        return self.df.SQft

    @property
    def _node_id(self):
        return self.df._node_id


class Parcels(dataset.CustomDataFrame):

    def __init__(self, dset):
        self.dset = dset
        self.df = self.dset.parcels
        self.flds = ["parcel_size", "total_units", "total_sqft", "land_cost", "max_far",
                     "max_height"]

    def price(self, use):
        return misc.reindex(self.dset.nodes_prices[use], self.df._node_id)

    def allowed(self, form):
        # we have zoning by building type but want to know if specific forms are allowed
        allowed = [self.dset.zoning_baseline['type%d' % typ] == 't' for typ in self.type_d[form]]
        return pd.concat(allowed, axis=1).max(axis=1)\
            .reindex(self.df.index).fillna(False)

    @property
    def max_far(self):
        baseline = self.dset.zoning_baseline
        max_far = baseline.max_far
        if self.dset.scenario == "test":
            upzone = self.dset.zoning_test_scenario.far_up.dropna()
            max_far = pd.concat([max_far, upzone], axis=1).max(skipna=True, axis=1)
        return max_far.reindex(self.df.index).fillna(0)

    @property
    def max_height(self):
        return self.dset.zoning_baseline.max_height\
            .reindex(self.df.index).fillna(0)

    @property
    def parcel_size(self):
        return self.df.shape_area * 10.764

    @property
    def total_units(self):
        return self.dset.buildings.groupby('parcel_id').residential_units.sum()\
            .reindex(self.df.index).fillna(0)

    @property
    def total_sqft(self):
        return self.dset.buildings.groupby('parcel_id').building_sqft.sum()\
            .reindex(self.df.index).fillna(0)

    @property
    def land_cost(self):
        # TODO
        # this needs to account for cost for the type of building it is
        return (self.total_sqft * self.price("residential"))\
            .reindex(self.df.index).fillna(0)



LocalDataset = BayAreaDataset
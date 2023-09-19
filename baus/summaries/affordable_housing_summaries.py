from __future__ import print_function

import os
import orca
import pandas as pd
from baus import datasources

@orca.step()
def deed_restricted_units_summary(run_name, parcels, buildings, year, initial_summary_year, final_year, superdistricts_geography):

    if year != initial_summary_year and year != final_year:
        return

    # get buldings table and geography columns to tally deed restricted (dr) units
    buildings = orca.merge_tables('buildings', [parcels, buildings],
                columns=['juris', 'superdistrict', 'county', 'residential_units',
                        'deed_restricted_units', 'preserved_units', 'inclusionary_units', 'subsidized_units', 'source'])
    
    # use the buildings "source" column to get dr units by source
    # this include removing preserved unit counts so that they're not double counted
    buildings["h5_dr_units"] = 0
    buildings.loc[buildings.source == 'h5_inputs', "h5_dr_units"] = buildings["deed_restricted_units"] - buildings["preserved_units"]
    buildings["cs_dr_units"] = 0
    buildings.loc[buildings.source == 'cs', "cs_dr_units"] = buildings["deed_restricted_units"] - buildings["preserved_units"]
    buildings["pub_dr_units"] = 0
    buildings.loc[buildings.source == 'pub', "pub_dr_units"] = buildings["deed_restricted_units"] - buildings["preserved_units"]
    buildings["mall_office_dr_units"] = 0
    buildings.loc[buildings.source == 'mall_office', "mall_office_dr_units"] = buildings["deed_restricted_units"] - buildings["preserved_units"]
    buildings["opp_dr_units"] = 0
    buildings.loc[buildings.source == 'opp', "opp_dr_units"] = buildings["deed_restricted_units"] - buildings["preserved_units"]

    #### regional deed restricted units summary ####
    # initialize region-level table
    region_dr = pd.DataFrame(index=['region'])
    region_dr.index.name = 'region'

    # add total dr units
    region_dr ['total_dr_units'] = buildings.deed_restricted_units.sum()

    # add types of modeled dr units
    region_dr["inclusionary_units"] = buildings.inclusionary_units.sum()
    region_dr["subsidized_units"] = buildings.subsidized_units.sum()
    region_dr["preserved_units"] = buildings.preserved_units.sum()
    # add dr units from strategies (entered through pipeline)
    region_dr["public_lands_dr_units"] = buildings.pub_dr_units.sum()
    region_dr["mall_office_dr_units"] = buildings.mall_office_dr_units.sum()
    region_dr["opp_dr_units"] = buildings.opp_dr_units.sum()
    # add base year and pipeline dr units
    region_dr["h5_dr_units"] = buildings.h5_dr_units.sum()
    region_dr["cs_dr_units"] = buildings.cs_dr_units.sum()

    region_dr.to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                  "affordable_housing_summaries/{}_region_dr_summary_{}.csv").format(run_name, year))

    #### geographic deed restricted units summary ####
    geographies = ['juris', 'superdistrict', 'county']
    
    for geography in geographies:

        # remove rows with null geography- seen with "county"
        buildings = buildings[~pd.isna(buildings[geography])]

        # initialize a dataframe for the geography
        summary_table = pd.DataFrame(index=buildings[geography].unique())

        # add superdistrict name 
        if geography == 'superdistrict':
            superdistricts_geography = superdistricts_geography.to_frame()
            summary_table = summary_table.merge(superdistricts_geography[['name']], left_index=True, right_index=True)

        # add total dr units
        summary_table['total_dr_units'] = buildings.groupby(geography)["deed_restricted_units"].sum()

        # add types of modeled dr units
        summary_table["inclusionary_units"] = buildings.groupby(geography)["inclusionary_units"].sum()
        summary_table["subsidized_units"] = buildings.groupby(geography)["subsidized_units"].sum()
        summary_table["preserved_units"] = buildings.groupby(geography)["preserved_units"].sum()
        # add dr units from strategies (entered through pipeline)
        summary_table["public_lands_dr_units"] = buildings.groupby(geography)["pub_dr_units"].sum()
        summary_table["mall_office_dr_units"] = buildings.groupby(geography)["mall_office_dr_units"].sum()
        summary_table["opp_dr_units"] = buildings.groupby(geography)["opp_dr_units"].sum()
        # add base year and pipeline dr units
        summary_table["h5_dr_units"] = buildings.groupby(geography)["h5_dr_units"].sum()
        summary_table["cs_dr_units"] = buildings.groupby(geography)["cs_dr_units"].sum()

        summary_table.index.name = geography
        summary_table = summary_table.sort_index()
        summary_table.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), "affordable_housing_summaries/{}_{}_dr_summary_{}.csv").\
                                          format(run_name, geography, year))
        

@orca.step()
def deed_restricted_units_growth_summary(year, initial_summary_year, final_year, run_name):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county']

    for geography in geographies:

        # use 2015 as the base year
        year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "affordable_housing_summaries/%s_%s_dr_summary_%d.csv" % (run_name, geography, initial_summary_year)))
        year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "affordable_housing_summaries/%s_%s_dr_summary_%d.csv" % (run_name, geography, final_year)))

        dr_growth = year1.merge(year2, on=geography, suffixes=("_"+str(initial_summary_year), "_"+str(final_year)))
        
        dr_growth["run_name"] = run_name

        columns = ['total_dr_units', "inclusionary_units", "subsidized_units", "preserved_units", "public_lands_dr_units", 
                   "mall_office_dr_units", "opp_dr_units", "h5_dr_units", "cs_dr_units"]
    
        for col in columns:
            # growth in units
            dr_growth[col+'_growth'] = dr_growth[col+"_"+str(final_year)] - dr_growth[col+"_"+str(initial_summary_year)]

            # change in the regional share of units in the geography 
            dr_growth[col+"_"+str(initial_summary_year)+"_share"] = (round(dr_growth[col+"_"+str(initial_summary_year)] / 
                                                                           dr_growth[col+"_"+str(initial_summary_year)].sum(), 2))
            dr_growth[col+"_"+str(final_year)+"_share"] = (round(dr_growth[col+"_"+str(final_year)] / 
                                                                 dr_growth[col+"_"+str(final_year)].sum(), 2)   )         
            dr_growth[col+'_share_change'] =  (dr_growth[col+"_"+str(final_year)+"_share"] - 
                                               dr_growth[col+"_"+str(initial_summary_year)+"_share"])
        
        dr_growth = dr_growth.fillna(0)
        dr_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "affordable_housing_summaries/{}_{}_dr_growth.csv").format(run_name, geography))
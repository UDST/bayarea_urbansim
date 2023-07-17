from __future__ import print_function

import os
import orca
import pandas as pd
from baus.utils import format_df
from urbansim.utils import misc
from baus import datasources

@orca.step()
def deed_restricted_units_summary(parcels, buildings, year, initial_summary_year, final_year, run_number):

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
    region_dr = pd.DataFrame(index={'region'})

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

    region_dr.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_region_dr_summary_{}.csv").format(run_number, year))

    #### geographic deed restricted units summary ####
    geographies = ['juris', 'superdistrict', 'county']
    
    for geography in geographies:
        # initialize a dataframe for the geography
        summary_table = pd.DataFrame(index=buildings[geography].unique())

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

        summary_table = summary_table.sort_index()
        summary_table.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_dr_summary_{}.csv").\
                    format(run_number, geography, year))
        

@orca.step()
def deed_restricted_units_growth_summary(year, initial_summary_year, final_year, run_number):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county']

    for geography in geographies:

        # use 2015 as the base year
        year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_dr_summary_%d.csv" % (run_number, geography, initial_summary_year)))
        year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_dr_summary_%d.csv" % (run_number, geography, final_year)))

        dr_growth = pd.DataFrame(index=year1.index)

        columns = ['total_dr_units', "inclusionary_units", "subsidized_units", "preserved_units", "public_lands_dr_units", 
                   "mall_office_dr_units", "opp_dr_units", "h5_dr_units", "cs_dr_units"]
    
        for col in columns:
            dr_growth[col+"_"+str(initial_summary_year)] = year1[col]
            dr_growth[col+"_"+str(final_year)] = year2[col]
            # growth in units
            dr_growth[col+'_growth'] = year2[col] - year1[col]

            # change in the regional share of units in the geography 
            dr_growth[col+"_"+str(initial_summary_year)+"_share"] = round(year1[col] / year1[col].sum(), 2)
            dr_growth[col+"_"+str(final_year)+"_share"] = round(year2[col] / year2[col].sum(), 2)            
            dr_growth[col+'_share_change'] =  (dr_growth[col+"_"+str(final_year)+"_share"] - 
                                               dr_growth[col+"_"+str(initial_summary_year)+"_share"])
        
        dr_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_dr_growth.csv").format(run_number, geography))
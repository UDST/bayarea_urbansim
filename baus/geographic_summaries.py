from __future__ import print_function

import os
import orca
import pandas as pd
import numpy as np
from baus import datasources


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, run_number, year):  

    households_df = orca.merge_tables('households', [parcels, buildings, households],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'base_income_quartile',])

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'empsix'])

    buildings_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'building_type', 
                 'residential_units', 'deed_restricted_units', 'non_residential_sqft'])

    #### summarize regional results ####
    region = pd.DataFrame(index=['region'])

    # households
    region['tothh'] = households_df.size
    for quartile in [1, 2, 3, 4]:
        region['hhincq'+str(quartile)] = households_df[households_df.base_income_quartile == quartile].size

    # employees by sector
    region['totemp'] = jobs_df.size
    for empsix in [ 'AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
        region[empsix] = jobs_df[jobs_df.empsix == empsix].size

    # residential buildings
    region['res_units'] = buildings_df.residential_units.sum() 
    region['deed_restricted_units'] = buildings_df.deed_restricted_units.sum()  
    region['sfdu'] = buildings_df[(buildings_df.building_type == 'HS') | (buildings_df.building_type == 'HT')].residential_units.sum()
    region['mfdu'] = buildings_df[(buildings_df.building_type == 'HM') | (buildings_df.building_type == 'MR')].residential_units.sum()
    
    # non-residential buildings
    region['non_residential_sqft'] = buildings_df.non_residential_sqft.sum()
    region.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_region_summary_{}.csv").format(run_number, year))

    #### summarize by sub-regional geography ####
    geographies = ['juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:
        summary_table = pd.DataFrame(index=buildings_df[geography].unique())

        # households
        summary_table['tothh'] = households_df.groupby(geography).size()
        for quartile in [1, 2, 3, 4]:
            summary_table['hhincq'+str(quartile)] = households_df[households_df.base_income_quartile == quartile].groupby(geography).size()

        # residential buildings
        summary_table['residential_units'] = buildings_df.groupby(geography).residential_units.sum() 
        summary_table['deed_restricted_units'] = buildings_df.groupby(geography).deed_restricted_units.sum() 
        summary_table['sfdu'] = buildings_df[(buildings_df.building_type == 'HS') | (buildings_df.building_type == 'HT')].\
            groupby(geography).residential_units.sum()
        summary_table['mfdu'] = buildings_df[(buildings_df.building_type == 'HM') | (buildings_df.building_type == 'MR')].\
            groupby(geography).residential_units.sum()
        
        # employees by sector
        summary_table['totemp'] = jobs_df.groupby(geography).size()
        for empsix in ['AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
            summary_table[empsix] = jobs_df[jobs_df.empsix == empsix].groupby(geography).size()

        # non-residential buildings
        summary_table['non_residential_sqft'] = buildings_df.groupby(geography)['non_residential_sqft'].sum().round(0)
   
        summary_table = summary_table.sort_index()
        summary_table.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summary_{}.csv").\
                    format(run_number, geography, year))


@orca.step()
def geographic_growth_summary(year, final_year, initial_summary_year, run_number):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:

        geography_growth = pd.DataFrame(index=[0])

        # use 2015 as the base year
        year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summary_%d.csv" % (run_number, geography, initial_summary_year)))
        year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summary_%d.csv" % (run_number, geography, final_year)))

        columns = ['tothh', 'hhincq1', 'hhincq4', 'totemp', 'residential_units', 'deed_restricted_units', 'non_residential_sqft']
    
        for col in columns:
            geography_growth[col+"_"+str(initial_summary_year)] = year1[col]
            geography_growth[col+"_"+str(final_year)] = year2[col]
            # growth in households/jobs/etc.
            geography_growth[col+"growth"] = year2[col] - year1[col]

            # percent change in geography's households/jobs/etc.
            geography_growth[col+'_pct_change'] = round((year2[col] / year1[col] - 1), 2)

            # percent geography's growth of households/jobs/etc. of all regional growth in households/jobs/etc.
            geography_growth[col+'_pct_of_regional_growth'] = round(((geography_growth[col+"growth"]) / 
                                                                     (year2[col].sum() - year1[col].sum()) * 100), 2)

            # change in the regional share of households/jobs/etc. in the geography      
            geography_growth[col+"_"+str(initial_summary_year)+"_regional_share"] = round(year1[col] / year1[col].sum(), 2)
            geography_growth[col+"_"+str(final_year)+"_regional_share"] = round(year2[col] / year2[col].sum(), 2)            
            geography_growth[col+'_regional_share_change'] = (geography_growth[col+"_"+str(final_year)+"_regional_share"] - 
                                                              geography_growth[col+"_"+str(initial_summary_year)+"_regional_share"])
    
        geography_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summary_growth.csv").format(run_number, geography))
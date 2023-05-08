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
        columns=['juris', 'superdistrict', 'county', 'building_type', 'residential_units',
                 'deed_restricted_units', 'building_sqft', 'subregion', 'non_residential_sqft'])

    #### summarize regional results ####
    region = pd.DataFrame(data={'REGION': [1]})
    
    # households
    region['tothh'] = households_df.size
    region['hhincq1'] = households_df.query("base_income_quartile == 1").size
    region['hhincq2'] = households_df.query("base_income_quartile == 2").size
    region['hhincq3'] = households_df.query("base_income_quartile == 3").size
    region['hhincq4'] = households_df.query("base_income_quartile == 4").size
    # residential buildings
    region['res_units'] = buildings_df.residential_units.sum() 
    region['sfdu'] = buildings_df.query("building_type == 'HS' or building_type == 'HT'").residential_units.sum()
    region['mfdu'] = buildings_df.query("building_type == 'HM' or building_type == 'MR'").residential_units.sum()

    # employees by sector
    region['totemp'] = jobs_df.size
    region['agrempn'] = jobs_df.query("empsix == 'AGREMPN'").size
    region['mwtempn'] = jobs_df.query("empsix == 'MWTEMPN'").size
    region['retempn'] = jobs_df.query("empsix == 'RETEMPN'").size
    region['fpsempn'] = jobs_df.query("empsix == 'FPSEMPN'").size
    region['herempn'] = jobs_df.query("empsix == 'HEREMPN'").size
    region['othempn'] = jobs_df.query("empsix == 'OTHEMPN'").size
    # non-residential buildings
    region['non_residential_sqft'] = buildings_df.non_residential_sqft.sum()

    region.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_region_summary_{}.csv").format(run_number, year))

    #### summarize by sub-regional geography ####
    geographies = ['juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:
        # create dataframe for the geography
        summary_table = pd.DataFrame(index=buildings_df[geography].unique())

        # households
        summary_table['tothh'] = households_df.groupby(geography).size
        summary_table['hhincq1'] = households_df.query("base_income_quartile == 1").groupby(geography).size
        summary_table['hhincq2'] = households_df.query("base_income_quartile == 2").groupby(geography).size
        summary_table['hhincq3'] = households_df.query("base_income_quartile == 3").groupby(geography).size
        summary_table['hhincq4'] = households_df.query("base_income_quartile == 4").groupby(geography).size

        # residential buildings
        summary_table['res_units'] = buildings_df.groupby(geography).residential_units.sum() 
        summary_table['sfdu'] = buildings_df.query("building_type == 'HS' or building_type == 'HT'").\
            groupby(geography).residential_units.sum()
        summary_table['mfdu'] = buildings_df.query("building_type == 'HM' or building_type == 'MR'").\
            groupby(geography).residential_units.sum()

        # employees by sector
        summary_table['totemp'] = jobs_df.groupby(geography).size
        summary_table['agrempn'] = jobs_df.query("empsix == 'AGREMPN'").groupby(geography).size
        summary_table['mwtempn'] = jobs_df.query("empsix == 'MWTEMPN'").groupby(geography).size
        summary_table['retempn'] = jobs_df.query("empsix == 'RETEMPN'").groupby(geography).size
        summary_table['fpsempn'] = jobs_df.query("empsix == 'FPSEMPN'").groupby(geography).size
        summary_table['herempn'] = jobs_df.query("empsix == 'HEREMPN'").groupby(geography).size
        summary_table['othempn'] = jobs_df.query("empsix == 'OTHEMPN'").groupby(geography).size

        # non-residential buildings
        summary_table['non_residential_sqft'] = buildings_df.groupby(geography)['non_residential_sqft'].sum()
   
        summary_table = summary_table.sort_index()
        summary_table.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summary_{}.csv").\
                    format(run_number, geography, year))


@orca.step()
def geographic_growth_summary(year, final_year, run_number):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:

        # use 2015 as the base year
        df1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summary_%d.csv" % (run_number, geography, 2015)))
        df2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summary_%d.csv" % (run_number, geography, final_year)))
        df_merge = df1.merge(df2, on = geography).fillna(0)

        columns = ['tothh', 'hhincq1', 'hhincq4', 'totemp', 'res_units', 'deed_restricted_units', 'non_residential_sqft']
    
        for col in columns:
            df_merge[col+"_2015"] = df_merge[col+'_x']
            df_merge[col+"_2050"] = df_merge[col+'_y']
            df_merge[col+'_growth'] = df_merge[col+'_y'] - df_merge[col+'_x']
            df_merge[col+"_2015_share"] = round(df_merge[col+'_x']/df_merge[col+'_x'].sum(), 2)
            df_merge[col+"_2050_share"] = round(df_merge[col+'_y']/df_merge[col+'_y'].sum(), 2)            
            df_merge[col+'_share_change'] =  df_merge[col+"_2050_share"] - df_merge[col+"_2015_share"]
        
        df_merge.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summary_growth.csv").format(run_number, geography))
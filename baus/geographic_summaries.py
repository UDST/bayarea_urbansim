from __future__ import print_function

import os
import orca
import pandas as pd
import numpy as np
from baus import datasources


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, run_number, year):  

    households_df = orca.merge_tables('households', [parcels, buildings, households],
        columns=['county', 'juris', 'superdistrict',
                 'persons', 'income', 'base_income_quartile',
                 'juris_tra', 'juris_sesit', 'juris_ppa'])

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs],
        columns=['county', 'juris', 'superdistrict', 'empsix'])

    buildings_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['county', 'superdistrict', 'juris', 'building_type', 'zone_id', 'residential_units',
                 'deed_restricted_units', 'preserved_units', 'inclusionary_units', 'subsidized_units',
                 'building_sqft', 'non_residential_sqft'])

    # because merge_tables returns multiple zone_id_'s, but not the one we need
    buildings_df = buildings_df.rename(columns={'zone_id_x': 'zone_id'})

    geographies = ['juris', 'superdistrict', 'county']

    for geography in geographies:

        # create table with household/population summaries
        summary_table = pd.pivot_table(households_df, values=['persons'], index=[geography], aggfunc=[np.size])
        summary_table.columns = ['tothh']
           
         # fill in 0 values where there are NA's so that summary table outputs are the same over the years 
         # otherwise a summary geography would be dropped if it had no employment or housing
        if geography == 'superdistrict':
            all_summary_geographies = buildings_df[geography].unique()
        else:
            all_summary_geographies = parcels[geography].unique()
        summary_table = summary_table.reindex(all_summary_geographies).fillna(0)

        # income quartile counts
        summary_table['hhincq1'] = households_df.query("base_income_quartile == 1").groupby(geography).size()
        summary_table['hhincq2'] = households_df.query("base_income_quartile == 2").groupby(geography).size()
        summary_table['hhincq3'] = households_df.query("base_income_quartile == 3").groupby(geography).size()
        summary_table['hhincq4'] = households_df.query("base_income_quartile == 4").groupby(geography).size()

        # residential buildings by type
        summary_table['res_units'] = buildings_df.groupby(geography).residential_units.sum()
        summary_table['sfdu'] = buildings_df.query("building_type == 'HS' or building_type == 'HT'").\
            groupby(geography).residential_units.sum()
        summary_table['mfdu'] = buildings_df.query("building_type == 'HM' or building_type == 'MR'").\
            groupby(geography).residential_units.sum()

        # employees by sector
        summary_table['totemp'] = jobs_df.groupby(geography).size()
        summary_table['agrempn'] = jobs_df.query("empsix == 'AGREMPN'").groupby(geography).size()
        summary_table['mwtempn'] = jobs_df.query("empsix == 'MWTEMPN'").groupby(geography).size()
        summary_table['retempn'] = jobs_df.query("empsix == 'RETEMPN'").groupby(geography).size()
        summary_table['fpsempn'] = jobs_df.query("empsix == 'FPSEMPN'").groupby(geography).size()
        summary_table['herempn'] = jobs_df.query("empsix == 'HEREMPN'").groupby(geography).size()
        summary_table['othempn'] = jobs_df.query("empsix == 'OTHEMPN'").groupby(geography).size()

        # summary columns
        summary_table['occupancy_rate'] = summary_table['tothh'] / (summary_table['sfdu'] + summary_table['mfdu'])
        summary_table['non_residential_sqft'] = buildings_df.groupby(geography)['non_residential_sqft'].sum()

        # columns re: affordable housing
        summary_table['deed_restricted_units'] = buildings_df.groupby(geography).deed_restricted_units.sum()
        summary_table['preserved_units'] = buildings_df.groupby(geography).preserved_units.sum()
        summary_table['inclusionary_units'] = buildings_df.groupby(geography).inclusionary_units.sum()
        summary_table['subsidized_units'] = buildings_df.groupby(geography).subsidized_units.sum()       

        summary_table = summary_table.sort_index()
        summary_table.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summaries_{}.csv").\
                    format(run_number, geography, year))


@orca.step()
def geographic_growth_summaries(final_year, run_number):
    if year != final_year: 
        return
    baseyear = 2015

    geographies = ['juris', 'superdistrict', 'county']

    for geography in geographies:

    # (1) geographic growth
        DF1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summaries_%d.csv" % (run_number, geography, baseyear)))
        DF2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_%s_summaries_%d.csv" % (run_number, geography, final_year)))
    
        DF_merge = DF1.merge(DF2, on = geography).fillna(0)
    
        DF_merge['sd_name'] = DF_merge['superdistrict'].map(sd_mapping)
        df_growth['county'] = df_growth['juris'].map(juris_to_county)

        columns = ['hhincq1', 'hhincq2', 'hhincq3', 'hhincq4', 'res_units', 'mfdu', 'sfdu', 'totemp', 'agrempn', 'mwtempn', 'retempn',
                'fpsempn', 'herempn', 'othempn', 'non_residential_sqft', 'deed_restricted_units', 'preserved_units', 
                'subsidized_units', 'inclusionary_units']
    
        # calculate absolute growth 
        for col in columns:
            DF_merge[col+' growth'] = DF_merge[col+'_y']-DF_merge[col+'_x']
        # calculate percent growth
        for col in columns:
            DF_merge[col+' pct growth'] = round(DF_merge[col+'_y']/DF_merge[col+'_x']-1, 2)
        # calculate growth as share of regional growth
        for col in columns:
            DF_merge[col+' growth shr'] = round(DF_merge[col+' GROWTH']/(DF_merge[col+'_y'].sum()-DF_merge[col+'_x'].sum()), 2)
        # calculate growth as change in regional share of growth
        for col in columns:
            DF_merge[col+' shr chng'] = round(DF_merge[col+'_y']/DF_merge[col+'_y'].sum()-DF_merge[col+'_x']/DF_merge[col+'_x'].sum(), 2)
        
        DF_merge.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_taz_growth_summaries.csv" % run_number))
        

        df_growth.sort_values(by = ['county','juris','geo_category'], ascending=[True, True, False], inplace=True)
        df_growth.set_index(['RUNID','county','juris','geo_category'], inplace=True)
        df_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_%s_growth_summaries.csv".format(run_number, geography)))


### VISUALIZER CALCS
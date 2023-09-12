from __future__ import print_function

import os
import orca
import pandas as pd
from baus import datasources


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, year, superdistricts_geography,
                       initial_summary_year, interim_summary_year, final_year, run_name):  

    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    households_df = orca.merge_tables('households', [parcels, buildings, households],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'base_income_quartile',])

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'empsix'])

    buildings_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'building_type', 
                 'residential_units', 'deed_restricted_units', 'non_residential_sqft'])

    #### summarize regional results ####
    region = pd.DataFrame(index=['region'])
    region.index.name = 'region'

    # households
    region['tothh'] = households_df.size
    for quartile in [1, 2, 3, 4]:
        region['hhincq'+str(quartile)] = households_df[households_df.base_income_quartile == quartile].size

    # employees by sector
    region['totemp'] = jobs_df.size
    for empsix in [ 'AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
        region[empsix] = jobs_df[jobs_df.empsix == empsix].size

    # residential buildings
    region['residential_units'] = buildings_df.residential_units.sum() 
    region['deed_restricted_units'] = buildings_df.deed_restricted_units.sum()  
    region['sfdu'] = buildings_df[(buildings_df.building_type == 'HS') | (buildings_df.building_type == 'HT')].residential_units.sum()
    region['mfdu'] = buildings_df[(buildings_df.building_type == 'HM') | (buildings_df.building_type == 'MR')].residential_units.sum()
    
    # non-residential buildings
    region['non_residential_sqft'] = buildings_df.non_residential_sqft.sum()
    region.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "geographic_summaries/region_summary_{}.csv").format(year))

    #### summarize by sub-regional geography ####
    geographies = ['juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:

        # remove rows with null geography- seen with "county"
        buildings_df = buildings_df[~pd.isna(buildings_df[geography])]
        households_df = households_df[~pd.isna(households_df[geography])]
        jobs_df = jobs_df[~pd.isna(jobs_df[geography])]

        summary_table = pd.DataFrame(index=buildings_df[geography].unique())
        
        # add superdistrict name 
        if geography == 'superdistrict':
            superdistricts_geography = superdistricts_geography.to_frame()
            summary_table = summary_table.merge(superdistricts_geography[['name']], left_index=True, right_index=True)

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
   
        summary_table.index.name = geography
        summary_table = summary_table.sort_index()
        summary_table.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                                    "geographic_summaries/{}_{}_summary_{}.csv").\
                                                    format(run_name, geography, year))


@orca.step()
def geographic_growth_summary(year, final_year, initial_summary_year, run_name):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:

        # use 2015 as the base year
        year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "geographic_summaries/%s_%s_summary_%d.csv" % (run_name, geography, initial_summary_year)))
        year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "geographic_summaries/%s_%s_summary_%d.csv" % (run_name, geography, final_year)))

        geog_growth = year1.merge(year2, on=geography, suffixes=("_"+str(initial_summary_year), "_"+str(final_year)))

        if geography == 'superdistict':
            geog_growth = geog_growth.rename(columns={"name_"+(str(initial_summary_year)): "name"})
            geog_growth = geog_growth.drop(columns=["name_"+(str(final_year))])
        
        columns = ['tothh', 'totemp', 'residential_units', 'deed_restricted_units', 'non_residential_sqft']
    
        for col in columns:
            # growth in households/jobs/etc.
            geog_growth[col+"_growth"] = (geog_growth[col+"_"+str(final_year)] - 
                                          geog_growth[col+"_"+str(initial_summary_year)])

            # percent change in geography's households/jobs/etc.
            geog_growth[col+'_pct_change'] = (round((geog_growth[col+"_"+str(final_year)] / 
                                                     geog_growth[col+"_"+str(initial_summary_year)] - 1) * 100, 2))

            # percent geography's growth of households/jobs/etc. of all regional growth in households/jobs/etc.
            geog_growth[col+'_pct_of_regional_growth'] = (round(((geog_growth[col+"_growth"]) / 
                                                                 (geog_growth[col+"_"+str(final_year)].sum() - 
                                                                  geog_growth[col+"_"+str(initial_summary_year)].sum())) * 100, 2))

            # change in the regional share of households/jobs/etc. in the geography      
            geog_growth[col+"_"+str(initial_summary_year)+"_regional_share"] = (round(geog_growth[col+"_"+str(initial_summary_year)] / 
                                                                                     geog_growth[col+"_"+str(initial_summary_year)].sum(), 2))
            geog_growth[col+"_"+str(final_year)+"_regional_share"] = (round(geog_growth[col+"_"+str(final_year)] / 
                                                                           geog_growth[col+"_"+str(final_year)].sum(), 2))            
            geog_growth[col+'_regional_share_change'] = (geog_growth[col+"_"+str(final_year)+"_regional_share"] - 
                                                         geog_growth[col+"_"+str(initial_summary_year)+"_regional_share"])
    
        geog_growth = geog_growth.fillna(0)
        geog_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                        "geographic_summaries/{}_{}_summary_growth.csv").format(run_name, geography))
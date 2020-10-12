
#The script is used for process geograpy summary files and combine different run results for visualization in Tableau
import pandas as pd
import numpy as np
import os


juris_to_county = {'alameda' : 'Alameda',
'albany' : 'Alameda',
'american_canyon' : 'Napa',
'antioch' : 'Contra Costa',
'atherton' : 'San Mateo',
'belmont' : 'San Mateo',
'belvedere' : 'Marin',
'benicia' : 'Solano',
'berkeley' : 'Alameda',
'brentwood' : 'Contra Costa',
'brisbane' : 'San Mateo',
'burlingame' : 'San Mateo',
'calistoga' : 'Napa',
'campbell' : 'Santa Clara',
'clayton' : 'Contra Costa',
'cloverdale' : 'Sonoma',
'colma' : 'San Mateo',
'concord' : 'Contra Costa',
'corte_madera' : 'Marin',
'cotati' : 'Sonoma',
'cupertino' : 'Santa Clara',
'daly_city' : 'San Mateo',
'danville' : 'Contra Costa',
'dixon' : 'Solano',
'dublin' : 'Alameda',
'east_palo_alto' : 'San Mateo',
'el_cerrito' : 'Contra Costa',
'emeryville' : 'Alameda',
'fairfax' : 'Marin',
'fairfield' : 'Solano',
'foster_city' : 'San Mateo',
'fremont' : 'Alameda',
'gilroy' : 'Santa Clara',
'half_moon_bay' : 'San Mateo',
'hayward' : 'Alameda',
'healdsburg' : 'Sonoma',
'hercules' : 'Contra Costa',
'hillsborough' : 'San Mateo',
'lafayette' : 'Contra Costa',
'larkspur' : 'Marin',
'livermore' : 'Alameda',
'los_altos' : 'Santa Clara',
'los_altos_hills' : 'Santa Clara',
'los_gatos' : 'Santa Clara',
'martinez' : 'Contra Costa',
'menlo_park' : 'San Mateo',
'millbrae' : 'San Mateo',
'mill_valley' : 'Marin',
'milpitas' : 'Santa Clara',
'monte_sereno' : 'Santa Clara',
'moraga' : 'Contra Costa',
'morgan_hill' : 'Santa Clara',
'mountain_view' : 'Santa Clara',
'napa' : 'Napa',
'newark' : 'Alameda',
'novato' : 'Marin',
'oakland' : 'Alameda',
'oakley' : 'Contra Costa',
'orinda' : 'Contra Costa',
'pacifica' : 'San Mateo',
'palo_alto' : 'Santa Clara',
'petaluma' : 'Sonoma',
'piedmont' : 'Alameda',
'pinole' : 'Contra Costa',
'pittsburg' : 'Contra Costa',
'pleasant_hill' : 'Contra Costa',
'pleasanton' : 'Alameda',
'portola_valley' : 'San Mateo',
'redwood_city' : 'San Mateo',
'richmond' : 'Contra Costa',
'rio_vista' : 'Solano',
'rohnert_park' : 'Sonoma',
'ross' : 'Marin',
'st_helena' : 'Napa',
'san_anselmo' : 'Marin',
'san_bruno' : 'San Mateo',
'san_carlos' : 'San Mateo',
'san_francisco' : 'San Francisco',
'san_jose' : 'Santa Clara',
'san_leandro' : 'Alameda',
'san_mateo' : 'San Mateo',
'san_pablo' : 'Contra Costa',
'san_rafael' : 'Marin',
'san_ramon' : 'Contra Costa',
'santa_clara' : 'Santa Clara',
'santa_rosa' : 'Sonoma',
'saratoga' : 'Santa Clara',
'sausalito' : 'Marin',
'sebastopol' : 'Sonoma',
'sonoma' : 'Sonoma',
'south_san_francisco' : 'San Mateo',
'suisun_city' : 'Solano',
'sunnyvale' : 'Santa Clara',
'tiburon' : 'Marin',
'union_city' : 'Alameda',
'vacaville' : 'Solano',
'vallejo' : 'Solano',
'walnut_creek' : 'Contra Costa',
'windsor' : 'Sonoma',
'woodside' : 'San Mateo',
'yountville' : 'Napa',
'unincorporated_alameda' : "Alameda",
'unincorporated_contra_costa' : "Contra Costa",
'unincorporated_marin' : "Marin",
'unincorporated_napa' : "Napa",
'unincorporated_san_francisco' : "San Francisco",
'unincorporated_san_mateo' : "San Mateo",
'unincorporated_santa_clara' : "Santa Clara",
'unincorporated_solano' : "Solano",
'unincorporated_sonoma' : "Sonoma"}


#calculate growth at the regional level for main variables using taz summaries
def county_calculator(run_num, DF1, DF2):
    if ('zone_id' in DF1.columns) & ('zone_id' in DF2.columns):
        DF1.rename(columns={'zone_id': 'TAZ'}, inplace=True)
        DF2.rename(columns={'zone_id': 'TAZ'}, inplace=True)    
        
    if ('TAZ' in DF1.columns) & ('TAZ' in DF2.columns):
        DF_merge = DF1.merge(DF2, on = 'TAZ').fillna(0)
        DF_merge['TOTPOP GROWTH'] = DF_merge['TOTPOP_y']-DF_merge['TOTPOP_x']
        DF_merge['TOTEMP GROWTH'] = DF_merge['TOTEMP_y']-DF_merge['TOTEMP_x']
        DF_merge['AGREMPN GROWTH'] = DF_merge['AGREMPN_y']-DF_merge['AGREMPN_x']
        DF_merge['FPSEMPN GROWTH'] = DF_merge['FPSEMPN_y']-DF_merge['FPSEMPN_x']
        DF_merge['HEREMPN GROWTH'] = DF_merge['HEREMPN_y']-DF_merge['HEREMPN_x']
        DF_merge['MWTEMPN GROWTH'] = DF_merge['MWTEMPN_y']-DF_merge['MWTEMPN_x']
        DF_merge['OTHEMPN GROWTH'] = DF_merge['OTHEMPN_y']-DF_merge['OTHEMPN_x']
        DF_merge['RETEMPN GROWTH'] = DF_merge['RETEMPN_y']-DF_merge['RETEMPN_x']
        DF_merge['TOTHH GROWTH'] = DF_merge['TOTHH_y']-DF_merge['TOTHH_x']
        DF_merge['HHINCQ1 GROWTH'] = DF_merge['HHINCQ1_y']-DF_merge['HHINCQ1_x']
        DF_merge['HHINCQ2 GROWTH'] = DF_merge['HHINCQ2_y']-DF_merge['HHINCQ2_x']
        DF_merge['HHINCQ3 GROWTH'] = DF_merge['HHINCQ3_y']-DF_merge['HHINCQ3_x']
        DF_merge['HHINCQ4 GROWTH'] = DF_merge['HHINCQ4_y']-DF_merge['HHINCQ4_x']
                   
        DF_COLUMNS = ['COUNTY_NAME_x',
                      'TOTPOP GROWTH',
                      'TOTEMP GROWTH',
                      'AGREMPN GROWTH',
                      'FPSEMPN GROWTH',
                      'HEREMPN GROWTH',
                      'MWTEMPN GROWTH',
                      'OTHEMPN GROWTH',
                      'RETEMPN GROWTH',
                      'TOTHH GROWTH',
                      'HHINCQ1 GROWTH',
                      'HHINCQ2 GROWTH',
                      'HHINCQ3 GROWTH',
                      'HHINCQ4 GROWTH']
        
        DF_TAZ_GROWTH = DF_merge[DF_COLUMNS].copy()
        DF_COUNTY_GROWTH = DF_TAZ_GROWTH.groupby(['COUNTY_NAME_x']).sum().reset_index()
        DF_COUNTY_GROWTH['RUNID'] = run_num
        #add county mapping
        return DF_COUNTY_GROWTH
    else:
        print ('Merge cannot be performed')

#calculate the growth between 2015 and 2050 for taz summaries
def taz_calculator(run_num, DF1, DF2):
    #PBA40 has a couple of different columns
    if ('total_residential_units' in DF1.columns) & ('total_residential_units' in DF2.columns):
        DF1.rename(columns={'total_residential_units': 'RES_UNITS'}, inplace=True)
        DF2.rename(columns={'total_residential_units': 'RES_UNITS'}, inplace=True)
        
    if ('zone_id' in DF1.columns) & ('zone_id' in DF2.columns):
        DF1.rename(columns={'zone_id': 'TAZ'}, inplace=True)
        DF2.rename(columns={'zone_id': 'TAZ'}, inplace=True)    
        
    if ('TAZ' in DF1.columns) & ('TAZ' in DF2.columns):
        DF_merge = DF1.merge(DF2, on = 'TAZ').fillna(0)
        DF_merge['AGREMPN GROWTH'] = DF_merge['AGREMPN_y']-DF_merge['AGREMPN_x']
        DF_merge['FPSEMPN GROWTH'] = DF_merge['FPSEMPN_y']-DF_merge['FPSEMPN_x']
        DF_merge['HEREMPN GROWTH'] = DF_merge['HEREMPN_y']-DF_merge['HEREMPN_x']
        DF_merge['MWTEMPN GROWTH'] = DF_merge['MWTEMPN_y']-DF_merge['MWTEMPN_x']
        DF_merge['OTHEMPN GROWTH'] = DF_merge['OTHEMPN_y']-DF_merge['OTHEMPN_x']
        DF_merge['RETEMPN GROWTH'] = DF_merge['RETEMPN_y']-DF_merge['RETEMPN_x']
        DF_merge['TOTEMP GROWTH'] = DF_merge['TOTEMP_y']-DF_merge['TOTEMP_x']
        DF_merge['HHINCQ1 GROWTH'] = DF_merge['HHINCQ1_y']-DF_merge['HHINCQ1_x']
        DF_merge['HHINCQ2 GROWTH'] = DF_merge['HHINCQ2_y']-DF_merge['HHINCQ2_x']
        DF_merge['HHINCQ3 GROWTH'] = DF_merge['HHINCQ3_y']-DF_merge['HHINCQ3_x']
        DF_merge['HHINCQ4 GROWTH'] = DF_merge['HHINCQ4_y']-DF_merge['HHINCQ4_x']
        #DF_merge['HHPOP GROWTH'] = DF_merge['HHPOP_y']-DF_merge['HHPOP_x']
        DF_merge['TOTHH GROWTH'] = DF_merge['TOTHH_y']-DF_merge['TOTHH_x']
        #DF_merge['SHPOP62P GROWTH'] = DF_merge['SHPOP62P_y']-DF_merge['SHPOP62P_x']
        #DF_merge['GQPOP GROWTH'] = DF_merge['GQPOP_y']-DF_merge['GQPOP_x']
        DF_merge['TOTPOP GROWTH'] = DF_merge['TOTPOP_y']-DF_merge['TOTPOP_x']
        DF_merge['RES_UNITS GROWTH'] = DF_merge['RES_UNITS_y']-DF_merge['RES_UNITS_x']
        DF_merge['MFDU GROWTH'] = DF_merge['MFDU_y']-DF_merge['MFDU_x']
        DF_merge['SFDU GROWTH'] = DF_merge['SFDU_y']-DF_merge['SFDU_x']
        #DF_merge['RESACRE GROWTH'] = DF_merge['RESACRE_y']-DF_merge['RESACRE_x']
        #DF_merge['EMPRES GROWTH'] = DF_merge['EMPRES_y']-DF_merge['EMPRES_x']
        #DF_merge['AGE0004 GROWTH'] = DF_merge['AGE0004_y']-DF_merge['AGE0004_x']
        #DF_merge['AGE0519 GROWTH'] = DF_merge['AGE0519_y']-DF_merge['AGE0519_x']
        #DF_merge['AGE2044 GROWTH'] = DF_merge['AGE2044_y']-DF_merge['AGE2044_x']
        #DF_merge['AGE4564 GROWTH'] = DF_merge['AGE4564_y']-DF_merge['AGE4564_x']
        #DF_merge['AGE65P GROWTH'] = DF_merge['AGE65P_y']-DF_merge['AGE65P_x']
            
        TAZ_DF_COLUMNS = ['TAZ',
                         'SD_x',
                         'ZONE_x',
                         'COUNTY_x',
                         'AGREMPN GROWTH',
                         'FPSEMPN GROWTH',
                         'HEREMPN GROWTH',
                         'MWTEMPN GROWTH',
                         'OTHEMPN GROWTH',
                         'RETEMPN GROWTH',
                         'TOTEMP GROWTH',
                         'HHINCQ1 GROWTH',
                         'HHINCQ2 GROWTH',
                         'HHINCQ3 GROWTH',
                         'HHINCQ4 GROWTH',
                         #'HHPOP GROWTH',
                         'TOTHH GROWTH',
                         #'SHPOP62P GROWTH',
                         #'GQPOP GROWTH',
                         'TOTPOP GROWTH',
                         'RES_UNITS GROWTH',
                         'MFDU GROWTH',
                         'SFDU GROWTH'
                         #'RESACRE GROWTH',
                         #'EMPRES GROWTH',
                         #'AGE0004 GROWTH',
                         #'AGE0519 GROWTH',
                         #'AGE2044 GROWTH',
                         #'AGE4564 GROWTH',
                         #'AGE65P GROWTH'
                         ]
        
        DF_TAZ_GROWTH = DF_merge[TAZ_DF_COLUMNS].copy()
        DF_TAZ_GROWTH['RUNID'] = run_num
        return DF_TAZ_GROWTH
    else:
        print ('Merge cannot be performed')

#A separate calculator for juris, pda, and superdistrct summaries, because they have different columns
def nontaz_calculator(run_num, DF1, DF2):
    
    DF_COLUMNS = ['agrempn growth',
                  'fpsempn growth',
                  'herempn growth',
                  'mwtempn growth',
                  'othempn growth',
                  'retempn growth',
                  'totemp growth',
                  'hhincq1 growth',
                  'hhincq2 growth',
                  'hhincq3 growth',
                  'hhincq4 growth',
                  'tothh growth',
                  'mfdu growth',
                  'sfdu growth',
                  #'occupancy_rate growth',
                  'non_residential_sqft growth',
                  'deed_restricted_units growth',
                  'inclusionary_units growth',
                  'subsidized_units growth',
                  'preserved_units growth']
    
    if ('juris' in DF1.columns) & ('juris' in DF2.columns):
        DF_merge = DF1.merge(DF2, on = 'juris').fillna(0)
        DF_COLUMNS = ['juris'] + DF_COLUMNS
    elif ('superdistrict' in DF1.columns) & ('superdistrict' in DF2.columns):
        DF_merge = DF1.merge(DF2, on = 'superdistrict').fillna(0)
        DF_COLUMNS = ['superdistrict'] + DF_COLUMNS
    else:
        print ('Merge cannot be performed')
        
    DF_merge['agrempn growth'] = DF_merge['agrempn_y']-DF_merge['agrempn_x']
    DF_merge['fpsempn growth'] = DF_merge['fpsempn_y']-DF_merge['fpsempn_x']
    DF_merge['herempn growth'] = DF_merge['herempn_y']-DF_merge['herempn_x']
    DF_merge['mwtempn growth'] = DF_merge['mwtempn_y']-DF_merge['mwtempn_x']
    DF_merge['othempn growth'] = DF_merge['othempn_y']-DF_merge['othempn_x']
    DF_merge['retempn growth'] = DF_merge['retempn_y']-DF_merge['retempn_x']
    DF_merge['totemp growth'] = DF_merge['totemp_y']-DF_merge['totemp_x']
    DF_merge['hhincq1 growth'] = DF_merge['hhincq1_y']-DF_merge['hhincq1_x']
    DF_merge['hhincq2 growth'] = DF_merge['hhincq2_y']-DF_merge['hhincq2_x']
    DF_merge['hhincq3 growth'] = DF_merge['hhincq3_y']-DF_merge['hhincq3_x']
    DF_merge['hhincq4 growth'] = DF_merge['hhincq4_y']-DF_merge['hhincq4_x']
    DF_merge['tothh growth'] = DF_merge['tothh_y']-DF_merge['tothh_x']
    DF_merge['mfdu growth'] = DF_merge['mfdu_y']-DF_merge['mfdu_x']
    DF_merge['sfdu growth'] = DF_merge['sfdu_y']-DF_merge['sfdu_x']
    #DF_merge['occupancy_rate growth'] = DF_merge['occupancy_rate_y']-DF_merge['occupancy_rate_x']
    DF_merge['non_residential_sqft growth'] = DF_merge['non_residential_sqft_y']-DF_merge['non_residential_sqft_x']
    DF_merge['deed_restricted_units growth'] = DF_merge['deed_restricted_units_y']-DF_merge['deed_restricted_units_x']
    DF_merge['inclusionary_units growth'] = DF_merge['inclusionary_units_y']-DF_merge['inclusionary_units_x']
    DF_merge['subsidized_units growth'] = DF_merge['subsidized_units_y']-DF_merge['subsidized_units_x']
    DF_merge['preserved_units growth'] = DF_merge['preserved_units_y']-DF_merge['preserved_units_x']
        
    DF_GROWTH = DF_merge[DF_COLUMNS].copy()
    DF_GROWTH['RUNID'] = run_num
    return DF_GROWTH


#This is to define a separate fileloader for parcel difference data. With the zoningmod category, we should be able to
#summarize growth by different geography types that is more nuanced.
def GEO_SUMMARY_LOADER(run_num, geo, parcel_baseyear, parcel_endyear):

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','juris','fbpchcat']]
    parcel_data = parcel_baseyear.merge(parcel_endyear, on = 'parcel_id', how = 'left').fillna(0)
    
    parcel_data['totemp diff'] = parcel_data['totemp_y']-parcel_data['totemp_x']
    parcel_data['tothh diff'] = parcel_data['tothh_y']-parcel_data['tothh_x']
    parcel_data['hhq1 diff'] = parcel_data['hhq1_y']-parcel_data['hhq1_x']
    parcel_data['hhq2 diff'] = parcel_data['hhq2_y']-parcel_data['hhq2_x']
    parcel_data['hhq3 diff'] = parcel_data['hhq3_y']-parcel_data['hhq3_x']
    parcel_data['hhq4 diff'] = parcel_data['hhq4_y']-parcel_data['hhq4_x']

    parcel_data = parcel_data[['parcel_id','tothh diff','totemp diff','hhq1 diff','hhq2 diff','hhq3 diff','hhq4 diff','juris','fbpchcat']].copy()
        
    #geography summaries
    parcel_geo = parcel_data.loc[parcel_data['fbpchcat'].str.contains(geo, na=False)]
    parcel_geo = parcel_geo.groupby(['juris']).agg({'tothh diff':'sum','totemp diff':'sum','hhq1 diff':'sum', 'hhq2 diff':'sum','hhq3 diff':'sum', 'hhq4 diff':'sum', }).reset_index()
    parcel_geo['geo_category'] = 'yes_%s'%(geo)
    parcel_geo_no = parcel_data.loc[~parcel_data['fbpchcat'].str.contains(geo, na=False)]
    parcel_geo_no = parcel_geo_no.groupby(['juris']).agg({'tothh diff':'sum','totemp diff':'sum','hhq1 diff':'sum', 'hhq2 diff':'sum','hhq3 diff':'sum', 'hhq4 diff':'sum', }).reset_index()
    parcel_geo_no['geo_category'] = 'no_%s'%(geo)
    
    parcel_geo_summary = pd.concat([parcel_geo, parcel_geo_no], ignore_index = True)
    dropindex = parcel_geo_summary[parcel_geo_summary['juris'] == 0].index
    parcel_geo_summary.drop(dropindex,inplace = True)
    parcel_geo_summary.sort_values(by = 'juris', inplace = True)
    parcel_geo_summary['RUNID'] = run_num
    
    return parcel_geo_summary


##Similar to above, this is to define a separate fileloader to produce summaries for overlapping geographies. W
def TWO_GEO_SUMMARY_LOADER(run_num, geo1, geo2, parcel_baseyear, parcel_endyear):

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','juris','fbpchcat']]
    parcel_data = parcel_baseyear.merge(parcel_endyear, on = 'parcel_id', how = 'left').fillna(0)
    
    parcel_data['totemp diff'] = parcel_data['totemp_y']-parcel_data['totemp_x']
    parcel_data['tothh diff'] = parcel_data['tothh_y']-parcel_data['tothh_x']
    parcel_data['hhq1 diff'] = parcel_data['hhq1_y']-parcel_data['hhq1_x']
    parcel_data['hhq2 diff'] = parcel_data['hhq2_y']-parcel_data['hhq2_x']
    parcel_data['hhq3 diff'] = parcel_data['hhq3_y']-parcel_data['hhq3_x']
    parcel_data['hhq4 diff'] = parcel_data['hhq4_y']-parcel_data['hhq4_x']

    parcel_data = parcel_data[['parcel_id','tothh diff','totemp diff','hhq1 diff','hhq2 diff','hhq3 diff','hhq4 diff','juris','fbpchcat']].copy()
    
    #two geographies
    parcel_geo2 = parcel_data.loc[parcel_data['fbpchcat'].str.contains(geo1, na=False)]
    parcel_geo2 = parcel_geo2.loc[parcel_geo2['fbpchcat'].str.contains(geo2, na=False)]
    parcel_geo2_group = parcel_geo2.groupby(['juris']).agg({'tothh diff':'sum','totemp diff':'sum','hhq1 diff':'sum', 'hhq2 diff':'sum','hhq3 diff':'sum', 'hhq4 diff':'sum', }).reset_index()
    parcel_geo2_group['geo_category'] = 'yes_%s'%(geo1+geo2)
    
    parcel_geo2_no_1 = parcel_data.loc[parcel_data['fbpchcat'].str.contains(geo1, na=False)]
    parcel_geo2_no_1 = parcel_geo2_no_1.loc[~parcel_geo2_no_1['fbpchcat'].str.contains(geo2, na=False)]
    parcel_geo2_no_2 = parcel_data.loc[parcel_data['fbpchcat'].str.contains(geo2, na=False)]
    parcel_geo2_no_2 = parcel_geo2_no_2.loc[~parcel_geo2_no_2['fbpchcat'].str.contains(geo1, na=False)]
    parcel_geo2_no_3 = parcel_data.loc[~parcel_data['fbpchcat'].str.contains(geo1 + "|" + geo2, na=False)]
    
    parcel_geo2_no = pd.concat([parcel_geo2_no_1, parcel_geo2_no_2, parcel_geo2_no_3], ignore_index = True)
    parcel_geo2_no_group = parcel_geo2_no.groupby(['juris']).agg({'tothh diff':'sum','totemp diff':'sum','hhq1 diff':'sum', 'hhq2 diff':'sum','hhq3 diff':'sum', 'hhq4 diff':'sum', }).reset_index()
    parcel_geo2_no_group['geo_category'] = 'no_%s'%(geo1+geo2)
    
    parcel_geo2_summary = pd.concat([parcel_geo2_group, parcel_geo2_no_group], ignore_index = True)
    dropindex = parcel_geo2_summary[parcel_geo2_summary['juris'] == 0].index
    parcel_geo2_summary.drop(dropindex,inplace = True)
    parcel_geo2_summary.sort_values(by = 'juris', inplace = True)
    parcel_geo2_summary['RUNID'] = run_num
    
    return parcel_geo2_summary
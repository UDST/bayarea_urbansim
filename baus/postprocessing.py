
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

        DF_CO_GRWTH = DF_merge.groupby(['COUNTY_NAME_x']).sum().reset_index()

        DF_CO_GRWTH['TOTPOP GROWTH SHR'] = round(DF_CO_GRWTH['TOTPOP GROWTH']/(DF_CO_GRWTH['TOTPOP_y'].sum()-DF_CO_GRWTH['TOTPOP_x'].sum()), 2)
        DF_CO_GRWTH['TOTEMP GROWTH SHR'] = round(DF_CO_GRWTH['TOTEMP GROWTH']/(DF_CO_GRWTH['TOTEMP_y'].sum()-DF_CO_GRWTH['TOTEMP_x'].sum()), 2)
        DF_CO_GRWTH['AGREMPN GROWTH SHR'] = round(DF_CO_GRWTH['AGREMPN GROWTH']/(DF_CO_GRWTH['AGREMPN_y'].sum()-DF_CO_GRWTH['AGREMPN_x'].sum()), 2)
        DF_CO_GRWTH['FPSEMPN GROWTH SHR'] = round(DF_CO_GRWTH['FPSEMPN GROWTH']/(DF_CO_GRWTH['FPSEMPN_y'].sum()-DF_CO_GRWTH['FPSEMPN_x'].sum()), 2)
        DF_CO_GRWTH['HEREMPN GROWTH SHR'] = round(DF_CO_GRWTH['HEREMPN GROWTH']/(DF_CO_GRWTH['HEREMPN_y'].sum()-DF_CO_GRWTH['HEREMPN_x'].sum()), 2)
        DF_CO_GRWTH['MWTEMPN GROWTH SHR'] = round(DF_CO_GRWTH['MWTEMPN GROWTH']/(DF_CO_GRWTH['MWTEMPN_y'].sum()-DF_CO_GRWTH['MWTEMPN_x'].sum()), 2)
        DF_CO_GRWTH['OTHEMPN GROWTH SHR'] = round(DF_CO_GRWTH['OTHEMPN GROWTH']/(DF_CO_GRWTH['OTHEMPN_y'].sum()-DF_CO_GRWTH['OTHEMPN_x'].sum()), 2)
        DF_CO_GRWTH['RETEMPN GROWTH SHR'] = round(DF_CO_GRWTH['RETEMPN GROWTH']/(DF_CO_GRWTH['RETEMPN_y'].sum()-DF_CO_GRWTH['RETEMPN_x'].sum()), 2)
        DF_CO_GRWTH['TOTHH GROWTH SHR'] = round(DF_CO_GRWTH['TOTHH GROWTH']/(DF_CO_GRWTH['TOTHH_y'].sum()-DF_CO_GRWTH['TOTHH_x'].sum()), 2)
        DF_CO_GRWTH['HHINCQ1 GROWTH SHR'] = round(DF_CO_GRWTH['HHINCQ1 GROWTH']/(DF_CO_GRWTH['HHINCQ1_y'].sum()-DF_CO_GRWTH['HHINCQ1_x'].sum()), 2)
        DF_CO_GRWTH['HHINCQ2 GROWTH SHR'] = round(DF_CO_GRWTH['HHINCQ2 GROWTH']/(DF_CO_GRWTH['HHINCQ2_y'].sum()-DF_CO_GRWTH['HHINCQ2_x'].sum()), 2)
        DF_CO_GRWTH['HHINCQ3 GROWTH SHR'] = round(DF_CO_GRWTH['HHINCQ3 GROWTH']/(DF_CO_GRWTH['HHINCQ3_y'].sum()-DF_CO_GRWTH['HHINCQ3_x'].sum()), 2)
        DF_CO_GRWTH['HHINCQ4 GROWTH SHR'] = round(DF_CO_GRWTH['HHINCQ4 GROWTH']/(DF_CO_GRWTH['HHINCQ4_y'].sum()-DF_CO_GRWTH['HHINCQ4_x'].sum()), 2)

        DF_CO_GRWTH['TOTPOP PCT GROWTH'] = round(DF_CO_GRWTH['TOTPOP_y']/DF_CO_GRWTH['TOTPOP_x']-1, 2)
        DF_CO_GRWTH['TOTEMP PCT GROWTH'] = round(DF_CO_GRWTH['TOTEMP_y']/DF_CO_GRWTH['TOTEMP_x']-1, 2)
        DF_CO_GRWTH['AGREMPN PCT GROWTH'] = round(DF_CO_GRWTH['AGREMPN_y']/DF_CO_GRWTH['AGREMPN_x']-1, 2)
        DF_CO_GRWTH['FPSEMPN PCT GROWTH'] = round(DF_CO_GRWTH['FPSEMPN_y']/DF_CO_GRWTH['FPSEMPN_x']-1, 2)
        DF_CO_GRWTH['HEREMPN PCT GROWTH'] = round(DF_CO_GRWTH['HEREMPN_y']/DF_CO_GRWTH['HEREMPN_x']-1, 2)
        DF_CO_GRWTH['MWTEMPN PCT GROWTH'] = round(DF_CO_GRWTH['MWTEMPN_y']/DF_CO_GRWTH['MWTEMPN_x']-1, 2)
        DF_CO_GRWTH['OTHEMPN PCT GROWTH'] = round(DF_CO_GRWTH['OTHEMPN_y']/DF_CO_GRWTH['OTHEMPN_x']-1, 2)
        DF_CO_GRWTH['RETEMPN PCT GROWTH'] = round(DF_CO_GRWTH['RETEMPN_y']/DF_CO_GRWTH['RETEMPN_x']-1, 2)
        DF_CO_GRWTH['TOTHH PCT GROWTH'] = round(DF_CO_GRWTH['TOTHH_y']/DF_CO_GRWTH['TOTHH_x']-1, 2)
        DF_CO_GRWTH['HHINCQ1 PCT GROWTH'] = round(DF_CO_GRWTH['HHINCQ1_y']/DF_CO_GRWTH['HHINCQ1_x']-1, 2)
        DF_CO_GRWTH['HHINCQ2 PCT GROWTH'] = round(DF_CO_GRWTH['HHINCQ2_y']/DF_CO_GRWTH['HHINCQ2_x']-1, 2)
        DF_CO_GRWTH['HHINCQ3 PCT GROWTH'] = round(DF_CO_GRWTH['HHINCQ3_y']/DF_CO_GRWTH['HHINCQ3_x']-1, 2)
        DF_CO_GRWTH['HHINCQ4 PCT GROWTH'] = round(DF_CO_GRWTH['HHINCQ4_y']/DF_CO_GRWTH['HHINCQ4_x']-1, 2)

        DF_CO_GRWTH['TOTPOP SHR CHNG'] = round(DF_CO_GRWTH['TOTPOP_y']/DF_CO_GRWTH['TOTPOP_y'].sum()-DF_CO_GRWTH['TOTPOP_x']/DF_CO_GRWTH['TOTPOP_x'].sum(), 2)
        DF_CO_GRWTH['TOTEMP SHR CHNG'] = round(DF_CO_GRWTH['TOTEMP_y']/DF_CO_GRWTH['TOTEMP_y'].sum()-DF_CO_GRWTH['TOTEMP_x']/DF_CO_GRWTH['TOTEMP_x'].sum(), 2)
        DF_CO_GRWTH['AGREMPN SHR CHNG'] = round(DF_CO_GRWTH['AGREMPN_y']/DF_CO_GRWTH['AGREMPN_y'].sum()-DF_CO_GRWTH['AGREMPN_x']/DF_CO_GRWTH['AGREMPN_x'].sum(), 2)
        DF_CO_GRWTH['FPSEMPN SHR CHNG'] = round(DF_CO_GRWTH['FPSEMPN_y']/DF_CO_GRWTH['FPSEMPN_y'].sum()-DF_CO_GRWTH['FPSEMPN_x']/DF_CO_GRWTH['FPSEMPN_x'].sum(), 2)
        DF_CO_GRWTH['HEREMPN SHR CHNG'] = round(DF_CO_GRWTH['HEREMPN_y']/DF_CO_GRWTH['HEREMPN_y'].sum()-DF_CO_GRWTH['HEREMPN_x']/DF_CO_GRWTH['HEREMPN_x'].sum(), 2)
        DF_CO_GRWTH['MWTEMPN SHR CHNG'] = round(DF_CO_GRWTH['MWTEMPN_y']/DF_CO_GRWTH['MWTEMPN_y'].sum()-DF_CO_GRWTH['MWTEMPN_x']/DF_CO_GRWTH['MWTEMPN_x'].sum(), 2)
        DF_CO_GRWTH['OTHEMPN SHR CHNG'] = round(DF_CO_GRWTH['OTHEMPN_y']/DF_CO_GRWTH['OTHEMPN_y'].sum()-DF_CO_GRWTH['OTHEMPN_x']/DF_CO_GRWTH['OTHEMPN_x'].sum(), 2)
        DF_CO_GRWTH['RETEMPN SHR CHNG'] = round(DF_CO_GRWTH['RETEMPN_y']/DF_CO_GRWTH['RETEMPN_y'].sum()-DF_CO_GRWTH['RETEMPN_x']/DF_CO_GRWTH['RETEMPN_x'].sum(), 2)
        DF_CO_GRWTH['TOTHH SHR CHNG'] = round(DF_CO_GRWTH['TOTHH_y']/DF_CO_GRWTH['TOTHH_y'].sum()-DF_CO_GRWTH['TOTHH_x']/DF_CO_GRWTH['TOTHH_x'].sum(), 2)
        DF_CO_GRWTH['HHINCQ1 SHR CHNG'] = round(DF_CO_GRWTH['HHINCQ1_y']/DF_CO_GRWTH['HHINCQ1_y'].sum()-DF_CO_GRWTH['HHINCQ1_x']/DF_CO_GRWTH['HHINCQ1_x'].sum(), 2)
        DF_CO_GRWTH['HHINCQ2 SHR CHNG'] = round(DF_CO_GRWTH['HHINCQ2_y']/DF_CO_GRWTH['HHINCQ2_y'].sum()-DF_CO_GRWTH['HHINCQ2_x']/DF_CO_GRWTH['HHINCQ2_x'].sum(), 2)
        DF_CO_GRWTH['HHINCQ3 SHR CHNG'] = round(DF_CO_GRWTH['HHINCQ3_y']/DF_CO_GRWTH['HHINCQ3_y'].sum()-DF_CO_GRWTH['HHINCQ3_x']/DF_CO_GRWTH['HHINCQ3_x'].sum(), 2)
        DF_CO_GRWTH['HHINCQ4 SHR CHNG'] = round(DF_CO_GRWTH['HHINCQ4_y']/DF_CO_GRWTH['HHINCQ4_y'].sum()-DF_CO_GRWTH['HHINCQ4_x']/DF_CO_GRWTH['HHINCQ4_x'].sum(), 2)

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
                      'HHINCQ4 GROWTH',
                      'TOTPOP GROWTH SHR',
                      'TOTEMP GROWTH SHR',
                      'AGREMPN GROWTH SHR',
                      'FPSEMPN GROWTH SHR',
                      'HEREMPN GROWTH SHR',
                      'MWTEMPN GROWTH SHR',
                      'OTHEMPN GROWTH SHR',
                      'RETEMPN GROWTH SHR',
                      'TOTHH GROWTH SHR',
                      'HHINCQ1 GROWTH SHR',
                      'HHINCQ2 GROWTH SHR',
                      'HHINCQ3 GROWTH SHR',
                      'HHINCQ4 GROWTH SHR',
                      'TOTPOP PCT GROWTH',
                      'TOTEMP PCT GROWTH',
                      'AGREMPN PCT GROWTH',
                      'FPSEMPN PCT GROWTH',
                      'HEREMPN PCT GROWTH',
                      'MWTEMPN PCT GROWTH',
                      'OTHEMPN PCT GROWTH',
                      'RETEMPN PCT GROWTH',
                      'TOTHH PCT GROWTH',
                      'HHINCQ1 PCT GROWTH',
                      'HHINCQ2 PCT GROWTH',
                      'HHINCQ3 PCT GROWTH',
                      'HHINCQ4 PCT GROWTH',
                      'TOTPOP SHR CHNG',
                      'TOTEMP SHR CHNG',
                      'AGREMPN SHR CHNG',
                      'FPSEMPN SHR CHNG',
                      'HEREMPN SHR CHNG',
                      'MWTEMPN SHR CHNG',
                      'OTHEMPN SHR CHNG',
                      'RETEMPN SHR CHNG',
                      'TOTHH SHR CHNG',
                      'HHINCQ1 SHR CHNG',
                      'HHINCQ2 SHR CHNG',
                      'HHINCQ3 SHR CHNG',
                      'HHINCQ4 SHR CHNG']

        DF_CO_GRWTH = DF_CO_GRWTH[DF_COLUMNS].copy()
        DF_CO_GRWTH = DF_CO_GRWTH.rename(columns={'COUNTY_NAME_x': 'COUNTY'})
        DF_CO_GRWTH['RUNID'] = run_num
        #add county mapping
        return DF_CO_GRWTH
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
        DF_merge['TOTHH GROWTH'] = DF_merge['TOTHH_y']-DF_merge['TOTHH_x']
        DF_merge['TOTPOP GROWTH'] = DF_merge['TOTPOP_y']-DF_merge['TOTPOP_x']
        DF_merge['RES_UNITS GROWTH'] = DF_merge['RES_UNITS_y']-DF_merge['RES_UNITS_x']
        DF_merge['MFDU GROWTH'] = DF_merge['MFDU_y']-DF_merge['MFDU_x']
        DF_merge['SFDU GROWTH'] = DF_merge['SFDU_y']-DF_merge['SFDU_x']

        DF_merge['TOTPOP GROWTH SHR'] = round(DF_merge['TOTPOP GROWTH']/(DF_merge['TOTPOP_y'].sum()-DF_merge['TOTPOP_x'].sum()), 2)
        DF_merge['TOTEMP GROWTH SHR'] = round(DF_merge['TOTEMP GROWTH']/(DF_merge['TOTEMP_y'].sum()/DF_merge['TOTEMP_x'].sum()), 2)
        DF_merge['TOTHH GROWTH SHR'] = round(DF_merge['TOTHH GROWTH']/(DF_merge['TOTHH_y'].sum()/DF_merge['TOTHH_x'].sum()), 2)

        DF_merge['TOTPOP PCT GROWTH'] = round(DF_merge['TOTPOP_y']/DF_merge['TOTPOP_x']-1, 2)
        DF_merge['TOTEMP PCT GROWTH'] = round(DF_merge['TOTEMP_y']/DF_merge['TOTEMP_x']-1, 2)
        DF_merge['TOTHH PCT GROWTH'] = round(DF_merge['TOTHH_y']/DF_merge['TOTHH_x']-1, 2)

        DF_merge['TOTPOP SHR CHNG'] = round(DF_merge['TOTPOP_y']/DF_merge['TOTPOP_y'].sum()-DF_merge['TOTPOP_x']/DF_merge['TOTPOP_x'].sum(), 2)
        DF_merge['TOTEMP SHR CHNG'] = round(DF_merge['TOTEMP_y']/DF_merge['TOTEMP_y'].sum()-DF_merge['TOTEMP_x']/DF_merge['TOTEMP_x'].sum(), 2)
        DF_merge['AGREMPN SHR CHNG'] = round(DF_merge['AGREMPN_y']/DF_merge['AGREMPN_y'].sum()-DF_merge['AGREMPN_x']/DF_merge['AGREMPN_x'].sum(), 2)
        DF_merge['FPSEMPN SHR CHNG'] = round(DF_merge['FPSEMPN_y']/DF_merge['FPSEMPN_y'].sum()-DF_merge['FPSEMPN_x']/DF_merge['FPSEMPN_x'].sum(), 2)
        DF_merge['HEREMPN SHR CHNG'] = round(DF_merge['HEREMPN_y']/DF_merge['HEREMPN_y'].sum()-DF_merge['HEREMPN_x']/DF_merge['HEREMPN_x'].sum(), 2)
        DF_merge['MWTEMPN SHR CHNG'] = round(DF_merge['MWTEMPN_y']/DF_merge['MWTEMPN_y'].sum()-DF_merge['MWTEMPN_x']/DF_merge['MWTEMPN_x'].sum(), 2)
        DF_merge['OTHEMPN SHR CHNG'] = round(DF_merge['OTHEMPN_y']/DF_merge['OTHEMPN_y'].sum()-DF_merge['OTHEMPN_x']/DF_merge['OTHEMPN_x'].sum(), 2)
        DF_merge['RETEMPN SHR CHNG'] = round(DF_merge['RETEMPN_y']/DF_merge['RETEMPN_y'].sum()-DF_merge['RETEMPN_x']/DF_merge['RETEMPN_x'].sum(), 2)
        DF_merge['TOTHH SHR CHNG'] = round(DF_merge['TOTHH_y']/DF_merge['TOTHH_y'].sum()-DF_merge['TOTHH_x']/DF_merge['TOTHH_x'].sum(), 2)
        DF_merge['HHINCQ1 SHR CHNG'] = round(DF_merge['HHINCQ1_y']/DF_merge['HHINCQ1_y'].sum()-DF_merge['HHINCQ1_x']/DF_merge['HHINCQ1_x'].sum(), 2)
        DF_merge['HHINCQ2 SHR CHNG'] = round(DF_merge['HHINCQ2_y']/DF_merge['HHINCQ2_y'].sum()-DF_merge['HHINCQ2_x']/DF_merge['HHINCQ2_x'].sum(), 2)
        DF_merge['HHINCQ3 SHR CHNG'] = round(DF_merge['HHINCQ3_y']/DF_merge['HHINCQ3_y'].sum()-DF_merge['HHINCQ3_x']/DF_merge['HHINCQ3_x'].sum(), 2)
        DF_merge['HHINCQ4 SHR CHNG'] = round(DF_merge['HHINCQ4_y']/DF_merge['HHINCQ4_y'].sum()-DF_merge['HHINCQ4_x']/DF_merge['HHINCQ4_x'].sum(), 2)
        DF_merge['RES_UNITS SHR CHNG'] = round(DF_merge['RES_UNITS_y']/DF_merge['RES_UNITS_y'].sum()-DF_merge['RES_UNITS_x']/DF_merge['RES_UNITS_x'].sum(), 2)
        DF_merge['MFDU SHR CHNG'] = round(DF_merge['MFDU_y']/DF_merge['MFDU_y'].sum()-DF_merge['MFDU_x']/DF_merge['MFDU_x'].sum(), 2)
        DF_merge['SFDU SHR CHNG'] = round(DF_merge['SFDU_y']/DF_merge['SFDU_y'].sum()-DF_merge['SFDU_x']/DF_merge['SFDU_x'].sum(), 2)

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
                         'TOTHH GROWTH',
                         'TOTPOP GROWTH',
                         'RES_UNITS GROWTH',
                         'MFDU GROWTH',
                         'SFDU GROWTH',
                         'TOTPOP GROWTH SHR',
                         'TOTEMP GROWTH SHR',
                         'TOTHH GROWTH SHR',
                         'TOTPOP PCT GROWTH',
                      	 'TOTEMP PCT GROWTH',
                         'AGREMPN SHR CHNG',
                         'FPSEMPN SHR CHNG',
                         'HEREMPN SHR CHNG',
                         'MWTEMPN SHR CHNG',
                         'OTHEMPN SHR CHNG',
                         'RETEMPN SHR CHNG',
                         'TOTEMP SHR CHNG',
                         'HHINCQ1 SHR CHNG',
                         'HHINCQ2 SHR CHNG',
                         'HHINCQ3 SHR CHNG',
                         'HHINCQ4 SHR CHNG',
                         'TOTHH SHR CHNG',
                         'TOTPOP SHR CHNG',
                         'RES_UNITS SHR CHNG',
                         'MFDU SHR CHNG',
                         'SFDU SHR CHNG']
        
        DF_TAZ_GROWTH = DF_merge[TAZ_DF_COLUMNS].copy()
        DF_TAZ_GROWTH = DF_TAZ_GROWTH.rename(columns={'SD_x': 'SD', 'ZONE_x': 'ZONE', 'COUNTY_x': 'COUNTY'})
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
                  'nonres_sqft growth',
                  'res_units growth',
                  'dr_units growth',
                  'incl_units growth',
                  'subsd_units growth',
                  'presrv_units growth',
                  'totemp growth shr',
                  'tothh growth shr',
                  'totemp pct growth',
                  'tothh pct growth',
                  'totemp shr chng',
                  'tothh shr chng',
                  'agrempn shr chng',
                  'fpsempn shr chng',
                  'herempn shr chng',
                  'mwtempn shr chng',
                  'othempn shr chng',
                  'retempn shr chng',
                  'totemp shr chng',
                  'hhincq1 shr chng',
                  'hhincq2 shr chng',
                  'hhincq3 shr chng',
                  'hhincq4 shr chng',
                  'tothh shr chng',
                  'mfdu shr chng',
                  'sfdu shr chng',
                  'res_units shr chng',
                  'nonres_sqft shr chng',
                  'dr_units shr chng',
                  'incl_units shr chng',
                  'subsd_units shr chng',
                  'presrv_units shr chng']
    
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
    DF_merge['nonres_sqft growth'] = DF_merge['non_residential_sqft_y']-DF_merge['non_residential_sqft_x']
    DF_merge['res_units growth'] = DF_merge['res_units_y']-DF_merge['res_units_x']
    DF_merge['dr_units growth'] = DF_merge['deed_restricted_units_y']-DF_merge['deed_restricted_units_x']
    DF_merge['incl_units growth'] = DF_merge['inclusionary_units_y']-DF_merge['inclusionary_units_x']
    DF_merge['subsd_units growth'] = DF_merge['subsidized_units_y']-DF_merge['subsidized_units_x']
    DF_merge['presrv_units growth'] = DF_merge['preserved_units_y']-DF_merge['preserved_units_x']

    DF_merge['totemp growth shr'] = round(DF_merge['totemp growth']/(DF_merge['totemp_y'].sum()-DF_merge['totemp_x'].sum()), 2)
    DF_merge['tothh growth shr'] = round(DF_merge['tothh growth']/(DF_merge['tothh_y'].sum()-DF_merge['tothh_x'].sum()), 2)

    DF_merge['totemp pct growth'] = round(DF_merge['totemp_y']/DF_merge['totemp_x']-1, 2)
    DF_merge['tothh pct growth'] = round(DF_merge['tothh_y']/DF_merge['tothh_x']-1, 2)
    DF_merge['totemp shr chng'] = round(DF_merge['totemp_y']/DF_merge['totemp_y'].sum()-DF_merge['totemp_x']/DF_merge['totemp_x'].sum(), 2)
    DF_merge['tothh shr chng'] = round(DF_merge['tothh_y']/DF_merge['tothh_y'].sum()-DF_merge['tothh_x']/DF_merge['tothh_x'].sum(), 2)
    DF_merge['agrempn shr chng'] = round(DF_merge['agrempn_y']/DF_merge['agrempn_y'].sum()-DF_merge['agrempn_x']/DF_merge['agrempn_x'].sum(), 2)
    DF_merge['fpsempn shr chng'] = round(DF_merge['fpsempn_y']/DF_merge['fpsempn_y'].sum()-DF_merge['fpsempn_x']/DF_merge['fpsempn_x'].sum(), 2)
    DF_merge['herempn shr chng'] = round(DF_merge['herempn_y']/DF_merge['herempn_y'].sum()-DF_merge['herempn_x']/DF_merge['herempn_x'].sum(), 2)
    DF_merge['mwtempn shr chng'] = round(DF_merge['mwtempn_y']/DF_merge['mwtempn_y'].sum()-DF_merge['mwtempn_x']/DF_merge['mwtempn_x'].sum(), 2)
    DF_merge['othempn shr chng'] = round(DF_merge['othempn_y']/DF_merge['othempn_y'].sum()-DF_merge['othempn_x']/DF_merge['othempn_x'].sum(), 2)
    DF_merge['retempn shr chng'] = round(DF_merge['retempn_y']/DF_merge['retempn_y'].sum()-DF_merge['retempn_x']/DF_merge['retempn_x'].sum(), 2)
    DF_merge['hhincq1 shr chng'] = round(DF_merge['hhincq1_y']/DF_merge['hhincq1_y'].sum()-DF_merge['hhincq1_x']/DF_merge['hhincq1_x'].sum(), 2)
    DF_merge['hhincq2 shr chng'] = round(DF_merge['hhincq2_y']/DF_merge['hhincq2_y'].sum()-DF_merge['hhincq2_x']/DF_merge['hhincq2_x'].sum(), 2)
    DF_merge['hhincq3 shr chng'] = round(DF_merge['hhincq3_y']/DF_merge['hhincq3_y'].sum()-DF_merge['hhincq3_x']/DF_merge['hhincq3_x'].sum(), 2)
    DF_merge['hhincq4 shr chng'] = round(DF_merge['hhincq4_y']/DF_merge['hhincq4_y'].sum()-DF_merge['hhincq4_x']/DF_merge['hhincq4_x'].sum(), 2)
    DF_merge['res_units shr chng'] = round(DF_merge['res_units_y']/DF_merge['res_units_y'].sum()-DF_merge['res_units_x']/DF_merge['res_units_x'].sum(), 2)
    DF_merge['mfdu shr chng'] = round(DF_merge['mfdu_y']/DF_merge['mfdu_y'].sum()-DF_merge['mfdu_x']/DF_merge['mfdu_x'].sum(), 2)
    DF_merge['sfdu shr chng'] = round(DF_merge['sfdu_y']/DF_merge['sfdu_y'].sum()-DF_merge['sfdu_x']/DF_merge['sfdu_x'].sum(), 2)
    DF_merge['nonres_sqft shr chng'] = round(DF_merge['non_residential_sqft_y']/DF_merge['non_residential_sqft_y'].sum()-DF_merge['non_residential_sqft_x']/DF_merge['non_residential_sqft_x'].sum(), 2)
    DF_merge['dr_units shr chng'] = round(DF_merge['deed_restricted_units_y']/DF_merge['deed_restricted_units_y'].sum()-DF_merge['deed_restricted_units_x']/DF_merge['deed_restricted_units_x'].sum(), 2)
    DF_merge['incl_units shr chng'] = round(DF_merge['inclusionary_units_y']/DF_merge['inclusionary_units_y'].sum()-DF_merge['inclusionary_units_x']/DF_merge['inclusionary_units_x'].sum(), 2)
    DF_merge['subsd_units shr chng'] = round(DF_merge['subsidized_units_y']/DF_merge['subsidized_units_y'].sum()-DF_merge['subsidized_units_x']/DF_merge['subsidized_units_x'].sum(), 2)
    DF_merge['presrv_units shr chng'] = round(DF_merge['preserved_units_y']/DF_merge['preserved_units_y'].sum()-DF_merge['preserved_units_x']/DF_merge['preserved_units_x'].sum(), 2)

    DF_GROWTH = DF_merge[DF_COLUMNS].copy()
    DF_GROWTH['RUNID'] = run_num
    return DF_GROWTH

def GEO_SUMMARY_CALCULATOR(parcel_geo_data):
    parcel_geo_data = parcel_geo_data.groupby(['juris']).agg({'tothh_y':'sum','totemp_y':'sum','hhq1_y':'sum', 'hhq2_y':'sum','hhq3_y':'sum', 'hhq4_y':'sum', 
                                                    'tothh_x':'sum','totemp_x':'sum','hhq1_x':'sum', 'hhq2_x':'sum','hhq3_x':'sum', 'hhq4_x':'sum', 
                                                    'residential_units_y':'sum','deed_restricted_units_y':'sum','inclusionary_units_y':'sum', 'subsidized_units_y':'sum','preserved_units_y':'sum', 
                                                    'residential_units_x':'sum','deed_restricted_units_x':'sum','inclusionary_units_x':'sum', 'subsidized_units_x':'sum','preserved_units_x':'sum',}).reset_index()
    parcel_geo_data['totemp growth'] = parcel_geo_data['totemp_y']-parcel_geo_data['totemp_x']
    parcel_geo_data['tothh growth'] = parcel_geo_data['tothh_y']-parcel_geo_data['tothh_x']
    parcel_geo_data['hhq1 growth'] = parcel_geo_data['hhq1_y']-parcel_geo_data['hhq1_x']
    parcel_geo_data['hhq2 growth'] = parcel_geo_data['hhq2_y']-parcel_geo_data['hhq2_x']
    parcel_geo_data['hhq3 growth'] = parcel_geo_data['hhq3_y']-parcel_geo_data['hhq3_x']
    parcel_geo_data['hhq4 growth'] = parcel_geo_data['hhq4_y']-parcel_geo_data['hhq4_x']
    parcel_geo_data['res_units growth'] = parcel_geo_data['residential_units_y']-parcel_geo_data['residential_units_x']
    parcel_geo_data['dr_units growth'] = parcel_geo_data['deed_restricted_units_y']-parcel_geo_data['deed_restricted_units_x']
    parcel_geo_data['incl_units growth'] = parcel_geo_data['inclusionary_units_y']-parcel_geo_data['inclusionary_units_x']
    parcel_geo_data['subsd_units growth'] = parcel_geo_data['subsidized_units_y']-parcel_geo_data['subsidized_units_x']
    parcel_geo_data['presrv_units growth'] = parcel_geo_data['preserved_units_y']-parcel_geo_data['preserved_units_x']
    parcel_geo_data = parcel_geo_data[['tothh growth','totemp growth','hhq1 growth','hhq2 growth','hhq3 growth','hhq4 growth',
                                      'res_units growth','dr_units growth','incl_units growth','subsd_units growth','presrv_units growth','juris']].copy()
    return parcel_geo_data	

#This is to define a separate fileloader for parcel difference data. With the zoningmod category, we should be able to
#summarize growth by different geography types that is more nuanced.
def GEO_SUMMARY_LOADER(run_num, geo, parcel_baseyear, parcel_endyear):

    if 'fbpchcat' in parcel_endyear.columns:
      zoningtag = 'fbpchcat'
    elif 'pba50chcat' in parcel_endyear.columns:
      zoningtag = 'pba50chcat'
    elif 'zoningmodcat' in parcel_endyear.columns:
      zoningtag = 'zoningmodcat'
    else: 
      zoningtag = 'zoninghzcat'

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','residential_units','deed_restricted_units','inclusionary_units', 'subsidized_units','preserved_units']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','residential_units','deed_restricted_units','inclusionary_units', 'subsidized_units','preserved_units','juris',zoningtag]]
    parcel_data = parcel_baseyear.merge(parcel_endyear, on = 'parcel_id', how = 'left').fillna(0)
    if 0 in parcel_data.juris.values:
        dropindex = parcel_data[parcel_data['juris'] == 0].index
        parcel_data.drop(dropindex,inplace = True)
        
    #geography summaries
    parcel_geo = parcel_data.loc[parcel_data[zoningtag].str.contains(geo, na=False)]
    parcel_geo = GEO_SUMMARY_CALCULATOR(parcel_geo)
    parcel_geo['geo_category'] = 'yes_%s'%(geo)
    parcel_geo_no = parcel_data.loc[~parcel_data[zoningtag].str.contains(geo, na=False)]
    parcel_geo_no = GEO_SUMMARY_CALCULATOR(parcel_geo_no)
    parcel_geo_no['geo_category'] = 'no_%s'%(geo)
    
    parcel_geo_summary = pd.concat([parcel_geo, parcel_geo_no])
    parcel_geo_summary.sort_values(by = 'juris', inplace = True)
    parcel_geo_summary['RUNID'] = run_num
    
    return parcel_geo_summary


##Similar to above, this is to define a separate fileloader to produce summaries for overlapping geographies. W
def TWO_GEO_SUMMARY_LOADER(run_num, geo1, geo2, parcel_baseyear, parcel_endyear):

    if 'fbpchcat' in parcel_endyear.columns:
      zoningtag = 'fbpchcat'
    elif 'pba50chcat' in parcel_endyear.columns:
      zoningtag = 'pab50chcat'
    elif 'zoningmodcat' in parcel_endyear.columns:
      zoningtag = 'zoningmodcat'
    else: 
      zoningtag = 'zoninghzcat'

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','residential_units','deed_restricted_units','inclusionary_units', 'subsidized_units','preserved_units']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4','residential_units','deed_restricted_units','inclusionary_units', 'subsidized_units','preserved_units','juris',zoningtag]]
    parcel_data = parcel_baseyear.merge(parcel_endyear, on = 'parcel_id', how = 'left').fillna(0)
    if 0 in parcel_data.juris.values:
        dropindex = parcel_data[parcel_data['juris'] == 0].index
        parcel_data.drop(dropindex,inplace = True)    
    #two geographies
    parcel_geo2 = parcel_data.loc[parcel_data[zoningtag].str.contains(geo1, na=False)]
    parcel_geo2 = parcel_geo2.loc[parcel_geo2[zoningtag].str.contains(geo2, na=False)]
    parcel_geo2_group = GEO_SUMMARY_CALCULATOR(parcel_geo2)
    parcel_geo2_group['geo_category'] = 'yes_%s'%(geo1+geo2)
    
    parcel_geo2_no_1 = parcel_data.loc[parcel_data[zoningtag].str.contains(geo1, na=False)]
    parcel_geo2_no_1 = parcel_geo2_no_1.loc[~parcel_geo2_no_1[zoningtag].str.contains(geo2, na=False)]
    parcel_geo2_no_2 = parcel_data.loc[parcel_data[zoningtag].str.contains(geo2, na=False)]
    parcel_geo2_no_2 = parcel_geo2_no_2.loc[~parcel_geo2_no_2[zoningtag].str.contains(geo1, na=False)]
    parcel_geo2_no_3 = parcel_data.loc[~parcel_data[zoningtag].str.contains(geo1 + "|" + geo2, na=False)]
    
    parcel_geo2_no = pd.concat([parcel_geo2_no_1, parcel_geo2_no_2, parcel_geo2_no_3], ignore_index = True)
    parcel_geo2_no_group = GEO_SUMMARY_CALCULATOR(parcel_geo2_no)
    parcel_geo2_no_group['geo_category'] = 'no_%s'%(geo1+geo2)
    
    parcel_geo2_summary = pd.concat([parcel_geo2_group, parcel_geo2_no_group], ignore_index = True)
    parcel_geo2_summary.sort_values(by = 'juris', inplace = True)
    parcel_geo2_summary['RUNID'] = run_num
    
    return parcel_geo2_summary


from __future__ import print_function

import os
import orca
import pandas as pd
from pandas.util import testing as pdt
import numpy as np
from baus.utils import round_series_match_target, scale_by_target, simple_ipf, format_df
from urbansim.utils import misc
from baus.postprocessing import GEO_SUMMARY_LOADER, TWO_GEO_SUMMARY_LOADER, nontaz_calculator, taz_calculator,\
county_calculator, juris_to_county


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, run_setup, run_number, year, summary, final_year):
    # using the following conditional b/c `year` is used to pull a column
    # from a csv based on a string of the year in add_population()
    # and in add_employment() and 2009 is the
    # 'base'/pre-simulation year, as is the 2010 value in the csv.
    if year == 2009:
        year = 2010
        base = True
    else:
        base = False   

    households_df = orca.merge_tables('households', [parcels, buildings, households],
        columns=['pda_id', 'zone_id', 'juris', 'superdistrict',
                 'persons', 'income', 'base_income_quartile',
                 'juris_tra', 'juris_sesit', 'juris_ppa'])

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs],
        columns=['pda_id', 'superdistrict', 'juris', 'zone_id',
                 'empsix', 'juris_tra', 'juris_sesit', 'juris_ppa'])

    buildings_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['pda_id', 'superdistrict', 'juris',
                 'building_type', 'zone_id', 'residential_units',
                 'deed_restricted_units', 'preserved_units',
                 'inclusionary_units', 'subsidized_units',
                 'building_sqft', 'non_residential_sqft',
                 'juris_tra', 'juris_sesit', 'juris_ppa'])

    parcel_output = summary.parcel_output

    # because merge_tables returns multiple zone_id_'s, but not the one we need
    buildings_df = buildings_df.rename(columns={'zone_id_x': 'zone_id'})

    geographies = ['superdistrict', 'juris']

# disable final blueprint summaries being handled in post processing summaries
#    # append Draft/Final Blueprint strategy geographis
#    geographies.extend(['pda_id', 'juris_tra', 'juris_sesit', 'juris_ppa'])

    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]:

        for geography in geographies:

            # create table with household/population summaries

            summary_table = pd.pivot_table(households_df,
                                           values=['persons'],
                                           index=[geography],
                                           aggfunc=[np.size])

            summary_table.columns = ['tothh']

            # fill in 0 values where there are NA's so that summary table
            # outputs are the same over the years otherwise a PDA or summary
            # geography would be dropped if it had no employment or housing
            if geography == 'superdistrict':
                all_summary_geographies = buildings_df[geography].unique()
            else:
                all_summary_geographies = parcels[geography].unique()
            summary_table = summary_table.reindex(all_summary_geographies).fillna(0)

            # turns out the lines above had to be moved up - if there are no
            # households in a geography the index is missing that geography
            # right off the bat.  then when we try and add a jobs or buildings
            # aggregation that HAS that geography, it doesn't get saved.  ahh
            # pandas, so powerful but so darn confusing.

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
            summary_table['sq_ft_per_employee'] = summary_table['non_residential_sqft'] / summary_table['totemp']

            # columns re: affordable housing
            summary_table['deed_restricted_units'] = buildings_df.groupby(geography).deed_restricted_units.sum()
            summary_table['preserved_units'] = buildings_df.groupby(geography).preserved_units.sum()
            summary_table['inclusionary_units'] = buildings_df.groupby(geography).inclusionary_units.sum()
            summary_table['subsidized_units'] = buildings_df.groupby(geography).subsidized_units.sum()       

            # additional columns from parcel_output
            if parcel_output is not None:

                # columns re: affordable housing
                summary_table['inclusionary_revenue_reduction'] = parcel_output.groupby(geography).policy_based_revenue_reduction.sum()
                summary_table['inclusionary_revenue_reduction_per_unit'] = summary_table.inclusionary_revenue_reduction / \
                    summary_table.inclusionary_units
                summary_table['total_subsidy'] = parcel_output[parcel_output.subsidized_units > 0].\
                    groupby(geography).max_profit.sum() * -1    
                summary_table['subsidy_per_unit'] = summary_table.total_subsidy / summary_table.subsidized_units

            summary_table = summary_table.sort_index()

            if base is False:
                summary_csv = os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summaries_{}.csv").\
                    format(run_number, geography, year)
            elif base is True:
                summary_csv = os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summaries_{}.csv").\
                    format(run_number, geography, 2009)

            summary_table.to_csv(summary_csv)

    # Write Summary of Accounts
    if year == final_year and (run_setup["run_housing_bond_strategy"] or run_setup["run_office_bond_strategy"]
                               or run_setup['run_vmt_fee_strategy'] or run_setup['run_jobs_housing_fee_strategy']):

        for acct_name, acct in orca.get_injectable("coffer").items():
            fname = os.path.join(orca.get_injectable("outputs_dir"), "run{}_acctlog_{}_{}.csv").\
                format(run_number, acct_name, year)
            acct.to_frame().to_csv(fname)

    if year == final_year:
        baseyear = 2015
        for geography in geographies:
            df_base = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summaries_{}.csv".\
                                            format(run_number, geography, baseyear)))
            df_final = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_summaries_{}.csv".\
                                            format(run_number, geography, final_year)))
            df_growth = nontaz_calculator(run_number, df_base, df_final)
            df_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_growth_summaries.csv".\
                                        format(run_number, geography)), index = False)

    # Write Urban Footprint Summary
    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]:
        # 02 15 2019 ET: Using perffoot there was no greenfield change
        # between 2010 and 2050. Joined the parcels to Urbanized_Footprint
        # instead, which improved the diff. The large majority of greenfield
        # still occurs in 2010 (base year buildings outside of the
        # urbanized area).

        buildings_uf_df = orca.merge_tables(
            'buildings',
            [parcels, buildings],
            columns=['urbanized', 'year_built',
                     'acres', 'residential_units',
                     'non_residential_sqft'])

        buildings_uf_df['count'] = 1

        # residential units per acre in current year
        s1 = buildings_uf_df['residential_units'] / buildings_uf_df['acres']
        # residential units per acre > 1 in current year
        s2 = s1 > 1
        # urban footprint is 0 in base year (there's no development)
        s3 = (buildings_uf_df['urbanized'] == 0) * 1
        # urban footprint is 0 in the base year
        # AND residential units per acre > 1 in current year
        buildings_uf_df['denser_greenfield'] = s3 * s2

        # where buildings were built after the base year,
        # sum whether urban footprint was 0 or 1 in the base year
        df = buildings_uf_df.\
            loc[buildings_uf_df['year_built'] > 2010].\
            groupby('urbanized').sum()
        df = df[['count', 'residential_units', 'non_residential_sqft',
                 'acres']]

        # where buildings were built after the base year,
        # sum if it was denser greenfield
        df2 = buildings_uf_df.\
            loc[buildings_uf_df['year_built'] > 2010].\
            groupby('denser_greenfield').sum()
        df2 = df2[['count', 'residential_units', 'non_residential_sqft',
                   'acres']]

        formatters = {'count': '{:.0f}',
                      'residential_units': '{:.0f}',
                      'non_residential_sqft': '{:.0f}',
                      'acres': '{:.0f}'}

        df = format_df(df, formatters)

        df2 = format_df(df2, formatters)

        df = df.transpose()

        df2 = df2.transpose()

        df[2] = df2[1]

        df.columns = ['urban_footprint_0', 'urban_footprint_1',
                      'denser_greenfield']
        uf_summary_csv = os.path.join(orca.get_injectable("outputs_dir"), "run{}_urban_footprint_summary_{}.csv".\
            format(run_number, year))
        df.to_csv(uf_summary_csv)

    # Summarize Logsums
    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]:
        zones = orca.get_table('zones')
        df = zones.to_frame(['zone_cml', 'zone_cnml', 'zone_combo_logsum'])
        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"),
                               "run%d_taz_logsums_%d.csv"
                               % (run_number, year)))
        parcels = orca.get_table('parcels')
        df = parcels.to_frame(['cml', 'cnml', 'combo_logsum'])
        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"),
                               "run%d_parcel_logsums_%d.csv"
                               % (run_number, year)))
        





#The script is used for process geograpy summary files and combine different run results for visualization in Tableau
import pandas as pd
import numpy as np
import os

# bringing in zoning modifications growth geography tag
    join_col = 'zoningmodcat'
    if join_col in parcels_geography.to_frame().columns:
        parcel_gg = parcels_geography.to_frame(["parcel_id", join_col, "juris"])
        df = df.merge(parcel_gg, on='parcel_id', how='left')

    # if year == final_year:
    print('year printed for debug: {}'.format(year))
    if year not in [2010, 2015]:

        print('calculate diff for year {}'.format(year)) 
        baseyear = 2015
        df_base = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" % (run_number, baseyear)))
        df_final = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" # % (run_number, final_year)))
                                                                                                           % (run_number, year)))

        geographies = ['GG','tra','HRA', 'DIS']

        for geography in geographies:
            df_growth = GEO_SUMMARY_LOADER(run_number, geography, df_base, df_final)
            df_growth['county'] = df_growth['juris'].map(juris_to_county)
            df_growth.sort_values(by = ['county','juris','geo_category'], ascending=[True, True, False], inplace=True)
            df_growth.set_index(['RUNID','county','juris','geo_category'], inplace=True)
            df_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_growth_summaries.csv".\
                                          format(run_number, geography)))

        geo_1, geo_2, geo_3 = 'tra','DIS','HRA'

        df_growth_1 = TWO_GEO_SUMMARY_LOADER(run_number, geo_1, geo_2, df_base, df_final)
        df_growth_1['county'] = df_growth_1['juris'].map(juris_to_county)
        df_growth_1.sort_values(by = ['county','juris','geo_category'], ascending=[True, True, False], inplace=True)
        df_growth_1.set_index(['RUNID','county','juris','geo_category'], inplace=True)
        df_growth_1.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_growth_summaries.csv".\
                                        format(run_number, geo_1 + geo_2)))

        df_growth_2 = TWO_GEO_SUMMARY_LOADER(run_number, geo_1, geo_3, df_base, df_final)
        df_growth_2['county'] = df_growth_2['juris'].map(juris_to_county)
        df_growth_2.sort_values(by = ['county','juris','geo_category'], ascending=[True, True, False], inplace=True)
        df_growth_2.set_index(['RUNID','county','juris','geo_category'], inplace=True)
        df_growth_2.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_{}_growth_summaries.csv".\
                                        format(run_number, geo_1 + geo_2)))
        



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

sd_mapping = {1 : 'Greater Downtown San Francisco',
              2 : 'San Francisco Richmond District',
              3 : 'San Francisco Mission District',
              4 : 'San Francisco Sunset District',
              5 : 'Daly City and San Bruno',
              6 : 'San Mateo and Burlingame',
              7 : 'Redwood City and Menlo Park',
              8 : 'Palo Alto and Los Altos',
              9 : 'Sunnyvale and Mountain View',
              10 : 'Cupertino and Saratoga',
              11 : 'Central San Jose',
              12 : 'Milpitas and East San Jose',
              13 : 'South San Jose',
              14 : 'Gilroy and Morgan Hill',
              15 : 'Livermore and Pleasanton',
              16 : 'Fremont and Union City',
              17 : 'Hayward and San Leandro',
              18 : 'Oakland and Alameda',
              19 : 'Berkeley and Albany',
              20 : 'Richmond and El Cerrito',
              21 : 'Concord and Martinez',
              22 : 'Walnut Creek',
              23 : 'Danville and San Ramon',
              24 : 'Antioch and Pittsburg',
              25 : 'Vallejo and Benicia',
              26 : 'Fairfield and Vacaville',
              27 : 'Napa',
              28 : 'St Helena',
              29 : 'Petaluma and Rohnert Park',
              30 : 'Santa Rosa and Sebastopol',
              31 : 'Healdsburg and cloverdale',
              32 : 'Novato',
              33 : 'San Rafael',
              34 : 'Mill Valley and Sausalito'}


county_mapping = {1: 'San Francisco',
                  2: 'San Mateo',
                  3: 'Santa Clara',
                  4: 'Alameda',
                  5: 'Contra Costa',
                  6: 'Solano',
                  7: 'Napa',
                  8: 'Sonoma',
                  9: 'Marin'}

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
        DF_TAZ_GROWTH = DF_TAZ_GROWTH.rename(columns={'SD_x': 'SD', 'COUNTY_x': 'COUNTY'})
        DF_TAZ_GROWTH['SD_NAME'] = DF_TAZ_GROWTH['SD'].map(sd_mapping)
        DF_TAZ_GROWTH['CNTY_NAME'] = DF_TAZ_GROWTH['COUNTY'].map(county_mapping)
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
        DF_merge['sd_name'] = DF_merge['superdistrict'].map(sd_mapping)
        DF_COLUMNS = ['superdistrict','sd_name'] + DF_COLUMNS
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
    DF_merge['subsd_units shr chng'] = round(DF_merge['subsidized_units_y']/DF_merge['subsidized_units_y'].sum(), 2)
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

    zoningtag = 'zoningmodcat'

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4',
                                       'residential_units','deed_restricted_units',
                                       'inclusionary_units', 'subsidized_units','preserved_units']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4',
                                     'residential_units','deed_restricted_units','inclusionary_units',
                                     'subsidized_units','preserved_units','juris',zoningtag]]
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

    zoningtag = 'zoningmodcat'

    parcel_baseyear = parcel_baseyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4',
                                       'residential_units','deed_restricted_units',
                                       'inclusionary_units', 'subsidized_units', 'preserved_units']]
    parcel_endyear = parcel_endyear[['parcel_id','tothh','totemp', 'hhq1','hhq2','hhq3','hhq4',
                                     'residential_units','deed_restricted_units','inclusionary_units',
                                     'subsidized_units','preserved_units','juris',zoningtag]]
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


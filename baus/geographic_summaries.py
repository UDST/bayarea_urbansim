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
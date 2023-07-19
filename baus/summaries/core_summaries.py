from __future__ import print_function

import os
import orca
import pandas as pd


@orca.step()
def parcel_summary(parcels, buildings, households, jobs, run_number, year, initial_summary_year, 
                   interim_summary_year, final_year):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    df = parcels.to_frame(["geom_id", "x", "y"])
    # add building data for parcels
    building_df = orca.merge_tables('buildings', [parcels, buildings], columns=['parcel_id', 'residential_units', 'deed_restricted_units',
                                                                                'preserved_units', 'inclusionary_units', 'subsidized_units',
                                                                                'non_residential_sqft'])
    for col in building_df.columns:
        if col == 'parcel_id':
            continue
        df[col] = building_df.groupby('parcel_id')[col].sum()

    # add households by quartile on each parcel
    households_df = orca.merge_tables('households', [buildings, households], columns=['parcel_id', 'base_income_quartile'])
    for i in range(1, 5):
        df['hhq%d' % i] = households_df[households_df.base_income_quartile == i].parcel_id.value_counts()
    df["tothh"] = households_df.groupby('parcel_id').size()

    # add jobs by empsix category on each parcel
    jobs_df = orca.merge_tables('jobs', [buildings, jobs], columns=['parcel_id', 'empsix'])
    for cat in jobs_df.empsix.unique():
        df[cat] = jobs_df[jobs_df.empsix == cat].parcel_id.value_counts()
    df["totemp"] = jobs_df.groupby('parcel_id').size()

    df = df.fillna(0)
    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_summary_%d.csv" % (run_number, year)))


@orca.step()
def parcel_growth_summary(run_number, year, initial_summary_year, final_year):
    
    if year != final_year:
        return

    df1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_summary_%d.csv" %
                        (run_number, initial_summary_year)), index_col="parcel_id")
    df2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_summary_%d.csv" %
                        (run_number, final_year)), index_col="parcel_id")

    for col in df1.columns:
        if col in ["geom_id", "x", "y"]:
            continue

        # fill na with 0 otherwise it drops the parcel data during subtraction
        df1[col].fillna(0, inplace=True)
        df2[col].fillna(0, inplace=True)

        df1[col] = df2[col] - df1[col]

    df1.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_growth.csv" % run_number))


@orca.step()
def building_summary(parcels, buildings, run_number, year, initial_summary_year, final_year, interim_summary_year):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
        return

    df = orca.merge_tables('buildings',
        [parcels, buildings],
        columns=['parcel_id', 'year_built', 'building_type', 'residential_units', 'unit_price', 
                 'non_residential_sqft', 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces', 'source'])

    df = df.fillna(0)
    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_building_summary_%d.csv" % (run_number, year)))


@orca.step()
def diagnostic_output(households, buildings, parcels, jobs, developer_settings, zones, year, summary, residential_units):

    # commented code has been ported from summaries.py to be consolidated in this interim/diagnostic output code
    # TODO: the next summary cleaning PR will handle interim output for the BAUS visualizer

    households = households.to_frame()
    buildings = buildings.to_frame()
    parcels = parcels.to_frame()
    zones = zones.to_frame()

    zones['zoned_du'] = parcels.groupby('zone_id').zoned_du.sum()
    zones['zoned_du_underbuild'] = parcels.groupby('zone_id').zoned_du_underbuild.sum()
    zones['zoned_du_underbuild_ratio'] = zones.zoned_du_underbuild / zones.zoned_du

    zones['residential_units'] = buildings.groupby('zone_id').residential_units.sum()
    zones['job_spaces'] = buildings.groupby('zone_id').job_spaces.sum()
    tothh = households.zone_id.value_counts().reindex(zones.index).fillna(0)
    ####
    # summary_table['sq_ft_per_employee'] = summary_table['non_residential_sqft'] / summary_table['totemp']
    ####
    zones['residential_vacancy'] = 1.0 - tothh / zones.residential_units.replace(0, 1)
    zones['non_residential_sqft'] = buildings.groupby('zone_id').non_residential_sqft.sum()
    totjobs = jobs.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['non_residential_vacancy'] = 1.0 - totjobs / zones.job_spaces.replace(0, 1)

    zones['retail_sqft'] = buildings.query('general_type == "Retail"').groupby('zone_id').non_residential_sqft.sum()
    zones['office_sqft'] = buildings.query('general_type == "Office"').groupby('zone_id').non_residential_sqft.sum()
    zones['industrial_sqft'] = buildings.query('general_type == "Industrial"').groupby('zone_id').non_residential_sqft.sum()

    zones['average_income'] = households.groupby('zone_id').income.quantile()
    zones['household_size'] = households.groupby('zone_id').persons.quantile()

    zones['building_count'] = buildings.query('general_type == "Residential"').groupby('zone_id').size()
    # this price is the max of the original unit vector belows
    zones['residential_price'] = buildings.query('general_type == "Residential"').groupby('zone_id').residential_price.quantile()
    # these two are the original unit prices averaged up to the building id
    ru = residential_units
    zones['unit_residential_price'] = ru.unit_residential_price.groupby(ru.zone_id).quantile()
    zones['unit_residential_rent'] = ru.unit_residential_rent.groupby(ru.zone_id).quantile()
    cap_rate = developer_settings.get('cap_rate')
    # this compares price to rent and shows us where price is greater
    # rents are monthly and a cap rate is applied in order to do the conversion
    zones['unit_residential_price_>_rent'] = (zones.unit_residential_price > (zones.unit_residential_rent * 12 / cap_rate)).astype('int')
    ####
    #if parcel_output is not None:
    #    summary_table['inclusionary_revenue_reduction'] = parcel_output.groupby(geography).policy_based_revenue_reduction.sum()
    #    summary_table['inclusionary_revenue_reduction_per_unit'] = summary_table.inclusionary_revenue_reduction / \
    #        summary_table.inclusionary_units
    #    summary_table['total_subsidy'] = parcel_output[parcel_output.subsidized_units > 0].\
    #        groupby(geography).max_profit.sum() * -1    
    #    summary_table['subsidy_per_unit'] = summary_table.total_subsidy / summary_table.subsidized_units  
    ####
    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].groupby('zone_id').non_residential_rent.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].groupby('zone_id').non_residential_rent.quantile()
    zones['industrial_rent'] = buildings[buildings.general_type == "Industrial"].groupby('zone_id').non_residential_rent.quantile()

    zones['retail_sqft'] = buildings[buildings.general_type == "Retail"].groupby('zone_id').non_residential_sqft.sum()

    zones['retail_to_res_units_ratio'] = zones.retail_sqft / zones.residential_units.replace(0, 1)

    summary.add_zone_output(zones, "diagnostic_outputs", year)
    #du = buildings.residential_units.sum()
    #write("Number of residential units in buildings table = %d" % du)
    #write("Residential vacancy rate = %.2f" % (1-0 - float(nhh)/du))
    #du = buildings.deed_restricted_units.sum()

    #write("Number of residential units in units table = %d" % len(residential_units))
    #rent_own = residential_units.tenure.value_counts()
    #write("Split of units by rent/own = %s" % str(rent_own))

    #jsp = buildings.job_spaces.sum()
    #write("Number of job spaces = %d" % jsp)
    #write("Non-residential vacancy rate = %.2f" % (1-0 - float(nj)/jsp))
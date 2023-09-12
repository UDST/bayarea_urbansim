from __future__ import print_function

import os
import orca
import pandas as pd


@orca.step()
def parcel_summary(run_name, parcels, buildings, households, jobs, year, initial_summary_year, interim_summary_year, final_year):

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
    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/{}_parcel_summary_%d.csv" % (run_name, year)))


@orca.step()
def parcel_growth_summary(year, initial_summary_year, final_year):
    
    if year != final_year:
        return

    df1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/parcel_summary_%d.csv" %
                        (initial_summary_year)), index_col="parcel_id")
    df2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/parcel_summary_%d.csv" %
                        (final_year)), index_col="parcel_id")

    for col in df1.columns:
        if col in ["geom_id", "x", "y"]:
            continue

        # fill na with 0 otherwise it drops the parcel data during subtraction
        df1[col].fillna(0, inplace=True)
        df2[col].fillna(0, inplace=True)

        df1[col] = df2[col] - df1[col]

    df1.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/parcel_growth.csv"))


@orca.step()
def building_summary(run_name, parcels, buildings, year, initial_summary_year, final_year, interim_summary_year):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
        return

    df = orca.merge_tables('buildings',
        [parcels, buildings],
        columns=['parcel_id', 'year_built', 'building_type', 'residential_units', 'unit_price', 
                 'non_residential_sqft', 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces', 'source'])

    df = df.fillna(0)
    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/{}_building_summary_%d.csv" % (run_name, year)))


@orca.step()
def new_buildings_summary(run_name, parcels, buildings, year, final_year):

    if year != final_year:
        return

    df = orca.merge_tables('buildings', [parcels, buildings])    
    df = df[~df.source.isin(["h5_inputs"])]

    df = df.fillna(0)
    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/{}_new_building_summary.csv").format(run_name))


@orca.step()
def interim_zone_output(run_name, households, buildings, residential_units, parcels, jobs, zones, year):

    # TODO: currently TAZ, do we want this to be MAZ?
    zones = pd.DataFrame(index=zones.index)

    parcels = parcels.to_frame()
    buildings = buildings.to_frame()
    residential_units = residential_units.to_frame()
    households = households.to_frame()
    jobs = jobs.to_frame()

    # CAPACITY
    zones['zoned_du'] = parcels.groupby('zone_id').zoned_du.sum()
    zones['zoned_du_underbuild'] = parcels.groupby('zone_id').zoned_du_underbuild.sum()
    zones['zoned_du_underbuild_ratio'] = zones.zoned_du_underbuild / zones.zoned_du

    zones['residential_units'] = buildings.groupby('zone_id').residential_units.sum()
    zones['job_spaces'] = buildings.groupby('zone_id').job_spaces.sum()

    # VACANCY
    tothh = households.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['residential_vacancy'] = 1.0 - tothh / zones.residential_units.replace(0, 1)
    zones['non_residential_sqft'] = buildings.groupby('zone_id').non_residential_sqft.sum()
    totjobs = jobs.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['non_residential_vacancy'] = 1.0 - totjobs / zones.job_spaces.replace(0, 1)

    # PRICE VERSUS NONRES RENT
    zones['residential_price'] = residential_units.groupby('zone_id').unit_residential_price.quantile()
    zones['residential_rent'] = residential_units.groupby('zone_id').unit_residential_rent.quantile()
    zones['non_residential_rent'] = buildings.groupby('zone_id').non_residential_rent.quantile()

    zones.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/{}_interim_zone_output_%d.csv" % (run_name, year)))


@orca.step()
def all_zone_output(run_name, zones, year, final_year):

    if not final_year:
        return
    
    # TODO: currently TAZ, do we want this to be MAZ?
    all_zones = pd.DataFrame(index=zones.index)

    all_zones.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "core_summaries/{}_interim_zone_output_%d.csv" % (run_name, year)))


@orca.step()
def feasibility_table_output(run_name, feasibility_before_policy, feasibility_after_policy, year):

    # created by alt_feasibility(); 
    # modified by subsidized_residential_feasibility() and policy_modifications_of_profit()
    # residential_developer(), office_developer(), retail_developer(), run_subsidized_developer(), subsidized_office_developer()

    feasibility_before_policy.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "{}_feasibility_before_policy_%d.csv" % (run_name, year)))

    feasibility_after_policy.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "{}_feasibility_after_policy%d.csv" % (run_name, year)))
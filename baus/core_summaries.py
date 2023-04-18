from __future__ import print_function

import os
import orca
import pandas as pd


@orca.step()
def parcel_summary(parcels, buildings, households, jobs, run_number, year, initial_year, parcels_geography):

    if year not in [2010, 2015, 2035, 2050]:
         return
    
    # (1) print parcel data for model run year
    df = parcels.to_frame(["geom_id", "x", "y", "total_job_spaces", "first_building_type"])

    households_df = orca.merge_tables('households', [buildings, households], columns=['parcel_id', 'base_income_quartile'])

    # add households by quartile on each parcel
    for i in range(1, 5):
        df['hhq%d' % i] = households_df[households_df.base_income_quartile == i].parcel_id.value_counts()
    df["tothh"] = households_df.groupby('parcel_id').size()

    # add building data for parcels
    building_df = orca.merge_tables('buildings', [parcels, buildings],
                                    columns=['parcel_id', 'residential_units', 'deed_restricted_units', 
                                             'preserved_units', 'inclusionary_units', 'subsidized_units'])
    for col in building_df.columns:
        if col == 'parcel_id':
            return
        df[col] = building_df.groupby('parcel_id')[col].sum()

    jobs_df = orca.merge_tables('jobs', [buildings, jobs], columns=['parcel_id', 'empsix'])

    # add jobs by empsix category on each parcel
    for cat in jobs_df.empsix.unique():
        df[cat] = jobs_df[jobs_df.empsix == cat].parcel_id.value_counts()
    df["totemp"] = jobs_df.groupby('parcel_id').size()

    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" % (run_number, year)))

    # (2) write parcel growth summaries for future years
    if year not in [2010, 2015]:
        print('calculate diff for year {}'.format(year))

        df2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" %
                          (run_number, initial_year)), index_col="parcel_id")

        for col in df.columns:
            if col in ["x", "y", "first_building_type", "juris"]:
                continue
            # fill na with 0 for parcels with no building in either base year or current year, otherwise it drops the data 
            # during subtraction
            df[col].fillna(0, inplace=True)
            df2[col].fillna(0, inplace=True)
            df[col] = df[col] - df2[col]

        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_diff.csv" % run_number))


@orca.step()
def building_summary(parcels, run_number, year, buildings):

    if year not in [2010, 2015, 2035, 2050]:
        return

    df = orca.merge_tables(
        'buildings',
        [parcels, buildings],
        columns=['year_built', 'building_type',
                 'residential_units', 'unit_price', 'zone_id', 
                 'non_residential_sqft', 'vacant_res_units', 
                 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces',
                 'x', 'y', 'geom_id', 'source'])

    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_building_data_%d.csv" % (run_number, year)))


@orca.step()
def diagnostic_output(households, buildings, parcels, taz, jobs, developer_settings, zones, year, summary, run_number, residential_units):

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

    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].groupby('zone_id').non_residential_rent.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].groupby('zone_id').non_residential_rent.quantile()
    zones['industrial_rent'] = buildings[buildings.general_type == "Industrial"].groupby('zone_id').non_residential_rent.quantile()

    zones['retail_sqft'] = buildings[buildings.general_type == "Retail"].groupby('zone_id').non_residential_sqft.sum()

    zones['retail_to_res_units_ratio'] = zones.retail_sqft / zones.residential_units.replace(0, 1)

    summary.add_zone_output(zones, "diagnostic_outputs", year)

    # save the dropped buildings to a csv
    if "dropped_buildings" in orca.orca._TABLES:
        df = orca.get_table("dropped_buildings").to_frame()
        print("Dropped buildings", df.describe())
        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_dropped_buildings.csv").format(run_number))
        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run{}_dropped_buildings.csv").format(run_number))
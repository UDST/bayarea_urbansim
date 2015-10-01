import sys
import orca
import pandas as pd
import numpy as np


@orca.step("geographic_summary")
def pda_output(parcels, households, jobs, buildings, taz_to_superdistrict,
               run_number, year):

    households_df = orca.merge_tables(
        'households',
        [parcels, taz_to_superdistrict, buildings, households],
        columns=['pda', 'zone_id', 'juris', 'superdistrict', 'puma5',
                 'persons', 'income'])

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, taz_to_superdistrict, buildings, jobs],
        columns=['pda', 'superdistrict', 'juris', 'zone_id', 'empsix'])

    buildings_df = orca.merge_tables(
       'buildings',
       [parcels, taz_to_superdistrict, buildings],
       columns=['pda', 'superdistrict', 'juris', 'building_type_id', 'zone_id',
                'residential_units', 'building_sqft', 'non_residential_sqft'])

    # because merge_tables returns multiple zone_id_'s, but not the one we need
    buildings_df = buildings_df.rename(columns={'zone_id_x': 'zone_id'})

    geographies = ['superdistrict', 'pda', 'juris']

    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:

        for geography in geographies:

            # create table with household/population summaries

            summary_table = pd.pivot_table(households_df,
                                           values=['persons'],
                                           index=[geography],
                                           aggfunc=[np.size, np.sum])

            summary_table.columns = ['tothh', 'hhpop']

            # income quartile counts
            summary_table['hhincq1'] = households_df.query("income < 25000").\
                groupby(geography).size()
            summary_table['hhincq2'] = \
                households_df.query("income >= 25000 and income < 45000").\
                groupby(geography).size()
            summary_table['hhincq3'] = \
                households_df.query("income >= 45000 and income < 75000").\
                groupby(geography).size()
            summary_table['hhincq4'] = \
                households_df.query("income >= 75000").\
                groupby(geography).size()

            # residential buildings by type
            summary_table['sfdu'] = buildings_df.\
                query("building_type_id == 1 or building_type_id == 2").\
                groupby(geography).residential_units.sum()
            summary_table['mfdu'] = buildings_df.\
                query("building_type_id == 3 or building_type_id == 12").\
                groupby(geography).residential_units.sum()

            # employees by sector
            summary_table['totemp'] = jobs_df.\
                groupby(geography).size()
            summary_table['agrempn'] = jobs_df.query("empsix == 'AGREMPN'").\
                groupby(geography).size()
            summary_table['mwtempn'] = jobs_df.query("empsix == 'MWTEMPN'").\
                groupby(geography).size()
            summary_table['retempn'] = jobs_df.query("empsix == 'RETEMPN'").\
                groupby(geography).size()
            summary_table['fpsempn'] = jobs_df.query("empsix == 'FPSEMPN'").\
                groupby(geography).size()
            summary_table['herempn'] = jobs_df.query("empsix == 'HEREMPN'").\
                groupby(geography).size()
            summary_table['othempn'] = jobs_df.query("empsix == 'OTHEMPN'").\
                groupby(geography).size()

            # summary columns
            summary_table['occupancy_rate'] = summary_table['tothh'] / \
                (summary_table['sfdu'] + summary_table['sfdu'])
            summary_table['non_residential_sqft'] = buildings_df.\
                groupby(geography)['non_residential_sqft'].sum()
            summary_table['sq_ft_per_employee'] = \
                summary_table['non_residential_sqft'] / summary_table['totemp']

            #fill in 0 values where there are NA's so that summary table outputs are the same over the years
            #otherwise a PDA or summary geography would be dropped if it had no employment or housing
            if geography=='superdistrict':
                all_summary_geographies = buildings_df[geography].unique()
            else:
                all_summary_geographies = parcels[geography].unique()
            summary_table=summary_table.reindex(all_summary_geographies).fillna(0)

            summary_csv="runs/run{}_{}_summaries_{}.csv".\
                format(run_number, geography, year)
            summary_table.to_csv(summary_csv)


@orca.step("travel_model_output")
def travel_model_output(parcels, households, jobs, buildings,
                        zones, homesales, year, summary, coffer, run_number):

    households = households.to_frame()
    jobs = jobs.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()

    zones['tothh'] = households.\
        groupby('zone_id').size()
    zones['hhpop'] = households.\
        groupby('zone_id').persons.sum()

    zones['resunits'] = buildings.groupby('zone_id').residential_units.sum()
    zones['resvacancy'] = (zones.resunits - zones.tothh) / \
        zones.resunits.replace(0, 1)

    zones['sfdu'] = \
        buildings.query("building_type_id == 1 or building_type_id == 2").\
        groupby('zone_id').residential_units.sum()
    zones['mfdu'] = \
        buildings.query("building_type_id == 3 or building_type_id == 12").\
        groupby('zone_id').residential_units.sum()

    zones['hhincq1'] = households.query("income < 25000").\
        groupby('zone_id').size()
    zones['hhincq2'] = households.query("income >= 25000 and income < 45000").\
        groupby('zone_id').size()
    zones['hhincq3'] = households.query("income >= 45000 and income < 75000").\
        groupby('zone_id').size()
    zones['hhincq4'] = households.query("income >= 75000").\
        groupby('zone_id').size()

    # attempting to get at total zonal developed acres
    zones['NEWdevacres'] = \
        (buildings.query("building_sqft > 0").
         groupby('zone_id').lot_size_per_unit.sum()) / 43560

    zones['totemp'] = jobs.\
        groupby('zone_id').size()
    zones['agrempn'] = jobs.query("empsix == 'AGREMPN'").\
        groupby('zone_id').size()
    zones['mwtempn'] = jobs.query("empsix == 'MWTEMPN'").\
        groupby('zone_id').size()
    zones['retempn'] = jobs.query("empsix == 'RETEMPN'").\
        groupby('zone_id').size()
    zones['fpsempn'] = jobs.query("empsix == 'FPSEMPN'").\
        groupby('zone_id').size()
    zones['herempn'] = jobs.query("empsix == 'HEREMPN'").\
        groupby('zone_id').size()
    zones['othempn'] = jobs.query("empsix == 'OTHEMPN'").\
        groupby('zone_id').size()

    orca.add_table("travel_model_output", zones, year)

    summary.add_zone_output(zones, "travel_model_output", year)
    if sys.platform != 'win32':
        summary.write_zone_output()

    add_xy_config = {
        "xy_table": "parcels",
        "foreign_key": "parcel_id",
        "x_col": "x",
        "y_col": "y"
    }
    # otherwise it loses precision
    if summary.parcel_output is not None and \
            "geom_id" in summary.parcel_output:
        summary.parcel_output["geom_id"] = \
            summary.parcel_output.geom_id.astype('str')
    summary.write_parcel_output(add_xy=add_xy_config)

    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:

        # travel model csv
        travel_model_csv = \
            "runs/run{}_taz_summaries_{}.csv".format(run_number, year)
        travel_model_output = zones

        # list of columns that we need to fill eventually for valid travel
        # model file:
        template_columns = \
            ['age0519', 'age2044', 'age4564', 'age65p',
             'areatype', 'ciacre', 'collfte', 'collpte', 'county', 'district',
             'empres', 'gqpop', 'hhlds', 'hsenroll', 'oprkcst', 'prkcst',
             'resacre', 'sd', 'sftaz', 'shpop62p', 'terminal', 'topology',
             'totacre', 'totpop', 'zero', 'zone']

        # fill those columns with NaN until we have values for them
        for x in template_columns:
            travel_model_output[x] = np.nan

        # uppercase columns to match travel model template
        travel_model_output.columns = \
            [x.upper() for x in travel_model_output.columns]

        travel_model_output.to_csv(travel_model_csv)

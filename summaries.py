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
                        zones, homesales, summary, coffer, 
                        zone_forecast_inputs, 
                        zones_tm_output):
    year=2010
    run_number=150

    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:
        orca.add_table("travel_model_output", zones_tm_output, year)
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

        # travel model csv
        travel_model_csv = \
            "runs/run{}_taz_summaries_{}.csv".format(run_number, year)

        df = zones_tm_output.to_frame()

        # list of columns that we need to fill eventually for valid travel
        # model file:
        template_columns = \
            ['age0519', 'age2044', 'age4564', 'age65p',
             'areatype', 'collfte', 'collpte', 'county', 'district',
             'empres', 'hhlds', 'hsenroll', 'oprkcst', 'prkcst',
             'sd', 'sftaz', 'shpop62p', 'terminal', 'topology',
             'totpop', 'zero', 'zone']

        # fill those columns with NaN until we have values for them
        for x in template_columns:
            df[x] = np.nan

        # uppercase columns to match travel model template
        df.columns = \
            [x.upper() for x in zones_tm_output.columns]

        df.to_csv(travel_model_csv)
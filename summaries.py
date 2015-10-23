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
                        zones, homesales, year, summary, coffer, 
                        zone_forecast_inputs, run_number, 
                        taz):
    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:
        
        df = taz

        taz_df = pd.DataFrame(index=zones.index)
        taz_df["acres"] = df.acres
        taz_df["agrempn"] = df.agrempn
        taz_df["area"] = df.area
        taz_df["ciacre"] = df.ciacre
        taz_df["district"] = df.sd #intentionally identical to sd
        taz_df["fsempn"] = df.fsempn
        taz_df["gid"] = df.gid
        taz_df["gqpop"] = df.gqpop
        taz_df["herempn"] = df.herempn
        taz_df["hhinq1"] = df.hhinq1
        taz_df["hhinq2"] = df.hhinq2
        taz_df["hhinq3"] = df.hhinq3
        taz_df["hhinq4"] = df.hhinq4
        taz_df["hhlds"] = df.tothh #intentionally identical to tothh
        taz_df["hhpop"] = df.hhpop
        taz_df["mfdu"] = df.mfdu
        taz_df["mwtempn"] = df.mwtempn
        taz_df["newdevacres"] = df.newdevacres
        taz_df["othempn"] = df.othempn
        taz_df["resacre"] = df.resacre
        taz_df["resunits"] = df.resunits
        taz_df["resvacancy"] = df.resvacancy
        taz_df["retempn"] = df.retempn
        taz_df["sd"] = df.sd #intentionally identical to district
        taz_df["sfdu"] = df.sfdu
        taz_df["totemp"] = df.totemp
        taz_df["tothh"] = df.tothh #intentionally identical to hhlds
        taz_df["totpop"] = df.gqpop+df.hhpop
        taz_df["tract"] = df.tract
        taz_df["zero"] = pd.Series(index=df.index).fillna(0) #intentionally set to 0

        orca.add_table("travel_model_output", taz_df, year)
        summary.add_zone_output(taz_df, "travel_model_output", year)
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

        # list of columns that we need to fill eventually for valid travel
        # model file:
        template_columns = \
            ['age0519', 'age2044', 'age4564', 'age65p',
             'areatype', 'collfte', 'collpte', 'county', 
             'empres', 'hsenroll', 'oprkcst', 'prkcst',
             'terminal', 'topology','totpop']

        # fill those columns with NaN until we have values for them
        for x in template_columns:
            taz_df[x] = np.nan

        # uppercase columns to match travel model template
        taz_df.columns = \
            [x.upper() for x in taz_df.columns]

        taz_df.to_csv(travel_model_csv)
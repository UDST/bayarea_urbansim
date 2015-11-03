import sys
import os
import orca
import pandas as pd
import numpy as np
from utils import random_indexes, round_series_match_target,\
    scale_by_target, simple_ipf


@orca.step("diagnostic_output")
def diagnostic_output(households, buildings, parcels, zones, year, summary):
    households = households.to_frame()
    buildings = buildings.to_frame()
    parcels = parcels.to_frame()
    zones = zones.to_frame()

    zones['zoned_du'] = parcels.groupby('zone_id').zoned_du.sum()
    zones['zoned_du_underbuild'] = parcels.groupby('zone_id').\
        zoned_du_underbuild.sum()
    zones['zoned_du_underbuild_ratio'] = zones.zoned_du_underbuild /\
        zones.zoned_du

    zones['residential_units'] = buildings.groupby('zone_id').\
        residential_units.sum()
    zones['non_residential_sqft'] = buildings.groupby('zone_id').\
        non_residential_sqft.sum()

    zones['retail_sqft'] = buildings.query('general_type == "Retail"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['office_sqft'] = buildings.query('general_type == "Office"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['industrial_sqft'] = buildings.query('general_type == "Industrial"').\
        groupby('zone_id').non_residential_sqft.sum()

    zones['average_income'] = households.groupby('zone_id').income.quantile()
    zones['household_size'] = households.groupby('zone_id').persons.quantile()

    zones['building_count'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').size()
    zones['residential_price'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').\
        residential_price.quantile()
    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_price.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].\
        groupby('zone_id').non_residential_price.quantile()
    zones['industrial_rent'] = \
        buildings[buildings.general_type == "Industrial"].\
        groupby('zone_id').non_residential_price.quantile()

    zones['retail_sqft']  = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_sqft.sum()

    zones['retail_to_res_units_ratio'] = \
        zones.retail_sqft / zones.residential_units.replace(0, 1)

    summary.add_zone_output(zones, "diagnostic_outputs", year)


@orca.step("geographic_summary")
def pda_output(parcels, households, jobs, buildings, taz_geography,
               run_number, year):

    households_df = orca.merge_tables(
        'households',
        [parcels, taz_geography, buildings, households],
        columns=['pda', 'zone_id', 'juris', 'superdistrict', 'puma5',
                 'persons', 'income'])

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, taz_geography, buildings, jobs],
        columns=['pda', 'superdistrict', 'juris', 'zone_id', 'empsix'])

    buildings_df = orca.merge_tables(
        'buildings',
        [parcels, taz_geography, buildings],
        columns=['pda', 'superdistrict', 'juris', 'building_type_id',
                 'zone_id', 'residential_units', 'building_sqft',
                 'non_residential_sqft'])

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

            # fill in 0 values where there are NA's so that summary table
            # outputs are the same over the years otherwise a PDA or summary
            # geography would be dropped if it had no employment or housing
            if geography == 'superdistrict':
                all_summary_geographies = buildings_df[geography].unique()
            else:
                all_summary_geographies = parcels[geography].unique()
            summary_table = \
                summary_table.reindex(all_summary_geographies).fillna(0)

            summary_csv = "runs/run{}_{}_summaries_{}.csv".\
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
        taz_df["sd"] = df.sd
        taz_df["zone"] = df.index
        taz_df["county"] = df.county
        taz_df["agrempn"] = df.agrempn
        taz_df["fpsempn"] = df.fsempn
        taz_df["herempn"] = df.herempn
        taz_df["retempn"] = df.retempn
        taz_df["totemp"] = df.totemp
        taz_df["mwtempn"] = df.mwtempn
        taz_df["othempn"] = df.othempn
        taz_df["hhincq1"] = df.hhinq1
        taz_df["hhincq2"] = df.hhinq2
        taz_df["hhincq3"] = df.hhinq3
        taz_df["hhincq4"] = df.hhinq4
        taz_df["shpop62p"] = df.shpop62p
        taz_df["tothh"] = df.tothh
        taz_df["gqpop"] = df.gqpop.fillna(0)
        taz_df["mfdu"] = df.mfdu
        taz_df["sfdu"] = df.sfdu
        taz_df["ciacre"] = df.ciacre
        taz_df["resacre"] = df.resacre
        taz_df["totacre"] = df.totacre
        taz_df["totemp"] = df.totemp
        taz_df["tothh"] = df.tothh
        taz_df["zone"] = df.index

        taz_df = add_population(taz_df, year)
        # total population = group quarters plus households population
        taz_df["totpop"] = df.hhpop + df.gqpop
        taz_df["totpop"] = taz_df.totpop.fillna(0)
        taz_df = add_employment(taz_df, year)
        taz_df = add_age_categories(taz_df, year)

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

        # uppercase columns to match travel model template
        taz_df.columns = \
            [x.upper() for x in taz_df.columns]

        taz_df.fillna(0).to_csv(travel_model_csv)


def zone_forecast_inputs():
    return pd.read_csv(os.path.join('data', 'zone_forecast_inputs.csv'),
                       index_col="zone_id")


def regional_controls():
    return pd.read_csv(os.path.join('data', 'regional_controls.csv'),
                       index_col="year")


def add_population(df, year):
    rc = regional_controls()
    target = rc.totpop.loc[year] - df.gqpop.sum()

    zfi = zone_forecast_inputs()
    s = df.tothh * zfi.meanhhsize

    s = scale_by_target(s, target, .1)

    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df

# add employemnt to the dataframe - this uses a regression with
# estimated coefficients done by @mkreilly


def add_employment(df, year):

    hhs_by_inc = df[["hhincq1", "hhincq2", "hhincq3", "hhincq4"]]
    hh_shares = hhs_by_inc.divide(hhs_by_inc.sum(axis=1), axis="index")

    zfi = zone_forecast_inputs()

    empshare = 0.46381 * hh_shares.hhincq1 + 0.49361 * hh_shares.hhincq2 +\
        0.56938 * hh_shares.hhincq3 + 0.29818 * hh_shares.hhincq4 +\
        zfi.zonal_emp_sh_resid10

    # I really don't think more than 70% of people should be employed
    # in a given zone - this also makes sure that the employed residents
    # is less then the total population (after scaling) - if the
    # assertion below is triggered you can fix it by reducing this
    # .7 even a little bit more
    empshare = empshare.fillna(0).clip(.3, .7)

    empres = empshare * df.totpop

    rc = regional_controls()
    target = rc.empres.loc[year]

    empres = scale_by_target(empres, target)

    df["empres"] = round_series_match_target(empres, target, 0)

    # this should really make the assertion below pass, but this now
    # only occurs very infrequently
    df["empres"] = df[["empres", "totpop"]].min(axis=1)

    # make sure employed residents is less than total residents
    assert (df.empres <= df.totpop).all()

    return df


# add age categories necessary for the TM
def add_age_categories(df, year):
    zfi = zone_forecast_inputs()
    rc = regional_controls()

    seed_matrix = zfi[["sh_age0004", "sh_age0519", "sh_age2044",
                       "sh_age4564", "sh_age65p"]].\
        mul(df.totpop, axis='index').as_matrix()

    row_marginals = df.totpop.values
    agecols = ["age0004", "age0519", "age2044", "age4564", "age65p"]
    col_marginals = rc[agecols].loc[year].values

    target = df.totpop.sum()
    col_marginals = scale_by_target(pd.Series(col_marginals),
                                    target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    agedf = pd.DataFrame(mat)
    agedf.columns = [col.upper() for col in agecols]
    agedf.index = zfi.index

    for ind, row in agedf.iterrows():
        target = df.totpop.loc[ind]
        row = row.round()
        agedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in agedf.columns:
        df[col] = agedf[col]

    return df

import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt
import numpy as np
from utils import random_indexes, round_series_match_target,\
    scale_by_target, simple_ipf
from urbansim.utils import misc
from scripts.output_csv_utils import format_df


@orca.step()
def topsheet(households, jobs, buildings, parcels, zones, year,
             run_number, taz_geography, parcels_zoning_calculations,
             summary, settings, parcels_geography, abag_targets, new_tpp_id,
             residential_units):

    hh_by_subregion = misc.reindex(taz_geography.subregion,
                                   households.zone_id).value_counts()

    households_df = orca.merge_tables(
        'households',
        [parcels_geography, buildings, households],
        columns=['pda_id', 'tpp_id', 'income'])

    if settings["use_new_tpp_id_in_topsheet"]:
        del households_df["tpp_id"]
        households_df["tpp_id"] = misc.reindex(new_tpp_id.tpp_id,
                                               households_df.parcel_id)

    hh_by_inpda = households_df.pda_id.notnull().value_counts()
    hh_by_intpp = households_df.tpp_id.notnull().value_counts()

    hhincome_by_intpp = households_df.income.groupby(
        households_df.tpp_id.notnull()).mean()
    # round to nearest 100s
    hhincome_by_intpp = (hhincome_by_intpp/100).round()*100

    jobs_by_subregion = misc.reindex(taz_geography.subregion,
                                     jobs.zone_id).value_counts()

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['pda'])

    if settings["use_new_tpp_id_in_topsheet"]:
        jobs_df["tpp_id"] = misc.reindex(new_tpp_id.tpp_id,
                                         jobs_df.parcel_id)

    jobs_by_inpda = jobs_df.pda.notnull().value_counts()
    jobs_by_intpp = jobs_df.tpp_id.notnull().value_counts()

    capacity = parcels_zoning_calculations.\
        zoned_du_underbuild_nodev.groupby(parcels.subregion).sum()

    if year == 2010:
        # save some info for computing growth measures
        orca.add_injectable("base_year_measures", {
            "hh_by_subregion": hh_by_subregion,
            "jobs_by_subregion": jobs_by_subregion,
            "hh_by_inpda": hh_by_inpda,
            "jobs_by_inpda": jobs_by_inpda,
            "hh_by_intpp": hh_by_intpp,
            "jobs_by_intpp": jobs_by_intpp,
            "hhincome_by_intpp": hhincome_by_intpp,
            "capacity": capacity
        })

    try:
        base_year_measures = orca.get_injectable("base_year_measures")
    except:
        # the base year measures don't exist - we didn't run year 2010
        # this can happen when we skip the first year, usually because
        # we don't want to waste time doing so
        return

    f = open(os.path.join("runs", "run%d_topsheet_%d.log" %
             (run_number, year)), "w")

    def write(s):
        # print s
        f.write(s + "\n\n")

    def norm_and_round(s):
        # normalize and round a series
        return str((s/s.sum()).round(2))

    nhh = len(households)
    write("Number of households = %d" % nhh)
    nj = len(jobs)
    write("Number of jobs = %d" % nj)

    n = len(households.building_id[households.building_id == -1])
    write("Number of unplaced households = %d" % n)

    n = len(jobs.building_id[jobs.building_id == -1])
    write("Number of unplaced jobs = %d" % n)

    # we should assert there are no unplaces households and jobs right?
    # this is considered an error for the MTC-style model
    # could be configured in settings.yaml

    du = buildings.residential_units.sum()
    write("Number of residential units in buildings table = %d" % du)
    write("Residential vacancy rate = %.2f" % (1-0 - float(nhh)/du))

    write("Number of residential units in units table = %d"
          % len(residential_units))

    rent_own = residential_units.tenure.value_counts()
    write("Split of units by rent/own = %s" % str(rent_own))

    rent_own = households.tenure[households.building_id == -1].value_counts()
    write("Number of unplaced households by rent/own = %s" % str(rent_own))

    du = buildings.deed_restricted_units.sum()
    write("Number of deed restricted units = %d" % du)

    write("Base year mean income by whether household is in tpp:\n%s" %
          base_year_measures["hhincome_by_intpp"])

    write("Horizon year mean income by whether household is in tpp:\n%s" %
          hhincome_by_intpp)

    jsp = buildings.job_spaces.sum()
    write("Number of job spaces = %d" % jsp)
    write("Non-residential vacancy rate = %.2f" % (1-0 - float(nj)/jsp))

    tmp = base_year_measures["hh_by_subregion"]
    write("Households base year share by subregion:\n%s" %
          norm_and_round(tmp))

    write("Households share by subregion:\n%s" %
          norm_and_round(hh_by_subregion))
    diff = hh_by_subregion - base_year_measures["hh_by_subregion"]

    write("Households pct of regional growth by subregion:\n%s" %
          norm_and_round(diff))

    tmp = base_year_measures["jobs_by_subregion"]
    write("Jobs base year share by subregion:\n%s" %
          norm_and_round(tmp))

    write("Jobs share by subregion:\n%s" %
          norm_and_round(jobs_by_subregion))
    diff = jobs_by_subregion - base_year_measures["jobs_by_subregion"]

    write("Jobs pct of regional growth by subregion:\n%s" %
          norm_and_round(diff))

    tmp = base_year_measures["hh_by_inpda"]
    write("Households base year share in pdas:\n%s" %
          norm_and_round(tmp))

    write("Households share in pdas:\n%s" %
          norm_and_round(hh_by_inpda))

    diff = hh_by_inpda - base_year_measures["hh_by_inpda"]
    write("Households pct of regional growth in pdas:\n%s" %
          norm_and_round(diff))

    tmp = base_year_measures["jobs_by_inpda"]
    write("Jobs base year share in pdas:\n%s" %
          norm_and_round(tmp))

    write("Jobs share in pdas:\n%s" %
          norm_and_round(jobs_by_inpda))

    diff = jobs_by_inpda - base_year_measures["jobs_by_inpda"]
    write("Jobs pct of regional growth in pdas:\n%s" %
          norm_and_round(diff))

    tmp = base_year_measures["hh_by_intpp"]
    write("Households base year share in tpps:\n%s" %
          norm_and_round(tmp))

    write("Households share in tpps:\n%s" %
          norm_and_round(hh_by_intpp))

    diff = hh_by_intpp - base_year_measures["hh_by_intpp"]
    write("Households pct of regional growth in tpps:\n%s" %
          norm_and_round(diff))

    tmp = base_year_measures["jobs_by_intpp"]
    write("Jobs base year share in tpps:\n%s" %
          norm_and_round(tmp))

    write("Jobs share in tpps:\n%s" %
          norm_and_round(jobs_by_intpp))

    diff = jobs_by_intpp - base_year_measures["jobs_by_intpp"]
    write("Jobs pct of regional growth in tpps:\n%s" %
          norm_and_round(diff))

    write("Base year dwelling unit raw capacity:\n%s" %
          base_year_measures["capacity"])

    write("Dwelling unit raw capacity:\n%s" % capacity)

    if summary.parcel_output is not None:
        df = summary.parcel_output
        # we mark greenfield as a parcel with less than 500 current sqft
        greenfield = df.total_sqft < 500

        write("Current share of projects which are greenfield development:\n%s"
              % norm_and_round(greenfield.value_counts()))

        write("Current share of units which are greenfield development:\n%s" %
              norm_and_round(df.residential_units.groupby(greenfield).sum()))

    cmap = settings["county_id_tm_map"]
    jobs_by_county = jobs.zone_id.map(taz_geography.county)\
        .map(cmap).value_counts()
    households_by_county = households.zone_id.map(taz_geography.county)\
        .map(cmap).value_counts()
    jobs_by_housing = jobs_by_county / households_by_county.replace(0, 1)
    write("Jobs/housing balance:\n" + str(jobs_by_housing))

    for geo, typ, corr in compare_to_targets(parcels, buildings, jobs,
                                             households, abag_targets,
                                             write_comparison_dfs=True):
        write("{} in {} have correlation of {:,.4f} with targets".format(
            typ, geo, corr
        ))

    f.close()


def compare_to_targets(parcels, buildings, jobs, households, abag_targets,
                       write_comparison_dfs=False):

    # yes a similar join is used in the summarize step below - but it's
    # better to keep it clean and separate

    abag_targets = abag_targets.to_frame()
    abag_targets["pda_fill_juris"] = abag_targets["joinkey"].\
        replace("Non-PDA", np.nan).replace("Total", np.nan).\
        str.upper().fillna(abag_targets.juris)

    households_df = orca.merge_tables(
        'households',
        [parcels, buildings, households],
        columns=['pda', 'juris'])

    households_df["pda_fill_juris"] = \
        households_df.pda.str.upper().replace("Total", np.nan).\
        str.upper().fillna(households_df.juris)

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['pda', 'juris'])

    jobs_df["pda_fill_juris"] = \
        jobs_df.pda.str.upper().fillna(jobs_df.juris)

    # compute correlection for pda and juris, for households and for jobs

    l = []

    df = pd.DataFrame()

    for geo in ['pda_fill_juris', 'juris']:

        for typ in ['households', 'jobs']:

            abag_distribution = abag_targets.groupby(geo)[typ].sum()

            df["abag_"+geo+"_"+typ] = abag_distribution

            agents = {
                "households": households_df,
                "jobs": jobs_df
            }[typ]

            baus_distribution = agents.groupby(geo).size().\
                reindex(abag_distribution.index).fillna(0)

            df["baus_"+geo+"_"+typ] = baus_distribution

            assert len(abag_distribution) == len(baus_distribution)

            l.append((geo, typ, abag_distribution.corr(baus_distribution)))

    if write_comparison_dfs:

        run_number = orca.get_injectable("run_number")
        year = orca.get_injectable("year")

        df.to_csv(os.path.join("runs", "run%d_targets_comparison_%d.csv" %
                  (run_number, year)))

    return l


@orca.step()
def diagnostic_output(households, buildings, parcels, taz, jobs, settings,
                      zones, year, summary, run_number, residential_units):
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
    zones['job_spaces'] = buildings.groupby('zone_id').\
        job_spaces.sum()
    tothh = households.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['residential_vacancy'] = \
        1.0 - tothh / zones.residential_units.replace(0, 1)
    zones['non_residential_sqft'] = buildings.groupby('zone_id').\
        non_residential_sqft.sum()
    totjobs = jobs.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['non_residential_vacancy'] = \
        1.0 - totjobs / zones.job_spaces.replace(0, 1)

    zones['retail_sqft'] = buildings.query('general_type == "Retail"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['office_sqft'] = buildings.query('general_type == "Office"').\
        groupby('zone_id').non_residential_sqft.sum()
    zones['industrial_sqft'] = buildings.query(
        'general_type == "Industrial"').\
        groupby('zone_id').non_residential_sqft.sum()

    zones['average_income'] = households.groupby('zone_id').income.quantile()
    zones['household_size'] = households.groupby('zone_id').persons.quantile()

    zones['building_count'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').size()
    # this price is the max of the original unit vector belows
    zones['residential_price'] = buildings.\
        query('general_type == "Residential"').groupby('zone_id').\
        residential_price.quantile()
    # these two are the original unit prices averaged up to the building id
    ru = residential_units
    zones['unit_residential_price'] = \
        ru.unit_residential_price.groupby(ru.zone_id).quantile()
    zones['unit_residential_rent'] = \
        ru.unit_residential_rent.groupby(ru.zone_id).quantile()
    cap_rate = settings.get('cap_rate')
    # this compares price to rent and shows us where price is greater
    # rents are monthly and a cap rate is applied in order to do the conversion
    zones['unit_residential_price_>_rent'] = \
        (zones.unit_residential_price >
         (zones.unit_residential_rent * 12 / cap_rate)).astype('int')

    zones['retail_rent'] = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['office_rent'] = buildings[buildings.general_type == "Office"].\
        groupby('zone_id').non_residential_rent.quantile()
    zones['industrial_rent'] = \
        buildings[buildings.general_type == "Industrial"].\
        groupby('zone_id').non_residential_rent.quantile()

    zones['retail_sqft'] = buildings[buildings.general_type == "Retail"].\
        groupby('zone_id').non_residential_sqft.sum()

    zones['retail_to_res_units_ratio'] = \
        zones.retail_sqft / zones.residential_units.replace(0, 1)

    summary.add_zone_output(zones, "diagnostic_outputs", year)

    # save the dropped buildings to a csv
    if "dropped_buildings" in orca.orca._TABLES:
        df = orca.get_table("dropped_buildings").to_frame()
        print "Dropped buildings", df.describe()
        df.to_csv(
            "runs/run{}_dropped_buildings.csv".format(run_number)
        )


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, taz_geography,
                       run_number, year, summary, final_year):
    # using the following conditional b/c `year` is used to pull a column
    # from a csv based on a string of the year in add_population()
    # and in add_employment() and 2009 is the
    # 'base'/pre-simulation year, as is the 2010 value in the csv.
    if year == 2009:
        year = 2010
        base = True
    else:
        base = False

    households_df = orca.merge_tables(
        'households',
        [parcels, buildings, households],
        columns=['pda', 'zone_id', 'juris', 'superdistrict',
                 'persons', 'income', 'base_income_quartile'])

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['pda', 'superdistrict', 'juris', 'zone_id', 'empsix'])

    buildings_df = orca.merge_tables(
        'buildings',
        [parcels, buildings],
        columns=['pda', 'superdistrict', 'juris', 'building_type',
                 'zone_id', 'residential_units', 'building_sqft',
                 'non_residential_sqft'])

    parcel_output = summary.parcel_output

    # because merge_tables returns multiple zone_id_'s, but not the one we need
    buildings_df = buildings_df.rename(columns={'zone_id_x': 'zone_id'})

    geographies = ['superdistrict', 'pda', 'juris']

    if year in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:

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
            summary_table = \
                summary_table.reindex(all_summary_geographies).fillna(0)

            # turns out the lines above had to be moved up - if there are no
            # households in a geography the index is missing that geography
            # right off the bat.  then when we try and add a jobs or buildings
            # aggregation that HAS that geography, it doesn't get saved.  ahh
            # pandas, so powerful but so darn confusing.

            # income quartile counts
            summary_table['hhincq1'] = \
                households_df.query("base_income_quartile == 1").\
                groupby(geography).size()
            summary_table['hhincq2'] = \
                households_df.query("base_income_quartile == 2").\
                groupby(geography).size()
            summary_table['hhincq3'] = \
                households_df.query("base_income_quartile == 3").\
                groupby(geography).size()
            summary_table['hhincq4'] = \
                households_df.query("base_income_quartile == 4").\
                groupby(geography).size()

            # residential buildings by type
            summary_table['sfdu'] = buildings_df.\
                query("building_type == 'HS' or building_type == 'HT'").\
                groupby(geography).residential_units.sum()
            summary_table['mfdu'] = buildings_df.\
                query("building_type == 'HM' or building_type == 'MR'").\
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
                (summary_table['sfdu'] + summary_table['mfdu'])
            summary_table['non_residential_sqft'] = buildings_df.\
                groupby(geography)['non_residential_sqft'].sum()
            summary_table['sq_ft_per_employee'] = \
                summary_table['non_residential_sqft'] / summary_table['totemp']

            if parcel_output is not None:
                parcel_output['subsidized_units'] = \
                    parcel_output.deed_restricted_units - \
                    parcel_output.inclusionary_units

                # columns re: affordable housing
                summary_table['deed_restricted_units'] = \
                    parcel_output.groupby(geography).\
                    deed_restricted_units.sum()
                summary_table['inclusionary_units'] = \
                    parcel_output.groupby(geography).inclusionary_units.sum()
                summary_table['subsidized_units'] = \
                    parcel_output.groupby(geography).subsidized_units.sum()
                summary_table['inclusionary_revenue_reduction'] = \
                    parcel_output.groupby(geography).\
                    policy_based_revenue_reduction.sum()
                summary_table['inclusionary_revenue_reduction_per_unit'] = \
                    summary_table.inclusionary_revenue_reduction / \
                    summary_table.inclusionary_units
                summary_table['total_subsidy'] = \
                    parcel_output[parcel_output.subsidized_units > 0].\
                    groupby(geography).max_profit.sum() * -1
                summary_table['subsidy_per_unit'] = \
                    summary_table.total_subsidy / \
                    summary_table.subsidized_units

            if base is False:
                summary_csv = "runs/run{}_{}_summaries_{}.csv".\
                    format(run_number, geography, year)
            elif base is True:
                summary_csv = "runs/run{}_{}_summaries_{}.csv".\
                    format(run_number, geography, 2009)
            summary_table.to_csv(summary_csv)

    if year == final_year:

        # Write Summary of Accounts
        for acct_name, acct in orca.get_injectable("coffer").iteritems():
            fname = "runs/run{}_acctlog_{}_{}.csv".\
                format(run_number, acct_name, year)
            acct.to_frame().to_csv(fname)

        # Write Urban Footprint Summary
        buildings_uf_df = orca.merge_tables(
            'buildings',
            [parcels, buildings],
            columns=['urban_footprint', 'year_built',
                     'acres', 'residential_units',
                     'non_residential_sqft'])

        buildings_uf_df['count'] = 1

        s1 = buildings_uf_df['residential_units'] / buildings_uf_df['acres']
        s2 = s1 > 1
        s3 = (buildings_uf_df['urban_footprint'] == 0) * 1
        buildings_uf_df['denser_greenfield'] = s3 * s2

        df = buildings_uf_df.\
            loc[buildings_uf_df['year_built'] > 2010].\
            groupby('urban_footprint').sum()
        df = df[['count', 'residential_units', 'non_residential_sqft',
                 'acres']]

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
        uf_summary_csv = "runs/run{}_urban_footprint_summary_{}.csv".\
            format(run_number, year)
        df.to_csv(uf_summary_csv)


@orca.step()
def building_summary(parcels, run_number, year,
                     buildings,
                     initial_year, final_year):

    if year not in [initial_year, final_year]:
        return

    df = orca.merge_tables(
        'buildings',
        [parcels, buildings],
        columns=['performance_zone', 'year_built', 'residential_units',
                 'unit_price', 'zone_id', 'non_residential_sqft',
                 'deed_restricted_units', 'job_spaces', 'x', 'y'])

    df.to_csv(
        os.path.join("runs", "run%d_building_data_%d.csv" %
                     (run_number, year))
    )


@orca.step()
def parcel_summary(parcels, buildings, households, jobs,
                   run_number, year,
                   parcels_zoning_calculations,
                   initial_year, final_year):

    if year not in [initial_year, final_year]:
        return

    df = parcels.to_frame([
        "x", "y",
        "total_residential_units",
        "total_job_spaces",
        "first_building_type"
    ])

    df2 = parcels_zoning_calculations.to_frame([
        "zoned_du",
        "zoned_du_underbuild",
        "zoned_du_underbuild_nodev"
    ])

    df = df.join(df2)

    households_df = orca.merge_tables(
        'households',
        [buildings, households],
        columns=['parcel_id', 'base_income_quartile'])

    # add households by quartile on each parcel
    for i in range(1, 5):
        df['hhq%d' % i] = households_df[
            households_df.base_income_quartile == i].\
            parcel_id.value_counts()

    jobs_df = orca.merge_tables(
        'jobs',
        [buildings, jobs],
        columns=['parcel_id', 'empsix'])

    # add jobs by empsix category on each parcel
    for cat in jobs_df.empsix.unique():
        df[cat] = jobs_df[jobs_df.empsix == cat].\
            parcel_id.value_counts()

    df.to_csv(
        os.path.join("runs", "run%d_parcel_data_%d.csv" %
                     (run_number, year))
    )

    if year == final_year:

        # do diff with initial year

        df2 = pd.read_csv(
            os.path.join("runs", "run%d_parcel_data_%d.csv" %
                         (run_number, initial_year)), index_col="parcel_id")

        for col in df.columns:

            if col in ["x", "y", "first_building_type"]:
                continue

            df[col] = df[col] - df2[col]

        df.to_csv(
            os.path.join("runs", "run%d_parcel_data_diff.csv" %
                         run_number)
        )


@orca.step()
def travel_model_output(parcels, households, jobs, buildings,
                        zones, year, summary, coffer,
                        zone_forecast_inputs, run_number,
                        taz, base_year_summary_taz, taz_geography):

    if year not in [2010, 2015, 2020, 2025, 2030, 2035, 2040]:
        # only summarize for years which are multiples of 5
        return

    taz_df = pd.DataFrame(index=zones.index)

    taz_df["sd"] = taz_geography.superdistrict
    taz_df["zone"] = zones.index
    taz_df["county"] = taz_geography.county

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['zone_id', 'empsix']
    )

    # totally baffled by this - after joining the three tables we have three
    # zone_ids, one from the parcel table, one from buildings, and one from
    # jobs and the one called zone_id has null values while there others do not
    # going to change this while I think about this - turns out this has to do
    # with major caching issue which has been reported upstream
    jobs_df["zone_id"] = jobs_df.zone_id_x

    def getsectorcounts(sector):
        return jobs_df.query("empsix == '%s'" % sector).\
            groupby('zone_id').size()

    taz_df["agrempn"] = getsectorcounts("AGREMPN")
    taz_df["fpsempn"] = getsectorcounts("FPSEMPN")
    taz_df["herempn"] = getsectorcounts("HEREMPN")
    taz_df["retempn"] = getsectorcounts("RETEMPN")
    taz_df["mwtempn"] = getsectorcounts("MWTEMPN")
    taz_df["othempn"] = getsectorcounts("OTHEMPN")
    taz_df["totemp"] = jobs_df.groupby('zone_id').size()

    households_df = households.to_frame(['zone_id',
                                         'base_income_quartile',
                                         'income',
                                         'persons'])

    def gethhcounts(filter):
        return households_df.query(filter).groupby('zone_id').size()

    taz_df["hhincq1"] = gethhcounts("base_income_quartile == 1")
    taz_df["hhincq2"] = gethhcounts("base_income_quartile == 2")
    taz_df["hhincq3"] = gethhcounts("base_income_quartile == 3")
    taz_df["hhincq4"] = gethhcounts("base_income_quartile == 4")
    taz_df["hhpop"] = households_df.groupby('zone_id').persons.sum()
    taz_df["tothh"] = households_df.groupby('zone_id').size()

    taz_df["shpop62p"] = zone_forecast_inputs.sh_62plus
    taz_df["gqpop"] = zone_forecast_inputs["gqpop" + str(year)[-2:]].fillna(0)

    taz_df["totacre"] = zone_forecast_inputs.totacre_abag
    # total population = group quarters plus households population
    taz_df["totpop"] = (taz_df.hhpop + taz_df.gqpop).fillna(0)
    taz_df["density"] = \
        (taz_df.totpop + (2.5 * taz_df.totemp)) / taz_df.totacre
    taz_df["areatype"] = pd.cut(
        taz_df.density,
        bins=[0, 6, 30, 55, 100, 300, np.inf],
        labels=[5, 4, 3, 2, 1, 0]
    )

    buildings_df = buildings.to_frame(['zone_id',
                                       'building_type',
                                       'residential_units',
                                       'building_sqft',
                                       'lot_size_per_unit'])

    taz_df["res_units"] = buildings_df.\
        groupby('zone_id').residential_units.sum()

    taz_df["mfdu"] = buildings_df.\
        query("building_type == 'HM' or building_type == 'MR'").\
        groupby('zone_id').residential_units.sum()

    taz_df["sfdu"] = buildings_df.\
        query("building_type == 'HS' or building_type == 'HT'").\
        groupby('zone_id').residential_units.sum()

    f = orca.get_injectable('parcel_first_building_type_is')

    def count_acres_with_mask(mask):
        mask *= parcels.acres
        return mask.groupby(parcels.zone_id).sum()

    taz_df["resacre_unweighted"] = count_acres_with_mask(
        f('residential') | f('mixedresidential'))

    taz_df["ciacre_unweighted"] = count_acres_with_mask(
        f('select_non_residential'))

    taz_df["ciacre"] = scaled_ciacre(
        base_year_summary_taz.CIACRE_UNWEIGHTED, taz_df.ciacre_unweighted)
    taz_df["resacre"] = scaled_resacre(
        base_year_summary_taz.RESACRE_UNWEIGHTED, taz_df.resacre_unweighted)

    taz_df = add_population(taz_df, year)
    taz_df = add_employment(taz_df, year)
    taz_df = add_age_categories(taz_df, year)

    summary.add_zone_output(taz_df, "travel_model_output", year)
    summary.write_zone_output()

    # otherwise it loses precision
    if summary.parcel_output is not None\
            and "geom_id" in summary.parcel_output:
        summary.parcel_output["geom_id"] = \
            summary.parcel_output.geom_id.astype('str')

    summary.write_parcel_output(add_xy={
        "xy_table": "parcels",
        "foreign_key": "parcel_id",
        "x_col": "x",
        "y_col": "y"
    })

    # uppercase columns to match travel model template
    taz_df.columns = \
        [x.upper() for x in taz_df.columns]

    taz_df.fillna(0).to_csv(
        "runs/run{}_taz_summaries_{}.csv".format(run_number, year))


     # aggregate TAZ summaries to create county summaries

    county_df = pd.DataFrame(index=[1,2,3,4,5,6,7,8,9])

    county_df["COUNTY"] = county_df.index

    taz_cols = ["AGREMPN", "FPSEMPN", "HEREMPN", "RETEMPN", "MWTEMPN",
    "OTHEMPN", "TOTEMP", "HHINCQ1", "HHINCQ2", "HHINCQ3", "HHINCQ4",
    "HHPOP", "TOTHH", "SHPOP62P", "GQPOP", "TOTACRE","TOTPOP", "RES_UNITS",
    "MFDU", "SFDU", "RESACRE_UNWEIGHTED", "CIACRE_UNWEIGHTED", "EMPRES",
    "AGE0004", "AGE0519", "AGE2044", "AGE4564", "AGE65P"]

    for col in taz_cols:
        taz_df_grouped = taz_df.groupby('COUNTY').sum()
        county_df[col] = taz_df_grouped[col]

    county_df["DENSITY"] = \
        (county_df.TOTPOP + (2.5 * county_df.TOTEMP)) / county_df.TOTACRE

    county_df["AREATYPE"] = pd.cut(
        county_df.DENSITY,
        bins=[0, 6, 30, 55, 100, 300, np.inf],
        labels=[5, 4, 3, 2, 1, 0]
    )

    base_year_summary_taz = \
    base_year_summary_taz.to_frame()
    base_year_summary_county = \
    base_year_summary_taz.groupby('COUNTY').sum()
    base_year_summary_county_ciacre = \
    base_year_summary_county['CIACRE_UNWEIGHTED']
    base_year_summary_county_resacre = \
    base_year_summary_county['RESACRE_UNWEIGHTED']

    county_df["CIACRE"] = scaled_ciacre(
        base_year_summary_county_ciacre, county_df.CIACRE_UNWEIGHTED)
    county_df["RESACRE"] = scaled_resacre(
        base_year_summary_county_resacre, county_df.RESACRE_UNWEIGHTED)

    county_df = county_df[["COUNTY", "AGREMPN", "FPSEMPN", "HEREMPN", "RETEMPN",
    "MWTEMPN", "OTHEMPN", "TOTEMP", "HHINCQ1", "HHINCQ2", "HHINCQ3", "HHINCQ4",
    "HHPOP", "TOTHH", "SHPOP62P", "GQPOP", "TOTACRE", "TOTPOP", "DENSITY",
    "AREATYPE", "RES_UNITS", "MFDU", "SFDU", "RESACRE_UNWEIGHTED",
    "CIACRE_UNWEIGHTED", "CIACRE", "RESACRE", "EMPRES", "AGE0004", "AGE0519",
    "AGE2044", "AGE4564", "AGE65P"]]

    county_df.fillna(0).to_csv(
        "runs/run{}_county_summaries_{}.csv".format(run_number, year))


def scaled_ciacre(mtcc, us_outc):
    zfi = zone_forecast_inputs()
    abgc = zfi.ciacre10_abag
    sim_difference = [us_outc - mtcc][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgc + sim_difference
    return combined_acres


def scaled_resacre(mtcr, us_outr):
    zfi = zone_forecast_inputs()
    abgr = zfi.resacre10_abag
    sim_difference = [us_outr - mtcr][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgr + sim_difference
    return combined_acres


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

    s = scale_by_target(s, target, .15)

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

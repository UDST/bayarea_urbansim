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
def topsheet(households, jobs, buildings, parcels, zones, year, run_number, taz_geography, parcels_zoning_calculations,
             summary, parcels_geography, new_tpp_id, residential_units, mapping):

    hh_by_subregion = misc.reindex(taz_geography.subregion, households.zone_id).value_counts()

    # Cols for Draft/Final Blueprint and EIR geographies       
    households_df = orca.merge_tables('households', [parcels_geography, buildings, households],
                                      columns=['pda_id', 'tra_id', 'ppa_id', 'sesit_id', 'income'])

    hh_by_inpda = households_df.pda_id.notnull().value_counts()
    hhincome_by_inpda = households_df.income.groupby(households_df.pda_id.notnull()).mean()
    # round to nearest 100s
    hhincome_by_inpda = (hhincome_by_inpda/100).round()*100

    hh_by_intra = households_df.tra_id.notnull().value_counts()
    hhincome_by_intra = households_df.income.groupby(households_df.tra_id.notnull()).mean()
    # round to nearest 100s
    hhincome_by_intra = (hhincome_by_intra/100).round()*100

    hh_by_insesit = households_df.sesit_id.notnull().value_counts()
    hhincome_by_insesit = households_df.income.groupby(households_df.sesit_id.notnull()).mean()
    # round to nearest 100s
    hhincome_by_insesit = (hhincome_by_insesit/100).round()*100

    jobs_by_subregion = misc.reindex(taz_geography.subregion, jobs.zone_id).value_counts()

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs], columns=['pda_id', 'tra_id'])

    jobs_by_inpda = jobs_df.pda_id.notnull().value_counts()
    jobs_by_intra = jobs_df.tra_id.notnull().value_counts()

    if year == 2010:
        # save some info for computing growth measures
        orca.add_injectable("base_year_measures", {
            "hh_by_subregion": hh_by_subregion,
            "jobs_by_subregion": jobs_by_subregion,
            "hh_by_inpda": hh_by_inpda,
            "hh_by_intra": hh_by_intra,
            "hh_by_insesit": hh_by_insesit,
            "jobs_by_inpda": jobs_by_inpda,
            "jobs_by_intra": jobs_by_intra,
            "hhincome_by_intra": hhincome_by_intra,
            "hhincome_by_insesit": hhincome_by_insesit
        })
    try:
        base_year_measures = orca.get_injectable("base_year_measures")
    except Exception as e:
        # the base year measures don't exist - we didn't run year 2010
        # this can happen when we skip the first year, usually because
        # we don't want to waste time doing so
        return

    f = open(os.path.join(orca.get_injectable("outputs_dir"), "run%d_topsheet_%d.log" % (run_number, year)), "w")

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
    orca.add_injectable("unplaced_hh", n)

    n = len(jobs.building_id[jobs.building_id == -1])
    write("Number of unplaced jobs = %d" % n)

    # we should assert there are no unplaces households and jobs right?
    # this is considered an error for the MTC-style model
    # could be configured in settings.yaml

    n = buildings.vacant_res_units[buildings.vacant_res_units < 0]
    write("Number of overfull buildings = %d" % len(n))
    write("Number of vacant units in overfull buildings = %d" % n.sum())

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

    write("Base year mean income by whether household is in tra:\n%s" % base_year_measures["hhincome_by_intra"])
    write("Forecast year mean income by whether household is in tra:\n%s" % hhincome_by_intra)
    write("Base year mean income by whether household is in hra/dr:\n%s" % base_year_measures["hhincome_by_insesit"])
    write("Forecast year mean income by whether household is in hra/dr:\n%s" % hhincome_by_insesit)

    jsp = buildings.job_spaces.sum()
    write("Number of job spaces = %d" % jsp)
    write("Non-residential vacancy rate = %.2f" % (1-0 - float(nj)/jsp))

    tmp = base_year_measures["hh_by_subregion"]
    write("Households base year share by subregion:\n%s" % norm_and_round(tmp))

    write("Households share by subregion:\n%s" % norm_and_round(hh_by_subregion))
    diff = hh_by_subregion - base_year_measures["hh_by_subregion"]

    write("Households pct of regional growth by subregion:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["jobs_by_subregion"]
    write("Jobs base year share by subregion:\n%s" % norm_and_round(tmp))

    write("Jobs share by subregion:\n%s" % norm_and_round(jobs_by_subregion))
    diff = jobs_by_subregion - base_year_measures["jobs_by_subregion"]

    write("Jobs pct of regional growth by subregion:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["hh_by_inpda"]
    write("Households base year share in pdas:\n%s" % norm_and_round(tmp))

    write("Households share in pdas:\n%s" % norm_and_round(hh_by_inpda))

    diff = hh_by_inpda - base_year_measures["hh_by_inpda"]
    write("Households pct of regional growth in pdas:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["jobs_by_inpda"]
    write("Jobs base year share in pdas:\n%s" % norm_and_round(tmp))

    write("Jobs share in pdas:\n%s" % norm_and_round(jobs_by_inpda))

    diff = jobs_by_inpda - base_year_measures["jobs_by_inpda"]
    write("Jobs pct of regional growth in pdas:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["hh_by_intra"]
    write("Households base year share in tras:\n%s" % norm_and_round(tmp))

    write("Households share in tras:\n%s" % norm_and_round(hh_by_intra))

    diff = hh_by_intra - base_year_measures["hh_by_intra"]
    write("Households pct of regional growth in tras:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["jobs_by_intra"]
    write("Jobs base year share in tras:\n%s" % norm_and_round(tmp))

    write("Jobs share in tras:\n%s" % norm_and_round(jobs_by_intra))

    diff = jobs_by_intra - base_year_measures["jobs_by_intra"]
    write("Jobs pct of regional growth in tras:\n%s" % norm_and_round(diff))

    tmp = base_year_measures["hh_by_insesit"]
    write("Households base year share in hra/drs:\n%s" % norm_and_round(tmp))

    write("Households share in hra/drs:\n%s" % norm_and_round(hh_by_insesit))

    diff = hh_by_insesit - base_year_measures["hh_by_insesit"]
    write("Households pct of regional growth in hra/drs:\n%s" % norm_and_round(diff))

    if summary.parcel_output is not None:
        df = summary.parcel_output
        # we mark greenfield as a parcel with less than 500 current sqft
        greenfield = df.total_sqft < 500

        write("Current share of projects which are greenfield development:\n%s"
              % norm_and_round(greenfield.value_counts()))

        write("Current share of units which are greenfield development:\n%s" %
              norm_and_round(df.residential_units.groupby(greenfield).sum()))

    cmap = mapping["county_id_tm_map"]
    jobs_by_county = jobs.zone_id.map(taz_geography.county).map(cmap).value_counts()
    households_by_county = households.zone_id.map(taz_geography.county).map(cmap).value_counts()
    jobs_by_housing = jobs_by_county / households_by_county.replace(0, 1)
    write("Jobs/housing balance:\n" + str(jobs_by_housing))

    f.close()


@orca.step()
def building_summary(parcels, run_number, year,
                     buildings,
                     initial_year, final_year):

    if year not in [initial_year, 2015, final_year]:
        return

    df = orca.merge_tables(
        'buildings',
        [parcels, buildings],
        columns=['performance_zone', 'year_built', 'building_type',
                 'residential_units', 'unit_price', 'zone_id', 
                 'non_residential_sqft', 'vacant_res_units', 
                 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces',
                 'x', 'y', 'geom_id', 'source'])

    df.to_csv(
        os.path.join(orca.get_injectable("outputs_dir"), "run%d_building_data_%d.csv" %
                     (run_number, year))
    )


@orca.step()
def parcel_summary(parcels, buildings, households, jobs, run_number, year, parcels_zoning_calculations,
                   initial_year, final_year, parcels_geography):

    # if year not in [2010, 2015, 2035, 2050]:
    #     return

    df = parcels.to_frame([
        "geom_id",
        "x", "y",
        "total_job_spaces",
        "first_building_type"
    ])

    df2 = parcels_zoning_calculations.to_frame([
        "zoned_du",
        "zoned_du_underbuild"
    ])

    df = df.join(df2)

    # bringing in zoning modifications growth geography tag
    join_col = 'zoningmodcat'
    
    if join_col in parcels_geography.to_frame().columns:
        parcel_gg = parcels_geography.to_frame(["parcel_id", join_col, "juris"])
        df = df.merge(parcel_gg, on='parcel_id', how='left')

    households_df = orca.merge_tables('households', [buildings, households], columns=['parcel_id', 'base_income_quartile'])

    # add households by quartile on each parcel
    for i in range(1, 5):
        df['hhq%d' % i] = households_df[households_df.base_income_quartile == i].parcel_id.value_counts()
    df["tothh"] = households_df.groupby('parcel_id').size()

    building_df = orca.merge_tables('buildings', [parcels, buildings],
                                    columns=['parcel_id', 'residential_units', 'deed_restricted_units', 
                                             'preserved_units', 'inclusionary_units', 'subsidized_units'])
    df['residential_units'] = building_df.groupby('parcel_id')['residential_units'].sum()
    df['deed_restricted_units'] = building_df.groupby('parcel_id')['deed_restricted_units'].sum()
    df['preserved_units'] = building_df.groupby('parcel_id')['preserved_units'].sum()
    df['inclusionary_units'] = building_df.groupby('parcel_id')['inclusionary_units'].sum()
    df['subsidized_units'] = building_df.groupby('parcel_id')['subsidized_units'].sum()

    jobs_df = orca.merge_tables('jobs', [buildings, jobs], columns=['parcel_id', 'empsix'])

    # add jobs by empsix category on each parcel
    for cat in jobs_df.empsix.unique():
        df[cat] = jobs_df[jobs_df.empsix == cat].parcel_id.value_counts()
    df["totemp"] = jobs_df.groupby('parcel_id').size()

    df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" % (run_number, year)))

    # if year == final_year:
    print('year printed for debug: {}'.format(year))
    if year not in [2010, 2015]:
        print('calculate diff for year {}'.format(year))
        # do diff with initial year

        df2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_%d.csv" %
                          (run_number, initial_year)), index_col="parcel_id")

        for col in df.columns:

            if col in ["x", "y", "first_building_type", "juris", join_col]:
                continue

            # fill na with 0 for parcels with no building in either base year or current year
            df[col].fillna(0, inplace=True)
            df2[col].fillna(0, inplace=True)
            
            df[col] = df[col] - df2[col]

        df.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "run%d_parcel_data_diff.csv" % run_number))

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


@orca.step()
def hazards_slr_summary(run_setup, run_number, year, households, jobs, parcels):

    if run_setup['run_slr']:

        destroy_parcels = orca.get_table("destroy_parcels")
        if len(destroy_parcels) > 0:

            def write(s):
                # print s
                f.write(s + "\n\n")

            f = open(os.path.join(orca.get_injectable("outputs_dir"), "run%d_hazards_slr_%d.log" %
                     (run_number, year)), "w")

            n = len(destroy_parcels)
            write("Number of impacted parcels = %d" % n)

            try:
                slr_demolish_cum = orca.get_table("slr_demolish_cum").to_frame()
            except Exception as e:
                slr_demolish_cum = pd.DataFrame()
            slr_demolish = orca.get_table("slr_demolish").to_frame()
            slr_demolish_cum = slr_demolish.append(slr_demolish_cum)
            orca.add_table("slr_demolish_cum", slr_demolish_cum)

            n = slr_demolish_cum['residential_units'].sum()
            write("Number of impacted residential units = %d" % n)
            n = slr_demolish_cum['building_sqft'].sum()
            write("Number of impacted building sqft = %d" % n)

            # income quartile counts
            try:
                hh_unplaced_slr_cum = \
                    orca.get_table("hh_unplaced_slr_cum").to_frame()
            except Exception as e:
                hh_unplaced_slr_cum = pd.DataFrame()
            hh_unplaced_slr = orca.get_injectable("hh_unplaced_slr")
            hh_unplaced_slr_cum = hh_unplaced_slr.append(hh_unplaced_slr_cum)
            orca.add_table("hh_unplaced_slr_cum", hh_unplaced_slr_cum)

            write("Number of impacted households by type")
            hs = pd.DataFrame(index=[0])
            hs['hhincq1'] = \
                (hh_unplaced_slr_cum["base_income_quartile"] == 1).sum()
            hs['hhincq2'] = \
                (hh_unplaced_slr_cum["base_income_quartile"] == 2).sum()
            hs['hhincq3'] = \
                (hh_unplaced_slr_cum["base_income_quartile"] == 3).sum()
            hs['hhincq4'] = \
                (hh_unplaced_slr_cum["base_income_quartile"] == 4).sum()
            hs.to_string(f, index=False)

            write("")

            # employees by sector
            try:
                jobs_unplaced_slr_cum = \
                    orca.get_table("jobs_unplaced_slr_cum").to_frame()
            except Exception as e:
                jobs_unplaced_slr_cum = pd.DataFrame()
            jobs_unplaced_slr = orca.get_injectable("jobs_unplaced_slr")
            jobs_unplaced_slr_cum = jobs_unplaced_slr.append(jobs_unplaced_slr_cum)
            orca.add_table("jobs_unplaced_slr_cum", jobs_unplaced_slr_cum)

            write("Number of impacted jobs by sector")
            js = pd.DataFrame(index=[0])
            js['agrempn'] = (jobs_unplaced_slr_cum["empsix"] == 'AGREMPN').sum()
            js['mwtempn'] = (jobs_unplaced_slr_cum["empsix"] == 'MWTEMPN').sum()
            js['retempn'] = (jobs_unplaced_slr_cum["empsix"] == 'RETEMPN').sum()
            js['fpsempn'] = (jobs_unplaced_slr_cum["empsix"] == 'FPSEMPN').sum()
            js['herempn'] = (jobs_unplaced_slr_cum["empsix"] == 'HEREMPN').sum()
            js['othempn'] = (jobs_unplaced_slr_cum["empsix"] == 'OTHEMPN').sum()
            js.to_string(f, index=False)

            f.close()

            slr_demolish.to_csv(os.path.join(orca.get_injectable("outputs_dir"),
                                             "run%d_hazards_slr_buildings_%d.csv"
                                             % (run_number, year)))


@orca.step()
def hazards_eq_summary(run_setup, run_number, year, households, jobs, parcels, buildings):

    if run_setup['run_eq']:
        if year == 2035:

            f = open(os.path.join(orca.get_injectable("outputs_dir"), "run%d_hazards_eq_%d.log" %
                     (run_number, year)), "w")

            def write(s):
                # print s
                f.write(s + "\n\n")

            write("Number of buildings with earthquake buildings codes")
            code = orca.get_injectable("code")
            code_counts = [[x, code.count(x)] for x in set(code)]
            code_counts_df = pd.DataFrame(code_counts,
                                          columns=['building_code', 'count'])
            code_counts_df.to_string(f, index=False)

            write("")

            write("Number of buildings with fragility codes")
            fragilities = orca.get_injectable("fragilities")
            fragility_counts = [[x, fragilities.count(x)]
                                for x in set(fragilities)]
            fragility_counts_df = pd.DataFrame(fragility_counts,
                                               columns=['fragility_code', 'count'])
            fragility_counts_df.to_string(f, index=False)

            write("")

            # buildings counts
            eq_buildings = orca.get_injectable("eq_buildings")
            n = len(eq_buildings)
            write("Total number of buildings destroyed = %d" % n)
            existing_buildings = orca.get_injectable("existing_buildings")
            n = len(existing_buildings)
            write("Number of existing buildings destroyed = %d" % n)
            new_buildings = orca.get_injectable("new_buildings")
            n = len(new_buildings)
            write("Number of new buildings destroyed = %d" % n)
            fire_buildings = orca.get_injectable("fire_buildings")
            n = len(fire_buildings)
            write("Number of buildings destroyed by fire = %d" % n)

            eq_demolish = orca.get_table("eq_demolish")
            n = eq_demolish['residential_units'].sum()
            write("Number of impacted residential units = %d" % n)
            n = eq_demolish['building_sqft'].sum()
            write("Number of impacted building sqft = %d" % n)

            # income quartile counts
            write("Number of impacted households by type")
            hh_unplaced_eq = orca.get_injectable("hh_unplaced_eq")
            hh_summary = pd.DataFrame(index=[0])
            hh_summary['hhincq1'] = \
                (hh_unplaced_eq["base_income_quartile"] == 1).sum()
            hh_summary['hhincq2'] = \
                (hh_unplaced_eq["base_income_quartile"] == 2).sum()
            hh_summary['hhincq3'] = \
                (hh_unplaced_eq["base_income_quartile"] == 3).sum()
            hh_summary['hhincq4'] = \
                (hh_unplaced_eq["base_income_quartile"] == 4).sum()
            hh_summary.to_string(f, index=False)

            write("")

            # employees by sector
            write("Number of impacted jobs by sector")
            jobs_unplaced_eq = orca.get_injectable("jobs_unplaced_eq")
            jobs_summary = pd.DataFrame(index=[0])
            jobs_summary['agrempn'] = \
                (jobs_unplaced_eq["empsix"] == 'AGREMPN').sum()
            jobs_summary['mwtempn'] = \
                (jobs_unplaced_eq["empsix"] == 'MWTEMPN').sum()
            jobs_summary['retempn'] = \
                (jobs_unplaced_eq["empsix"] == 'RETEMPN').sum()
            jobs_summary['fpsempn'] = \
                (jobs_unplaced_eq["empsix"] == 'FPSEMPN').sum()
            jobs_summary['herempn'] = \
                (jobs_unplaced_eq["empsix"] == 'HEREMPN').sum()
            jobs_summary['othempn'] = \
                (jobs_unplaced_eq["empsix"] == 'OTHEMPN').sum()
            jobs_summary.to_string(f, index=False)

            f.close()

            # print out demolished buildings
            eq_demolish = eq_demolish.to_frame()
            eq_demolish_taz = misc.reindex(parcels.zone_id,
                                           eq_demolish.parcel_id)
            eq_demolish['taz'] = eq_demolish_taz
            eq_demolish['count'] = 1
            eq_demolish = eq_demolish.drop(['parcel_id', 'year_built',
                                           'redfin_sale_year'], axis=1)
            eq_demolish = eq_demolish.groupby(['taz']).sum()
            eq_demolish.to_csv(os.path.join(orca.get_injectable("outputs_dir"),
                               "run%d_hazards_eq_demolish_buildings_%d.csv"
                                            % (run_number, year)))

            # print out retrofit buildings that were saved
            if run_setup['eq_mitigation']:
                retrofit_bldgs_tot = orca.get_table("retrofit_bldgs_tot")
                retrofit_bldgs_tot = retrofit_bldgs_tot.to_frame()
                retrofit_bldgs_tot_taz = misc.reindex(parcels.zone_id,
                                                      retrofit_bldgs_tot.parcel_id)
                retrofit_bldgs_tot['taz'] = retrofit_bldgs_tot_taz
                retrofit_bldgs_tot['count'] = 1
                retrofit_bldgs_tot = retrofit_bldgs_tot[[
                    'taz', 'residential_units', 'residential_sqft',
                    'non_residential_sqft', 'building_sqft', 'stories',
                    'redfin_sale_price', 'non_residential_rent',
                    'deed_restricted_units', 'residential_price', 'count']]
                retrofit_bldgs_tot = retrofit_bldgs_tot.groupby(['taz']).sum()
                retrofit_bldgs_tot.\
                    to_csv(os.path.join(
                           orca.get_injectable("outputs_dir"), "run%d_hazards_eq_retrofit_buildings_%d.csv"
                           % (run_number, year)))

        # print out buildings in 2030, 2035, and 2050 so Horizon team can compare
        # building inventory by TAZ
        if year in [2030, 2035, 2050] and eq:
            buildings = buildings.to_frame()
            buildings_taz = misc.reindex(parcels.zone_id,
                                         buildings.parcel_id)
            buildings['taz'] = buildings_taz
            buildings['count'] = 1
            buildings = buildings[['taz', 'count', 'residential_units',
                                   'residential_sqft', 'non_residential_sqft',
                                   'building_sqft', 'stories', 'redfin_sale_price',
                                   'non_residential_rent', 'deed_restricted_units',
                                   'residential_price']]
            buildings = buildings.groupby(['taz']).sum()
            buildings.to_csv(os.path.join(orca.get_injectable("outputs_dir"),
                             "run%d_hazards_eq_buildings_list_%d.csv"
                                          % (run_number, year)))


@orca.step()
def slack_report(year, base_year, slack_enabled, run_number, devproj_len, devproj_len_geomid, devproj_len_proc):

    if slack_enabled:
        from slacker import Slacker
        import socket
        slack = Slacker(os.environ["SLACK_TOKEN"])
        host = socket.gethostname()

        if year == base_year:
            dropped_devproj_geomid = devproj_len - devproj_len_geomid
            dropped_devproj_proc = devproj_len_geomid - devproj_len_proc
            slack.chat.post_message(
                '#urbansim_sim_update',
                'Development projects for run %d on %s: %d to start, '
                '%d dropped by geom_id check, '
                '%d dropped by processing'
                % (run_number, host, devproj_len, dropped_devproj_geomid, dropped_devproj_proc), as_user=True)

        unplaced_hh = orca.get_injectable("unplaced_hh")
        if unplaced_hh > 0:
            slack.chat.post_message(
                '#urbansim_sim_update',
                'WARNING: unplaced households in %d for run %d on %s'
                % (year, run_number, host), as_user=True)

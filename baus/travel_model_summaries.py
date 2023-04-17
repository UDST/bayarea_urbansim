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
def travel_model_output(parcels, households, jobs, buildings, zones, maz, year, summary, final_year,
                        tm1_taz1_forecast_inputs, run_number, base_year_summary_taz, taz_geography,
                        tm1_tm2_maz_forecast_inputs, tm1_tm2_regional_demographic_forecast, tm1_tm2_regional_controls):

    if year not in [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]:
        # only summarize for years which are multiples of 5
        return

    parcels = parcels.to_frame()
    parcels["zone_id_x"] = parcels.zone_id
    orca.add_table('parcels', parcels)
    parcels = orca.get_table("parcels")

    households_df = orca.merge_tables('households',
                                      [parcels, buildings, households],
                                      columns=['zone_id', 'zone_id_x',
                                               'base_income_quartile',
                                               'income', 'persons',
                                               'maz_id'])

    households_df["zone_id"] = households_df.zone_id_x

    taz_df = pd.DataFrame(index=zones.index)

    taz_df["sd"] = taz_geography.superdistrict
    taz_df["zone"] = zones.index
    taz_df["county"] = taz_geography.county
    taz_df["county_name"] = taz_geography.county_name

    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['zone_id', 'zone_id_x', 'empsix']
    )

    # totally baffled by this - after joining the three tables we have three
    # zone_ids, one from the parcel table, one from buildings, and one from
    # jobs and the one called zone_id has null values while there others do not
    # going to change this while I think about this - turns out this has to do
    # with major caching issue which has been reported upstream

    # the null values are present in the jobs table, however when you merge the
    # tables, the zone_id columns from the other tables don't have null values
    # however on lumodel, these duplicate columns don't get created in the
    # merge so a copy of zone_id (zone_id_x) is added to parcels to ensure
    # it doesn't get dropped

    # the same has now been repeated for households (as described with lumodel)
    # no duplicate zone_ids emerged to use, so one was created from parcels
    # a taz column existed, but was not consistently the same as zone_id

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

    def gethhcounts(filter):
        return households_df.query(filter).groupby('zone_id').size()

    taz_df["hhincq1"] = gethhcounts("base_income_quartile == 1")
    taz_df["hhincq2"] = gethhcounts("base_income_quartile == 2")
    taz_df["hhincq3"] = gethhcounts("base_income_quartile == 3")
    taz_df["hhincq4"] = gethhcounts("base_income_quartile == 4")
    taz_df["hhpop"] = households_df.groupby('zone_id').persons.sum()
    taz_df["tothh"] = households_df.groupby('zone_id').size()


    zone_forecast_inputs = tm1_taz1_forecast_inputs.to_frame()
    zone_forecast_inputs.index = zone_forecast_inputs.zone_id
    taz_df["shpop62p"] = zone_forecast_inputs.sh_62plus
    taz_df["gqpop"] = zone_forecast_inputs["gqpop" + str(year)[-2:]].fillna(0)

    taz_df["totacre"] = zone_forecast_inputs.totacre_abag
    # total population = group quarters plus households population
    taz_df["totpop"] = (taz_df.hhpop + taz_df.gqpop).fillna(0)

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
    rc = tm1_tm2_regional_controls.to_frame()
    taz_df = add_population(taz_df, year, rc)
    taz_df.totpop = taz_df.hhpop + taz_df.gqpop
    taz_df = add_employment(taz_df, year, rc)
    taz_df["density_pop"] = taz_df.totpop / taz_df.totacre
    taz_df["density_pop"] = taz_df["density_pop"].fillna(0)
    taz_df["density_emp"] = (2.5 * taz_df.totemp) / taz_df.totacre
    taz_df["density_emp"] = taz_df["density_emp"].fillna(0)
    taz_df["density"] = taz_df["density_pop"] + taz_df["density_emp"]
    taz_df["areatype"] = pd.cut(
        taz_df.density,
        bins=[0, 6, 30, 55, 100, 300, np.inf],
        labels=[5, 4, 3, 2, 1, 0]
    )
    taz_df = add_age_categories(taz_df, year, rc)
    orca.add_table('taz_summary_1', taz_df)

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

    maz = maz.to_frame(['TAZ', 'COUNTY', 'county_name', 'taz1454'])
    mazi = tm1_tm2_maz_forecast_inputs.to_frame()
    mazi_yr = str(year)[2:]
    households_df.maz_id = households_df.maz_id.fillna(213906)
    maz["hhpop"] = households_df.groupby('maz_id').persons.sum()
    maz["tothh"] = households_df.groupby('maz_id').size()
    tothh = taz_df.TOTHH.sum()
    maz = add_households(maz, tothh)
    maz['gq_type_univ'] = mazi['gqpopu' + mazi_yr]
    maz['gq_type_mil'] = mazi['gqpopm' + mazi_yr]
    maz['gq_type_othnon'] = mazi['gqpopo' + mazi_yr]
    maz['gq_tot_pop'] = maz['gq_type_univ'] + maz['gq_type_mil']\
        + maz['gq_type_othnon']
    tot_gqpop = maz.gq_tot_pop.sum()

    rdf = tm1_tm2_regional_demographic_forecast.to_frame()

    tfi = tm1_taz1_forecast_inputs.to_frame()
    tfi.index = tfi.TAZ1454
    taz_df['gq_type_univ'] = maz.groupby('taz1454'
                                         ).gq_type_univ.sum().fillna(0)
    taz_df['gq_type_mil'] = maz.groupby('taz1454').gq_type_mil.sum().fillna(0)
    taz_df['gq_type_othnon'] = maz.groupby('taz1454'
                                           ).gq_type_othnon.sum().fillna(0)
    taz_df['gq_tot_pop'] = maz.groupby('taz1454').gq_tot_pop.sum().fillna(0)

    taz_df['hh'] = taz_df.TOTHH
    taz_df['hh_size_1'] = taz_df['TOTHH'] * tfi.shrs1_2010
    taz_df['hh_size_2'] = taz_df['TOTHH'] * tfi.shrs2_2010
    taz_df['hh_size_3'] = taz_df['TOTHH'] * tfi.shrs3_2010
    taz_df['hh_size_4_plus'] = taz_df['TOTHH'] * tfi.shrs4_2010

    taz_df['county'] = maz.groupby('taz1454').COUNTY.first()
    taz_df['county_name'] = maz.groupby('taz1454').county_name.first()

    taz_df['hh_wrks_0'] = taz_df['TOTHH'] * tfi.shrw0_2010
    taz_df['hh_wrks_1'] = taz_df['TOTHH'] * tfi.shrw1_2010
    taz_df['hh_wrks_2'] = taz_df['TOTHH'] * tfi.shrw2_2010
    taz_df['hh_wrks_3_plus'] = taz_df['TOTHH'] * tfi.shrw3_2010

    taz_df['hh_kids_no'] = taz_df['TOTHH'] * tfi.shrn_2010
    taz_df['hh_kids_yes'] = taz_df['TOTHH'] * tfi.shry_2010
    taz_df = adjust_hhsize(taz_df, year, rdf, tothh)
    taz_df = adjust_hhwkrs(taz_df, year, rdf, tothh)
    taz_df = adjust_hhkids(taz_df, year, rdf, tothh)
    del taz_df['hh']

    taz_df.index.name = 'TAZ'

    taz_df.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                            "run{}_taz_summaries_{}.csv").format(run_number, year))

    # aggregate TAZ summaries to create county summaries

    county_df = pd.DataFrame(index=['San Francisco',
                                    'San Mateo',
                                    'Santa Clara',
                                    'Alameda',
                                    'Contra Costa',
                                    'Solano',
                                    'Napa',
                                    'Sonoma',
                                    'Marin'])

    county_df["COUNTY_NAME"] = county_df.index

    taz_cols = ["AGREMPN", "FPSEMPN", "HEREMPN", "RETEMPN", "MWTEMPN",
                "OTHEMPN", "TOTEMP", "HHINCQ1", "HHINCQ2", "HHINCQ3",
                "HHINCQ4", "HHPOP", "TOTHH", "SHPOP62P", "GQPOP",
                "TOTACRE", "TOTPOP", "RES_UNITS", "MFDU", "SFDU",
                "RESACRE_UNWEIGHTED", "CIACRE_UNWEIGHTED", "EMPRES",
                "AGE0004", "AGE0519", "AGE2044", "AGE4564", "AGE65P"]

    for col in taz_cols:
        taz_df_grouped = taz_df.groupby('county_name').sum()
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
        base_year_summary_taz.groupby('COUNTY_NAME').sum()
    base_year_summary_county_ciacre = \
        base_year_summary_county['CIACRE_UNWEIGHTED']
    base_year_summary_county_resacre = \
        base_year_summary_county['RESACRE_UNWEIGHTED']

    county_df["CIACRE"] = scaled_ciacre(
        base_year_summary_county_ciacre, county_df.CIACRE_UNWEIGHTED)
    county_df["RESACRE"] = scaled_resacre(
        base_year_summary_county_resacre, county_df.RESACRE_UNWEIGHTED)

    county_df = county_df[["COUNTY_NAME", "AGREMPN", "FPSEMPN", "HEREMPN",
                           "RETEMPN", "MWTEMPN", "OTHEMPN", "TOTEMP",
                           "HHINCQ1", "HHINCQ2", "HHINCQ3", "HHINCQ4",
                           "HHPOP", "TOTHH", "SHPOP62P", "GQPOP",
                           "TOTACRE", "TOTPOP", "DENSITY", "AREATYPE",
                           "RES_UNITS", "MFDU", "SFDU", "RESACRE_UNWEIGHTED",
                           "CIACRE_UNWEIGHTED", "CIACRE", "RESACRE", "EMPRES",
                           "AGE0004", "AGE0519", "AGE2044", "AGE4564",
                           "AGE65P"]]
    county_df = county_df.set_index('COUNTY_NAME')

    county_df.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                               "run{}_county_summaries_{}.csv").format(run_number, year))

    if year == final_year: 
        baseyear = 2015
        df_base = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"),
                              "run%d_taz_summaries_%d.csv" % (run_number, baseyear)))
        df_final = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"),
                               "run%d_taz_summaries_%d.csv" % (run_number, final_year)))
        df_growth = taz_calculator(run_number,
                                df_base, df_final)
        df_growth = df_growth.set_index(['RUNID', 'TAZ','SD',
                                        'SD_NAME','COUNTY','CNTY_NAME'])
        df_growth.to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                         "run%d_taz_growth_summaries.csv" % run_number))
        df_growth_c = county_calculator(run_number,
                                        df_base, df_final)
        df_growth_c.to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                           "run%d_county_growth_summaries.csv" % run_number),index = False)
    # add region marginals
    pd.DataFrame(data={'REGION': [1], 'gq_num_hh_region': [tot_gqpop]}).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                                                               "run{}_regional_marginals_{}.csv").format(run_number, year), 
                                                                               index=False)


@orca.step()
def travel_model_2_output(parcels, households, jobs, buildings, maz, year, tm2_emp27_employment_shares, run_number,
                          tm1_tm2_maz_forecast_inputs, tm2_taz2_forecast_inputs, tm2_occupation_shares, 
                          tm1_tm2_regional_demographic_forecast, tm1_tm2_regional_controls):
    if year not in [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]:
        # only summarize for years which are multiples of 5
        return

    maz = maz.to_frame(['TAZ', 'COUNTY', 'county_name', 'taz1454'])
    rc = tm1_tm2_regional_controls.to_frame()

    pcl = parcels.to_frame(['maz_id', 'acres'])
    maz['ACRES'] = pcl.groupby('maz_id').acres.sum()

    hh_df = orca.merge_tables('households',
                              [parcels, buildings, households],
                              columns=['zone_id',
                                       'base_income_quartile',
                                       'income',
                                       'persons',
                                       'maz_id'])

    hh_df.maz_id = hh_df.maz_id.fillna(213906)

    def gethhcounts(filter):
        return hh_df.query(filter).groupby('maz_id').size()

    tothh = len(hh_df)
    maz["hhincq1"] = gethhcounts("base_income_quartile == 1")
    maz["hhincq2"] = gethhcounts("base_income_quartile == 2")
    maz["hhincq3"] = gethhcounts("base_income_quartile == 3")
    maz["hhincq4"] = gethhcounts("base_income_quartile == 4")
    maz["hhpop"] = hh_df.groupby('maz_id').persons.sum()
    maz["tothh"] = hh_df.groupby('maz_id').size()
    maz = add_households(maz, tothh)

    jobs_df = orca.merge_tables('jobs',
                                [parcels, buildings, jobs],
                                columns=['maz_id', 'empsix'])

    bldg_df = orca.merge_tables('buildings',
                                [buildings, parcels],
                                columns=['maz_id', 'residential_units'])

    tm2_emp27_employment_shares = tm2_emp27_employment_shares.to_frame()

    def getsectorcounts(empsix, empsh):
        emp = jobs_df.query("empsix == '%s'" % empsix).\
            groupby('maz_id').size()
        return emp * tm2_emp27_employment_shares.loc[tm2_emp27_employment_shares.empsh == empsh,
                                         str(year)].values[0]

    maz["ag"] = getsectorcounts("AGREMPN", "ag")
    maz["natres"] = getsectorcounts("AGREMPN", "natres")

    maz["fire"] = getsectorcounts("FPSEMPN", "fire")
    maz["serv_bus"] = getsectorcounts("FPSEMPN", "serv_bus")
    maz["prof"] = getsectorcounts("FPSEMPN", "prof")
    maz["lease"] = getsectorcounts("FPSEMPN", "lease")

    maz["art_rec"] = getsectorcounts("HEREMPN", "art_rec")
    maz["serv_soc"] = getsectorcounts("HEREMPN", "serv_soc")
    maz["serv_per"] = getsectorcounts("HEREMPN", "serv_per")
    maz["ed_high"] = getsectorcounts("HEREMPN", "ed_high")
    maz["ed_k12"] = getsectorcounts("HEREMPN", "ed_k12")
    maz["ed_oth"] = getsectorcounts("HEREMPN", "ed_oth")
    maz["health"] = getsectorcounts("HEREMPN", "health")

    maz["man_tech"] = getsectorcounts("MWTEMPN", "man_tech")
    maz["man_lgt"] = getsectorcounts("MWTEMPN", "man_lgt")
    maz["logis"] = getsectorcounts("MWTEMPN", "logis")
    maz["man_bio"] = getsectorcounts("MWTEMPN", "man_bio")
    maz["transp"] = getsectorcounts("MWTEMPN", "transp")
    maz["man_hvy"] = getsectorcounts("MWTEMPN", "man_hvy")
    maz["util"] = getsectorcounts("MWTEMPN", "util")

    maz["info"] = getsectorcounts("OTHEMPN", "info")
    maz["gov"] = getsectorcounts("OTHEMPN", "gov")
    maz["constr"] = getsectorcounts("OTHEMPN", "constr")

    maz["hotel"] = getsectorcounts("RETEMPN", "hotel")
    maz["ret_loc"] = getsectorcounts("RETEMPN", "ret_loc")
    maz["ret_reg"] = getsectorcounts("RETEMPN", "ret_reg")
    maz["eat"] = getsectorcounts("RETEMPN", "eat")

    maz["emp_total"] = jobs_df.groupby('maz_id').size()

    maz = maz.fillna(0)

    emp_cols = ['ag', 'natres', 'logis', 'man_bio', 'man_hvy', 'man_lgt',
                'man_tech', 'transp', 'util', 'eat', 'hotel', 'ret_loc',
                'ret_reg', 'fire', 'lease', 'prof', 'serv_bus', 'art_rec',
                'ed_high', 'ed_k12', 'ed_oth', 'health', 'serv_per',
                'serv_soc', 'constr', 'info', 'gov']
    for i, r in maz.iterrows():
        c = r[emp_cols]
        maz.loc[i, emp_cols] = round_series_match_target(r[emp_cols],
                                                         r.emp_total, 0)

    maz['num_hh'] = maz['tothh']

    mazi = tm1_tm2_maz_forecast_inputs.to_frame()
    mazi_yr = str(year)[2:]
    maz['gq_type_univ'] = mazi['gqpopu' + mazi_yr]
    maz['gq_type_mil'] = mazi['gqpopm' + mazi_yr]
    maz['gq_type_othnon'] = mazi['gqpopo' + mazi_yr]
    maz['gq_tot_pop'] = maz['gq_type_univ'] + maz['gq_type_mil']\
        + maz['gq_type_othnon']
    maz['gqpop'] = maz['gq_tot_pop']
    maz = add_population_tm2(maz, year)

    maz['POP'] = maz.gq_tot_pop + maz.hhpop
    maz['HH'] = maz.tothh.fillna(0)

    maz['RetEmp'] = maz.hotel + maz.ret_loc + maz.ret_reg + maz.eat
    maz['ACRES'] = pcl.groupby('maz_id').acres.sum()
    maz['residential_units'] = bldg_df.groupby('maz_id').\
        residential_units.sum()
    maz['DUDen'] = maz.residential_units / maz.ACRES
    maz['EmpDen'] = maz.emp_total / maz.ACRES
    maz['RetEmpDen'] = maz.RetEmp / maz.ACRES
    maz['PopDen'] = maz.POP / maz.ACRES

    maz['hh_size_1'] = maz.tothh.fillna(0) * mazi.shrs1_2010
    maz['hh_size_2'] = maz.tothh.fillna(0) * mazi.shrs2_2010
    maz['hh_size_3'] = maz.tothh.fillna(0) * mazi.shrs3_2010
    maz['hh_size_4_plus'] = maz.tothh.fillna(0) * mazi.shs4_2010
    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    maz = adjust_hhsize(maz, year, rdf, tothh)

    taz2 = pd.DataFrame(index=tm2_taz2_forecast_inputs.index)
    t2fi = tm2_taz2_forecast_inputs.to_frame()
    taz2['hh'] = maz.groupby('TAZ').tothh.sum()
    taz2['hh_inc_30'] = maz.groupby('TAZ').hhincq1.sum().fillna(0)
    taz2['hh_inc_30_60'] = maz.groupby('TAZ').hhincq2.sum().fillna(0)
    taz2['hh_inc_60_100'] = maz.groupby('TAZ').hhincq3.sum().fillna(0)
    taz2['hh_inc_100_plus'] = maz.groupby('TAZ').hhincq4.sum().fillna(0)

    taz2['pop_hhsize1'] = maz.groupby('TAZ').hh_size_1.sum()
    taz2['pop_hhsize2'] = maz.groupby('TAZ').hh_size_2.sum() * 2
    taz2['pop_hhsize3'] = maz.groupby('TAZ').hh_size_3.sum() * 3
    taz2['pop_hhsize4'] = maz.groupby('TAZ').hh_size_4_plus.sum() * 4.781329

    taz2['pop'] = taz2.pop_hhsize1 + taz2.pop_hhsize2\
        + taz2.pop_hhsize3 + taz2.pop_hhsize4

    taz2['hhpop'] = maz.groupby('TAZ').hhpop.sum()
    taz2['county_name'] = maz.groupby('TAZ').county_name.first()

    taz2['pers_age_00_19'] = taz2['hhpop'] * t2fi.shra1_2010
    taz2['pers_age_20_34'] = taz2['hhpop'] * t2fi.shra2_2010
    taz2['pers_age_35_64'] = taz2['hhpop'] * t2fi.shra3_2010
    taz2['pers_age_65_plus'] = taz2['hhpop'] * t2fi.shra4_2010

    taz2['hh_wrks_0'] = taz2['hh'] * t2fi.shrw0_2010
    taz2['hh_wrks_1'] = taz2['hh'] * t2fi.shrw1_2010
    taz2['hh_wrks_2'] = taz2['hh'] * t2fi.shrw2_2010
    taz2['hh_wrks_3_plus'] = taz2['hh'] * t2fi.shrw3_2010

    taz2['hh_kids_no'] = taz2['hh'] * t2fi.shrn_2010
    taz2['hh_kids_yes'] = taz2['hh'] * t2fi.shry_2010
    taz2 = adjust_hhwkrs(taz2, year, rdf, tothh)
    taz2 = adjust_page(taz2, year)
    taz2 = adjust_hhkids(taz2, year, rdf, tothh)
    taz2.index.name = 'TAZ2'

    county = pd.DataFrame(index=[[4, 5, 9, 7, 1, 2, 3, 6, 8]])

    county['pop'] = maz.groupby('county_name').POP.sum()

    county[['hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']] =\
        taz2.groupby('county_name').agg({'hh_wrks_1': 'sum',
                                         'hh_wrks_2': 'sum',
                                         'hh_wrks_3_plus': 'sum'})

    county['workers'] = county.hh_wrks_1 + county.hh_wrks_2 * 2\
        + county.hh_wrks_3_plus * 3.474036

    cef = tm2_occupation_shares.to_frame()
    cef = cef.loc[cef.year == year].set_index('county_name')
    county['pers_occ_management'] = county.workers * cef.shr_occ_management
    county['pers_occ_management'] = round_series_match_target(
        county['pers_occ_management'], np.round(
            county['pers_occ_management'].sum()), 0)
    county['pers_occ_professional'] = county.workers *\
        cef.shr_occ_professional
    county['pers_occ_professional'] = round_series_match_target(
        county['pers_occ_professional'], np.round(
            county['pers_occ_professional'].sum()), 0)
    county['pers_occ_services'] = county.workers * cef.shr_occ_services
    county['pers_occ_services'] = round_series_match_target(
        county['pers_occ_services'], np.round(
            county['pers_occ_services'].sum()), 0)
    county['pers_occ_retail'] = county.workers * cef.shr_occ_retail
    county['pers_occ_retail'] = round_series_match_target(
        county['pers_occ_retail'], np.round(
            county['pers_occ_retail'].sum()), 0)
    county['pers_occ_manual'] = county.workers * cef.shr_occ_manual
    county['pers_occ_manual'] = round_series_match_target(
        county['pers_occ_manual'], np.round(
            county['pers_occ_manual'].sum()), 0)
    county['pers_occ_military'] = county.workers * cef.shr_occ_military
    county['pers_occ_military'] = round_series_match_target(
        county['pers_occ_military'], np.round(
            county['pers_occ_military'].sum()), 0)

    county['gq_tot_pop'] = maz.groupby('county_name').gq_tot_pop.sum()

    maz[['HH', 'POP', 'emp_total', 'ag', 'natres', 'logis',
         'man_bio', 'man_hvy', 'man_lgt', 'man_tech',
         'transp', 'util', 'eat', 'hotel',
         'ret_loc', 'ret_reg', 'fire', 'lease',
         'prof', 'serv_bus', 'art_rec', 'ed_high',
         'ed_k12', 'ed_oth', 'health', 'serv_per',
         'serv_soc', 'constr', 'info', 'gov',
         'DUDen', 'EmpDen', 'PopDen', 'RetEmpDen']].fillna(0).to_csv(
        os.path.join(orca.get_injectable("outputs_dir"), "run{}_maz_summaries_{}.csv".format(run_number, year)))

    maz[['num_hh', 'hh_size_1', 'hh_size_2',
         'hh_size_3', 'hh_size_4_plus', 'gq_tot_pop',
         'gq_type_univ', 'gq_type_mil', 'gq_type_othnon']].fillna(0).to_csv(
        os.path.join(orca.get_injectable("outputs_dir"), "run{}_maz_marginals_{}.csv".format(run_number, year)))

    taz2[['hh_inc_30', 'hh_inc_30_60',
          'hh_inc_60_100', 'hh_inc_100_plus',
          'hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2',
          'hh_wrks_3_plus', 'pers_age_00_19',
          'pers_age_20_34', 'pers_age_35_64',
          'pers_age_65_plus', 'hh_kids_no',
          'hh_kids_yes']].fillna(0).to_csv(
        os.path.join(orca.get_injectable("outputs_dir"), "run{}_taz2_marginals_{}.csv".format(run_number, year)))

    county[['pers_occ_management', 'pers_occ_professional',
            'pers_occ_services', 'pers_occ_retail',
            'pers_occ_manual', 'pers_occ_military',
            'gq_tot_pop']].fillna(0).to_csv(
        os.path.join(orca.get_injectable("outputs_dir"), "run{}_county_marginals_{}.csv".format(run_number, year)))


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
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 'zone_forecasts/tm1_taz1_forecast_inputs.csv'),
                       index_col="zone_id")


def add_population(df, year, tm1_tm2_regional_controls):
    rc = tm1_tm2_regional_controls
    target = rc.totpop.loc[year] - df.gqpop.sum()
    zfi = zone_forecast_inputs()
    s = df.tothh * zfi.meanhhsize

    s = scale_by_target(s, target)  # , .15

    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


def add_population_tm2(df, year, tm1_tm2_regional_controls):
    rc = tm1_tm2_regional_controls
    target = rc.totpop.loc[year] - df.gqpop.sum()
    s = df.hhpop
    s = scale_by_target(s, target, .15)
    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


# temporary function to balance hh while some parcels have
# unassigned MAZ
def add_households(df, tothh):
    s = scale_by_target(df.tothh, tothh)  # , .15

    df["tothh"] = round_series_match_target(s, tothh, 0)
    df["tothh"] = df.tothh.fillna(0)
    return df


# add employemnt to the dataframe - this uses a regression with
# estimated coefficients done by @mkreilly


def add_employment(df, year, tm1_tm2_regional_controls):

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

    rc = tm1_tm2_regional_controls
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
def add_age_categories(df, year, tm1_tm2_regional_controls):
    zfi = zone_forecast_inputs()
    rc = tm1_tm2_regional_controls

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


def adjust_hhsize(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year,
                             ['shrs1', 'shrs2', 'shrs3',
                              'shrs4']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_size_1', 'hh_size_2',
                               'hh_size_3', 'hh_size_4_plus']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals,
                                    target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhsizedf = pd.DataFrame(mat)

    hhsizedf.columns = ['hh_size_1', 'hh_size_2',
                        'hh_size_3', 'hh_size_4_plus']
    hhsizedf.index = df.index
    for ind, row in hhsizedf.iterrows():
        target = df.hh.loc[ind]
        row = row.round()
        hhsizedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhsizedf.columns:
        df[col] = hhsizedf[col]

    return df


def adjust_hhwkrs(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year,
                             ['shrw0', 'shrw1', 'shrw2',
                              'shrw3']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_wrks_0', 'hh_wrks_1',
                               'hh_wrks_2', 'hh_wrks_3_plus']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals,
                                    target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhwkrdf = pd.DataFrame(mat)

    hhwkrdf.columns = ['hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']
    hhwkrdf.index = df.index
    for ind, row in hhwkrdf.iterrows():
        target = df.hh.loc[ind]
        row = row.round()
        hhwkrdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhwkrdf.columns:
        df[col] = hhwkrdf[col]

    return df


def adjust_page(df, year, tm1_tm2_regional_controls):
    rc = tm1_tm2_regional_controls
    rc['age0019'] = rc.age0004 + rc.age0519
    col_marginals = rc.loc[year,
                           ['age0019', 'age2044', 'age4564',
                            'age65p']]
    row_marginals = df['hhpop'].fillna(0).values
    seed_matrix = np.round(df[['pers_age_00_19', 'pers_age_20_34',
                               'pers_age_35_64',
                               'pers_age_65_plus']]).as_matrix()

    target = df['hhpop'].sum()
    col_marginals = scale_by_target(col_marginals,
                                    target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    pagedf = pd.DataFrame(mat)

    pagedf.columns = ['pers_age_00_19', 'pers_age_20_34',
                      'pers_age_35_64', 'pers_age_65_plus']
    pagedf.index = df.index
    for ind, row in pagedf.iterrows():
        target = np.round(df['hhpop'].loc[ind])
        row = row.round()
        pagedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in pagedf.columns:
        df[col] = pagedf[col]

    return df


def adjust_hhkids(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year,
                             ['shrn', 'shry']] * total_hh).values[0]
    row_marginals = df.hh.fillna(0).values
    seed_matrix = np.round(df[['hh_kids_no', 'hh_kids_yes']]).as_matrix()

    target = df.hh.sum()
    col_marginals = scale_by_target(col_marginals,
                                    target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhkidsdf = pd.DataFrame(mat)

    hhkidsdf.columns = ['hh_kids_no', 'hh_kids_yes']
    hhkidsdf.index = df.index
    for ind, row in hhkidsdf.iterrows():
        target = np.round(df.hh.loc[ind])
        row = row.round()
        hhkidsdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhkidsdf.columns:
        df[col] = hhkidsdf[col]

    return df
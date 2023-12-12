from __future__ import print_function

import os
import pathlib
import orca
import pandas as pd
import numpy as np
from baus.utils import round_series_match_target, scale_by_target, simple_ipf
from baus import datasources

# ensure output directories are created before attempting to write to them

folder_stub = 'travel_model_summaries'
target_path = os.path.join(orca.get_injectable("outputs_dir"), folder_stub)
os.makedirs(target_path, exist_ok=True)

######################################################
### functions for producing travel model variables ###
######################################################

def scaled_ciacre(mtcc, us_outc, zfi):
    abgc = zfi.ciacre10_abag
    sim_difference = [us_outc - mtcc][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgc + sim_difference
    return combined_acres


def scaled_resacre(mtcr, us_outr, zfi):
    abgr = zfi.resacre10_abag
    sim_difference = [us_outr - mtcr][0]
    sim_difference[sim_difference < 0] = 0
    combined_acres = abgr + sim_difference
    return combined_acres


def add_population(df, year, rc, zfi):
    target = rc.totpop.loc[year] - df.gqpop.sum()
    s = df.tothh * zfi.meanhhsize
    s = scale_by_target(s, target)  # , .15
    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


def add_population_tm2(df, year, rc):
    target = rc.totpop.loc[year] - df.gqpop.sum()
    s = df.hhpop
    s = scale_by_target(s, target, .15)
    df["hhpop"] = round_series_match_target(s, target, 0)
    df["hhpop"] = df.hhpop.fillna(0)
    return df


# temporary function to balance hh while some parcels have unassigned MAZ
def add_households(df, tothh):
    s = scale_by_target(df.tothh, tothh)  # , .15

    df["tothh"] = round_series_match_target(s, tothh, 0)
    df["tothh"] = df.tothh.fillna(0)
    return df


def add_employment(df, year, rc, zfi):
    hhs_by_inc = df[["hhincq1", "hhincq2", "hhincq3", "hhincq4"]]
    hh_shares = hhs_by_inc.divide(hhs_by_inc.sum(axis=1), axis="index")

    # this uses a regression with estimated coefficients done by @mkreilly
    empshare = 0.46381 * hh_shares.hhincq1 + 0.49361 * hh_shares.hhincq2 +\
        0.56938 * hh_shares.hhincq3 + 0.29818 * hh_shares.hhincq4 + zfi.zonal_emp_sh_resid10
    # assert that only up to 70% of people can be employed in a given zone
    # this also makes sure that the employed residents is less then the total population (after scaling)
    empshare = empshare.fillna(0).clip(.3, .7)

    empres = empshare * df.totpop
    target = rc.empres.loc[year]
    empres = scale_by_target(empres, target)
    df["empres"] = round_series_match_target(empres, target, 0)
    # this should make the assertion below pass, but now only occurs very infrequently
    df["empres"] = df[["empres", "totpop"]].min(axis=1)
    # make sure employed residents is less than total residents
    assert (df.empres <= df.totpop).all()

    return df


def add_age_categories(df, year, rc, zfi):
    seed_matrix = zfi[["sh_age0004", "sh_age0519", "sh_age2044",
                       "sh_age4564", "sh_age65p"]].mul(df.totpop, axis='index').as_matrix()

    row_marginals = df.totpop.values
    agecols = ["age0004", "age0519", "age2044", "age4564", "age65p"]
    col_marginals = rc[agecols].loc[year].values

    target = df.totpop.sum()
    col_marginals = scale_by_target(pd.Series(col_marginals), target).round().astype('int')

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
    col_marginals = (rdf.loc[rdf.year == year, ['shrs1', 'shrs2', 'shrs3', 'shrs4']] * total_hh).values[0]
    row_marginals = df.tothh.fillna(0).values
    seed_matrix = np.round(df[['hh_size_1', 'hh_size_2', 'hh_size_3', 'hh_size_4_plus']]).as_matrix()

    target = df.tothh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhsizedf = pd.DataFrame(mat)

    hhsizedf.columns = ['hh_size_1', 'hh_size_2', 'hh_size_3', 'hh_size_4_plus']
    hhsizedf.index = df.index
    for ind, row in hhsizedf.iterrows():
        target = df.tothh.loc[ind]
        row = row.round()
        hhsizedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhsizedf.columns:
        df[col] = hhsizedf[col]

    return df
                         

def adjust_hhwkrs(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year, ['shrw0', 'shrw1', 'shrw2', 'shrw3']] * total_hh).values[0]
    row_marginals = df.tothh.fillna(0).values
    seed_matrix = np.round(df[['hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']]).as_matrix()

    target = df.tothh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhwkrdf = pd.DataFrame(mat)

    hhwkrdf.columns = ['hh_wrks_0', 'hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']
    hhwkrdf.index = df.index
    for ind, row in hhwkrdf.iterrows():
        target = df.tothh.loc[ind]
        row = row.round()
        hhwkrdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhwkrdf.columns:
        df[col] = hhwkrdf[col]

    return df


def adjust_page(df, year, rc):
    rc['age0019'] = rc.age0004 + rc.age0519
    col_marginals = rc.loc[year,
                           ['age0019', 'age2044', 'age4564', 'age65p']]
    row_marginals = df['hhpop'].fillna(0).values
    seed_matrix = np.round(df[['pers_age_00_19', 'pers_age_20_34', 'pers_age_35_64', 'pers_age_65_plus']]).as_matrix()

    target = df['hhpop'].sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    pagedf = pd.DataFrame(mat)

    pagedf.columns = ['pers_age_00_19', 'pers_age_20_34', 'pers_age_35_64', 'pers_age_65_plus']
    pagedf.index = df.index
    for ind, row in pagedf.iterrows():
        target = np.round(df['hhpop'].loc[ind])
        row = row.round()
        pagedf.loc[ind] = round_series_match_target(row, target, 0)

    for col in pagedf.columns:
        df[col] = pagedf[col]

    return df


def adjust_hhkids(df, year, rdf, total_hh):
    col_marginals = (rdf.loc[rdf.year == year, ['shrn', 'shry']] * total_hh).values[0]
    row_marginals = df.tothh.fillna(0).values
    seed_matrix = np.round(df[['hh_kids_no', 'hh_kids_yes']]).as_matrix()

    target = df.tothh.sum()
    col_marginals = scale_by_target(col_marginals, target).round().astype('int')

    seed_matrix[seed_matrix == 0] = .1
    seed_matrix[row_marginals == 0, :] = 0

    mat = simple_ipf(seed_matrix, col_marginals, row_marginals)
    hhkidsdf = pd.DataFrame(mat)

    hhkidsdf.columns = ['hh_kids_no', 'hh_kids_yes']
    hhkidsdf.index = df.index
    for ind, row in hhkidsdf.iterrows():
        target = np.round(df.tothh.loc[ind])
        row = row.round()
        hhkidsdf.loc[ind] = round_series_match_target(row, target, 0)

    for col in hhkidsdf.columns:
        df[col] = hhkidsdf[col]

    return df

######################################################
############# travel model summary steps #############
######################################################

@orca.step()
def taz1_summary(parcels, households, jobs, buildings, zones, maz, year, base_year_summary_taz, taz_geography, 
                 tm1_taz1_forecast_inputs, tm1_tm2_maz_forecast_inputs, tm1_tm2_regional_demographic_forecast, 
                 tm1_tm2_regional_controls, initial_summary_year, interim_summary_year, final_year, run_name):
    
    # Commenting this out so we get taz1 summaries for every year.
    # It's about 90 seconds a pop so not great, not terrible
    # if year not in [initial_summary_year, interim_summary_year, final_year]:
    #     return

    # (1) add relevant geographies to TAZ summaries
    taz_df = pd.DataFrame(index=zones.index)
    taz_df["sd"] = taz_geography.superdistrict
    taz_df["zone"] = zones.index
    taz_df["county"] = taz_geography.county_name
    
    # create a zone_id  for parcels to make sure it can get used in the merge bug mentioned below
    parcels = parcels.to_frame()
    parcels["zone_id_x"] = parcels.zone_id
    orca.add_table('parcels', parcels)
    parcels = orca.get_table("parcels")

    # (2) summarize households by TAZ1
    households_df = orca.merge_tables('households', [parcels, buildings, households],
                                      columns=['zone_id', 'zone_id_x', 'base_income_quartile',
                                               'income', 'persons', 'maz_id'])
    # merge_tables returns multiple zone_id_'s, but not the one we need
    households_df["zone_id"] = households_df.zone_id_x

    def gethhcounts(filter):
        return households_df.query(filter).groupby('zone_id').size()
    taz_df["hhincq1"] = gethhcounts("base_income_quartile == 1")
    taz_df["hhincq2"] = gethhcounts("base_income_quartile == 2")
    taz_df["hhincq3"] = gethhcounts("base_income_quartile == 3")
    taz_df["hhincq4"] = gethhcounts("base_income_quartile == 4")
    taz_df["hhpop"] = households_df.groupby('zone_id').persons.sum()
    taz_df["tothh"] = households_df.groupby('zone_id').size()

    # (3) summarize jobs by TAZ1
    jobs_df = orca.merge_tables(
        'jobs',
        [parcels, buildings, jobs],
        columns=['zone_id', 'zone_id_x', 'empsix'])
    # merge_tables returns multiple zone_id_'s, but not the one we need
    jobs_df["zone_id"] = jobs_df.zone_id_x

    def getsectorcounts(sector):
        return jobs_df.query("empsix == '%s'" % sector).groupby('zone_id').size()
    taz_df["agrempn"] = getsectorcounts("AGREMPN")
    taz_df["fpsempn"] = getsectorcounts("FPSEMPN")
    taz_df["herempn"] = getsectorcounts("HEREMPN")
    taz_df["retempn"] = getsectorcounts("RETEMPN")
    taz_df["mwtempn"] = getsectorcounts("MWTEMPN")
    taz_df["othempn"] = getsectorcounts("OTHEMPN")
    taz_df["totemp"] = jobs_df.groupby('zone_id').size()

    # (4) add residenital units by TAZ1
    buildings_df = buildings.to_frame(['zone_id', 'building_type', 'residential_units'])
    taz_df["res_units"] = buildings_df.groupby('zone_id').residential_units.sum()
    taz_df["mfdu"] = buildings_df.query("building_type == 'HM' or building_type == 'MR'").groupby('zone_id').residential_units.sum()
    taz_df["sfdu"] = buildings_df.query("building_type == 'HS' or building_type == 'HT'").groupby('zone_id').residential_units.sum()

    # (5) add variables from the taz forecast inputs 
    zfi = tm1_taz1_forecast_inputs.to_frame()
    zfi.index = zfi.zone_id
    taz_df["shpop62p"] = zfi.sh_62plus
    taz_df["gqpop"] = zfi["gqpop" + str(year)[-2:]].fillna(0)
    taz_df["totacre"] = zfi.totacre_abag
    taz_df["totpop"] = (taz_df.hhpop + taz_df.gqpop).fillna(0)

    # (6) add household variables from the taz forecast inputs using shares of households
    taz_df['hh_size_1'] = taz_df['tothh'] * zfi.shrs1_2010
    taz_df['hh_size_2'] = taz_df['tothh'] * zfi.shrs2_2010
    taz_df['hh_size_3'] = taz_df['tothh'] * zfi.shrs3_2010
    taz_df['hh_size_4_plus'] = taz_df['tothh'] * zfi.shrs4_2010
    taz_df['hh_wrks_0'] = taz_df['tothh'] * zfi.shrw0_2010
    taz_df['hh_wrks_1'] = taz_df['tothh'] * zfi.shrw1_2010
    taz_df['hh_wrks_2'] = taz_df['tothh'] * zfi.shrw2_2010
    taz_df['hh_wrks_3_plus'] = taz_df['tothh'] * zfi.shrw3_2010
    taz_df['hh_kids_no'] = taz_df['tothh'] * zfi.shrn_2010
    taz_df['hh_kids_yes'] = taz_df['tothh'] * zfi.shry_2010
    
    # (6b) adjust those same household variables with the regional demographic forecast
    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    taz_df = adjust_hhsize(taz_df, year, rdf, taz_df.tothh.sum())
    taz_df = adjust_hhwkrs(taz_df, year, rdf, taz_df.tothh.sum())
    taz_df = adjust_hhkids(taz_df, year, rdf, taz_df.tothh.sum())

    # (7) add population, employment, and age using the regional controls
    rc = tm1_tm2_regional_controls.to_frame()
    taz_df = add_population(taz_df, year, rc, zfi)
    taz_df.totpop = taz_df.hhpop + taz_df.gqpop
    taz_df = add_employment(taz_df, year, rc, zfi)
    taz_df = add_age_categories(taz_df, year, rc, zfi)

    # (8) add density and areatype variables
    taz_df["density_pop"] = taz_df.totpop / taz_df.totacre
    taz_df["density_pop"] = taz_df["density_pop"].fillna(0)
    taz_df["density_emp"] = (2.5 * taz_df.totemp) / taz_df.totacre
    taz_df["density_emp"] = taz_df["density_emp"].fillna(0)
    taz_df["density"] = taz_df["density_pop"] + taz_df["density_emp"]
    taz_df["areatype"] = pd.cut(taz_df.density, bins=[0, 6, 30, 55, 100, 300, np.inf], labels=[5, 4, 3, 2, 1, 0])

    # (9) use maz forecast inputs to forecast group quarters
    mazi = tm1_tm2_maz_forecast_inputs.to_frame()
    mazi_yr = str(year)[2:]
    maz = maz.to_frame(['taz1454']) 
    # fix for maz_id issue
    households_df.maz_id = households_df.maz_id.fillna(213906) 
    maz["hhpop"] = households_df.groupby('maz_id').persons.sum()
    maz["tothh"] = households_df.groupby('maz_id').size()
    maz = add_households(maz, taz_df.tothh.sum())
    maz['gq_type_univ'] = mazi['gqpopu' + mazi_yr]
    maz['gq_type_mil'] = mazi['gqpopm' + mazi_yr]
    maz['gq_type_othnon'] = mazi['gqpopo' + mazi_yr]
    maz['gq_tot_pop'] = maz['gq_type_univ'] + maz['gq_type_mil'] + maz['gq_type_othnon']
    taz_df['gq_type_univ'] = maz.groupby('taz1454').gq_type_univ.sum().fillna(0)
    taz_df['gq_type_mil'] = maz.groupby('taz1454').gq_type_mil.sum().fillna(0)
    taz_df['gq_type_othnon'] = maz.groupby('taz1454').gq_type_othnon.sum().fillna(0)
    taz_df['gq_tot_pop'] = maz.groupby('taz1454').gq_tot_pop.sum().fillna(0)

    # (10) add acreage variables
    def count_acres_with_mask(mask):
        mask *= parcels.acres
        return mask.groupby(parcels.zone_id).sum()
    f = orca.get_injectable('parcel_first_building_type_is')
    taz_df["resacre_unweighted"] = count_acres_with_mask(f('residential') | f('mixedresidential'))
    taz_df["ciacre_unweighted"] = count_acres_with_mask(f('select_non_residential'))
    taz_df["ciacre"] = scaled_ciacre(base_year_summary_taz.CIACRE_UNWEIGHTED, taz_df.ciacre_unweighted, zfi)
    taz_df["resacre"] = scaled_resacre(base_year_summary_taz.RESACRE_UNWEIGHTED, taz_df.resacre_unweighted, zfi)

    taz_df.index.name = 'TAZ'
    # uppercase columns to match travel model template
    taz_df.columns = [x.upper() for x in taz_df.columns]
    tmsum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "travel_model_summaries"
    tmsum_output_dir.mkdir(parents=True, exist_ok=True)
    taz_df.fillna(0).to_csv(tmsum_output_dir / f"{run_name}_taz1_summary_{year}.csv")


@orca.step()
def taz1_growth_summary(year, initial_summary_year, final_year, run_name, buildings):

    if year != final_year: 
        return

    # use 2015 as the base year
    year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/%s_taz1_summary_%d.csv" % (run_name, initial_summary_year)))
    year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/%s_taz1_summary_%d.csv" % (run_name, final_year)))

    taz_summary = year1.merge(year2, on='TAZ', suffixes=("_"+str(initial_summary_year), "_"+str(final_year)))
    taz_summary = taz_summary.rename(columns={"SD_"+(str(initial_summary_year)): "SD", "COUNTY_"+(str(initial_summary_year)): "COUNTY",
                                     "ZONE_"+(str(initial_summary_year)): "ZONE"})
    taz_summary = taz_summary.drop(columns=["SD_"+(str(final_year)), "COUNTY_"+(str(final_year)), "ZONE_"+(str(final_year))])

    taz_summary["run_name"] = run_name

    columns = ['TOTEMP', 'TOTHH', 'TOTPOP', 'RES_UNITS']

    for col in columns:

        taz_summary[col+"_growth"] = (taz_summary[col+"_"+str(final_year)] - 
                                      taz_summary[col+"_"+str(initial_summary_year)])

        # percent change in geography's households/jobs/etc.
        taz_summary[col+'_pct_change'] = (round((taz_summary[col+"_"+str(final_year)] / 
                                                  taz_summary[col+"_"+str(initial_summary_year)] - 1) * 100, 2))

        # percent geography's growth of households/jobs/etc. of all regional growth in households/jobs/etc.
        taz_summary[col+'_pct_of_regional_growth'] = (round(((taz_summary[col+"_growth"]) / 
                                                              (taz_summary[col+"_"+str(final_year)].sum() - 
                                                              taz_summary[col+"_"+str(initial_summary_year)].sum())) * 100, 2))
        
        taz_summary[col+"_"+str(initial_summary_year)+"_share"] = (round(taz_summary[col+"_"+str(initial_summary_year)] / 
                                                                         taz_summary[col+"_"+str(initial_summary_year)].sum(), 2))
        taz_summary[col+"_"+str(final_year)+"_share"] = (round(taz_summary[col+"_"+str(final_year)] / 
                                                               taz_summary[col+"_"+str(final_year)].sum(), 2)   )         
        taz_summary[col+'_share_change'] = (taz_summary[col+"_"+str(final_year)+"_share"] -  
                                            taz_summary[col+"_"+str(initial_summary_year)+"_share"])
    
    taz_summary.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/{}_taz1_summary_growth.csv").format(run_name))


@orca.step()
def maz_marginals(parcels, households, buildings, maz, year,
                  tm1_tm2_maz_forecast_inputs, tm1_tm2_regional_demographic_forecast, initial_summary_year, 
                  interim_summary_year, final_year, run_name):
    
    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return
    
    # (1) intiialize maz dataframe
    maz_m = maz.to_frame(['TAZ', 'county_name'])

    # (2) add households by MAZ
    hh_df = orca.merge_tables('households', [parcels, buildings, households], columns=['maz_id'])
    # apply fix to maz_id
    hh_df.maz_id = hh_df.maz_id.fillna(213906)
    maz_m["tothh"] = hh_df.groupby('maz_id').size()
    maz_m['tothh'] = maz_m.tothh.fillna(0)
    maz_m = add_households(maz_m, maz_m.tothh.sum())

    # (3a) use maz forecast inputs to forecast group quarters
    mazi = tm1_tm2_maz_forecast_inputs.to_frame()
    mazi_yr = str(year)[2:]
    maz_m['gq_type_univ'] = mazi['gqpopu' + mazi_yr]
    maz_m['gq_typeil'] = mazi['gqpopm' + mazi_yr]
    maz_m['gq_type_othnon'] = mazi['gqpopo' + mazi_yr]
    maz_m['gq_tot_pop'] = maz_m['gq_type_univ'] + maz_m['gq_typeil'] + maz_m['gq_type_othnon']

    # (4a) also add households size variables using maz forecast inputs
    maz_m['hh_size_1'] = maz_m.tothh.fillna(0) * mazi.shrs1_2010
    maz_m['hh_size_2'] = maz_m.tothh.fillna(0) * mazi.shrs2_2010
    maz_m['hh_size_3'] = maz_m.tothh.fillna(0) * mazi.shrs3_2010
    maz_m['hh_size_4_plus'] = maz_m.tothh.fillna(0) * mazi.shs4_2010
    # (4b) adjust household size variables with the regional demographic forecast
    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    maz_m = adjust_hhsize(maz_m, year, rdf, maz_m.tothh.sum())

    maz_m.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                        "travel_model_summaries/{}_maz_marginals_{}.csv".format(run_name, year)))
    orca.add_table("maz_marginals_df", maz_m)


@orca.step()
def maz_summary(parcels, jobs, households, buildings, maz, year, tm2_emp27_employment_shares, 
                tm1_tm2_regional_controls, initial_summary_year, interim_summary_year, final_year, run_name):
    
    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    # (1) intiialize maz dataframe
    maz_df = maz.to_frame(['TAZ', 'county_name'])

    # (2) get tothh from maz marginals dataframe
    maz_marginals_df = orca.get_table("maz_marginals_df").to_frame()
    maz_df['tothh'] = maz_marginals_df['tothh']

    # (3) summarize household data by MAZ
    hh_df = orca.merge_tables('households', [parcels, buildings, households],
                                            columns=['persons', 'base_income_quartile', 'maz_id'])
    def gethhcounts(filter):
        return hh_df.query(filter).groupby('maz_id').size()
    maz_df["hhincq1"] = gethhcounts("base_income_quartile == 1")
    maz_df["hhincq2"] = gethhcounts("base_income_quartile == 2")
    maz_df["hhincq3"] = gethhcounts("base_income_quartile == 3")
    maz_df["hhincq4"] = gethhcounts("base_income_quartile == 4")

    # (4) summarize jobs by MAZ
    jobs_df = orca.merge_tables('jobs',
                                [parcels, buildings, jobs],
                                columns=['maz_id', 'empsix'])

    # use the EMPSIX to EMP27 shares to disaggregate jobs
    tm2_emp27_employment_shares = tm2_emp27_employment_shares.to_frame()
    def getsectorcounts(empsix, empsh):
        emp = jobs_df.query("empsix == '%s'" % empsix).groupby('maz_id').size()
        return emp * tm2_emp27_employment_shares.loc[tm2_emp27_employment_shares.empsh == empsh, str(year)].values[0]
    maz_df["ag"] = getsectorcounts("AGREMPN", "ag")
    maz_df["natres"] = getsectorcounts("AGREMPN", "natres")
    maz_df["fire"] = getsectorcounts("FPSEMPN", "fire")
    maz_df["serv_bus"] = getsectorcounts("FPSEMPN", "serv_bus")
    maz_df["prof"] = getsectorcounts("FPSEMPN", "prof")
    maz_df["lease"] = getsectorcounts("FPSEMPN", "lease")
    maz_df["art_rec"] = getsectorcounts("HEREMPN", "art_rec")
    maz_df["serv_soc"] = getsectorcounts("HEREMPN", "serv_soc")
    maz_df["serv_per"] = getsectorcounts("HEREMPN", "serv_per")
    maz_df["ed_high"] = getsectorcounts("HEREMPN", "ed_high")
    maz_df["ed_k12"] = getsectorcounts("HEREMPN", "ed_k12")
    maz_df["ed_oth"] = getsectorcounts("HEREMPN", "ed_oth")
    maz_df["health"] = getsectorcounts("HEREMPN", "health")
    maz_df["man_tech"] = getsectorcounts("MWTEMPN", "man_tech")
    maz_df["man_lgt"] = getsectorcounts("MWTEMPN", "man_lgt")
    maz_df["logis"] = getsectorcounts("MWTEMPN", "logis")
    maz_df["man_bio"] = getsectorcounts("MWTEMPN", "man_bio")
    maz_df["transp"] = getsectorcounts("MWTEMPN", "transp")
    maz_df["man_hvy"] = getsectorcounts("MWTEMPN", "man_hvy")
    maz_df["util"] = getsectorcounts("MWTEMPN", "util")
    maz_df["info"] = getsectorcounts("OTHEMPN", "info")
    maz_df["gov"] = getsectorcounts("OTHEMPN", "gov")
    maz_df["constr"] = getsectorcounts("OTHEMPN", "constr")
    maz_df["hotel"] = getsectorcounts("RETEMPN", "hotel")
    maz_df["ret_loc"] = getsectorcounts("RETEMPN", "ret_loc")
    maz_df["ret_reg"] = getsectorcounts("RETEMPN", "ret_reg")
    maz_df["eat"] = getsectorcounts("RETEMPN", "eat")
    maz_df["emp_total"] = jobs_df.groupby('maz_id').size()
    maz_df = maz_df.fillna(0)
    emp_cols = ['ag', 'natres', 'logis', 'man_bio', 'man_hvy', 'man_lgt',
                'man_tech', 'transp', 'util', 'eat', 'hotel', 'ret_loc',
                'ret_reg', 'fire', 'lease', 'prof', 'serv_bus', 'art_rec',
                'ed_high', 'ed_k12', 'ed_oth', 'health', 'serv_per',
                'serv_soc', 'constr', 'info', 'gov']
    for i, r in maz_df.iterrows():
        maz_df.loc[i, emp_cols] = round_series_match_target(r[emp_cols], r.emp_total, 0)

    # (5) add population 
    maz_df["hhpop"] = hh_df.groupby('maz_id').persons.sum()
    # use marginals dataframe to get gqpop
    maz_df['gqpop'] = maz_marginals_df['gq_tot_pop']
    maz_df = add_population_tm2(maz_df, year, tm1_tm2_regional_controls.to_frame())
    maz_df['pop'] = maz_df.gqpop + maz_df.hhpop

    # (6) add density variables
    pcl_df = parcels.to_frame(['maz_id', 'acres'])
    bldg_df = orca.merge_tables('buildings', [buildings, parcels], columns=['maz_id', 'residential_units'])
    maz_df['ACRES'] = pcl_df.groupby('maz_id').acres.sum()
    maz_df['residential_units'] = bldg_df.groupby('maz_id').residential_units.sum()
    maz_df['DUDen'] = maz_df.residential_units / maz_df.ACRES
    maz_df['EmpDen'] = maz_df.emp_total / maz_df.ACRES
    maz_df['RetEmp'] = maz_df.hotel + maz_df.ret_loc + maz_df.ret_reg + maz_df.eat
    maz_df['RetEmpDen'] = maz_df.RetEmp / maz_df.ACRES
    maz_df['PopDen'] = maz_df["pop"] / maz_df.ACRES

    maz_df.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                         "travel_model_summaries/{}_maz_summary_{}.csv".format(run_name, year)))
    orca.add_table("maz_summary_df", maz_df)


@orca.step()
def maz_growth_summary(year, initial_summary_year, final_year, run_name):

    if year != final_year: 
        return

    # use 2015 as the base year
    year1 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/%s_maz_summary_%d.csv" % (run_name, initial_summary_year)))
    year2 = pd.read_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/%s_maz_summary_%d.csv" % (run_name, final_year)))

    maz_summary = year1.merge(year2, on='MAZ', suffixes=("_"+str(initial_summary_year), "_"+str(final_year)))
    maz_summary = maz_summary.rename(columns={"TAZ_"+(str(initial_summary_year)): "TAZ", "county_name_"+(str(initial_summary_year)): "county_name"})
    maz_summary = maz_summary.drop(columns=["TAZ_"+(str(final_year)), "county_name_"+(str(final_year))])

    maz_summary["run_name"] = run_name

    columns = ['tothh', 'pop', 'emp_total']

    for col in columns:

        maz_summary[col+'_growth'] = maz_summary[col+"_"+str(final_year)] - maz_summary[col+"_"+str(initial_summary_year)]

        maz_summary[col+"_"+str(initial_summary_year)+"_share"] = (round(maz_summary[col+"_"+str(initial_summary_year)] / 
                                                                        maz_summary[col+"_"+str(initial_summary_year)].sum(), 2))
        maz_summary[col+"_"+str(final_year)+"_share"] = (round(maz_summary[col+"_"+str(final_year)] / 
                                                              maz_summary[col+"_"+str(final_year)].sum(), 2))            
        maz_summary[col+'_share_change'] = (maz_summary[col+"_"+str(final_year)+"_share"] -  
                                            maz_summary[col+"_"+str(initial_summary_year)+"_share"])
    
    maz_summary.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/{}_maz_summary_growth.csv").format(run_name))


@orca.step()
def taz2_marginals(tm2_taz2_forecast_inputs, tm1_tm2_regional_demographic_forecast, tm1_tm2_regional_controls, 
                   year, initial_summary_year, interim_summary_year, final_year, run_name):
    
    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    # (1) bring in taz2 dataframe
    taz2 = pd.DataFrame(index=tm2_taz2_forecast_inputs.index)
    taz2.index.name = 'TAZ2'

    # (2) summarize maz vars for household income and population
    maz_summary_df = orca.get_table("maz_summary_df").to_frame()
    taz2['county_name'] = maz_summary_df.groupby('TAZ').county_name.first()
    taz2['tothh'] = maz_summary_df.groupby('TAZ').tothh.sum()
    taz2['hh_inc_30'] = maz_summary_df.groupby('TAZ').hhincq1.sum().fillna(0)
    taz2['hh_inc_30_60'] = maz_summary_df.groupby('TAZ').hhincq2.sum().fillna(0)
    taz2['hh_inc_60_100'] = maz_summary_df.groupby('TAZ').hhincq3.sum().fillna(0)
    taz2['hh_inc_100_plus'] = maz_summary_df.groupby('TAZ').hhincq4.sum().fillna(0)
    taz2['hhpop'] = maz_summary_df.groupby('TAZ').hhpop.sum()
    maz_marginals_df = orca.get_table("maz_marginals_df").to_frame()
    taz2['pop_hhsize1'] = maz_marginals_df.groupby('TAZ').hh_size_1.sum()
    taz2['pop_hhsize2'] = maz_marginals_df.groupby('TAZ').hh_size_2.sum() * 2
    taz2['pop_hhsize3'] = maz_marginals_df.groupby('TAZ').hh_size_3.sum() * 3
    taz2['pop_hhsize4'] = (maz_marginals_df.groupby('TAZ').hh_size_4_plus.sum() * 4.781329).round(0)
    taz2['pop'] = taz2.pop_hhsize1 + taz2.pop_hhsize2 + taz2.pop_hhsize3 + taz2.pop_hhsize4

    # (3a) add person age, household workers, and presence of children using taz2 forecast inputs
    t2fi = tm2_taz2_forecast_inputs.to_frame()
    taz2['pers_age_00_19'] = taz2['hhpop'] * t2fi.shra1_2010
    taz2['pers_age_20_34'] = taz2['hhpop'] * t2fi.shra2_2010
    taz2['pers_age_35_64'] = taz2['hhpop'] * t2fi.shra3_2010
    taz2['pers_age_65_plus'] = taz2['hhpop'] * t2fi.shra4_2010
    taz2['hh_wrks_0'] = taz2['tothh'] * t2fi.shrw0_2010
    taz2['hh_wrks_1'] = taz2['tothh'] * t2fi.shrw1_2010
    taz2['hh_wrks_2'] = taz2['tothh'] * t2fi.shrw2_2010
    taz2['hh_wrks_3_plus'] = taz2['tothh'] * t2fi.shrw3_2010
    taz2['hh_kids_no'] = taz2['tothh'] * t2fi.shrn_2010
    taz2['hh_kids_yes'] = taz2['tothh'] * t2fi.shry_2010
    # (3b) adjust person age, household workers, and presence of children with the regional demographic forecast
    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    taz2 = adjust_hhwkrs(taz2, year, rdf, taz2.tothh.sum())
    taz2 = adjust_page(taz2, year, tm1_tm2_regional_controls.to_frame())
    taz2 = adjust_hhkids(taz2, year, rdf, taz2.tothh.sum())

    taz2 = taz2.fillna(0)
    taz2.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/{}_taz2_marginals_{}.csv".format(run_name, year)))
    # save info to be used to produce county marginals
    orca.add_table("taz2_summary_df", taz2)


@orca.step()
def county_marginals(tm2_occupation_shares, year, initial_summary_year, 
                     interim_summary_year, final_year, run_name):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    maz = orca.get_table("maz_summary_df").to_frame()
    taz2 = orca.get_table("taz2_summary_df").to_frame()
    
    # (1) initialize county dataframe
    county = pd.DataFrame(index=maz.county_name.unique())

    # (2) add population
    county['gqpop'] = maz.groupby('county_name').gqpop.sum()
    county['pop'] = maz.groupby('county_name').pop.sum()

    # (3) add occupations
    county[['hh_wrks_1', 'hh_wrks_2', 'hh_wrks_3_plus']] = taz2.groupby('county_name').agg({'hh_wrks_1': 'sum',
                                                                                            'hh_wrks_2': 'sum',
                                                                                            'hh_wrks_3_plus': 'sum'})
    county['workers'] = (county.hh_wrks_1 + county.hh_wrks_2 * 2 + county.hh_wrks_3_plus * 3.474036).round(0)
    cef = tm2_occupation_shares.to_frame()
    cef = cef.loc[cef.year == year].set_index('county_name')
    county['pers_occ_management'] = county.workers * cef.shr_occ_management
    county['pers_occ_management'] = round_series_match_target(county['pers_occ_management'], np.round(county['pers_occ_management'].sum()), 0)
    county['pers_occ_professional'] = county.workers * cef.shr_occ_professional
    county['pers_occ_professional'] = round_series_match_target(county['pers_occ_professional'], np.round(county['pers_occ_professional'].sum()), 0)
    county['pers_occ_services'] = county.workers * cef.shr_occ_services
    county['pers_occ_services'] = round_series_match_target(county['pers_occ_services'], np.round(county['pers_occ_services'].sum()), 0)
    county['pers_occ_retail'] = county.workers * cef.shr_occ_retail
    county['pers_occ_retail'] = round_series_match_target(county['pers_occ_retail'], np.round(county['pers_occ_retail'].sum()), 0)
    county['pers_occ_manual'] = county.workers * cef.shr_occ_manual
    county['pers_occ_manual'] = round_series_match_target(county['pers_occ_manual'], np.round(county['pers_occ_manual'].sum()), 0)
    county['pers_occ_military'] = county.workers * cef.shr_occ_military
    county['pers_occ_military'] = round_series_match_target(county['pers_occ_military'], np.round(county['pers_occ_military'].sum()), 0)

    county.fillna(0).to_csv(os.path.join(orca.get_injectable("outputs_dir"), "travel_model_summaries/{}_county_marginals_{}.csv".format(run_name, year)))
    

@orca.step()
def region_marginals(year, initial_summary_year, interim_summary_year, final_year, run_name):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return
    
    # (1) get group quarters from MAZ summaries
    maz_marginals_df = orca.get_table("maz_marginals_df").to_frame()
    tot_gqpop = maz_marginals_df['gq_tot_pop'].sum()

    # (2) create regional dataframe with group quarters total
    region_m = pd.DataFrame(data={'REGION': [1], 'gq_num_hh_region': [tot_gqpop]})
    
    region_m.to_csv(os.path.join(orca.get_injectable("outputs_dir"), 
                                 "travel_model_summaries/{}_region_marginals_{}.csv").format(run_name, year), index=False)

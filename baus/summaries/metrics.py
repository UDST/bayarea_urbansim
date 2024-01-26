from __future__ import print_function

import pathlib
import orca
import pandas as pd
from baus import datasources


@orca.step()
def growth_geography_metrics(parcels, parcels_geography, buildings, households, jobs, year, 
                             initial_summary_year, final_year, run_name): 

    if year != initial_summary_year and year != final_year:
        return

    households_df = orca.merge_tables('households', [parcels, buildings, households, parcels_geography],
        columns=['income', 'base_income_quartile', 'gg_id', 'pda_id', 'tra_id', 'sesit_id'])
    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs, parcels_geography],
        columns=['empsix', 'gg_id', 'pda_id', 'tra_id', 'sesit_id'])
        
    # intialize growth geographies summary table
    growth_geog_summary = pd.DataFrame(index=[0])
    # households in growth geographies
    growth_geog_summary['tothh'] = households_df.size
    growth_geog_summary['gg_hh'] = households_df[households_df.gg_id > ''].size
    growth_geog_summary['pda_hh'] = households_df[households_df.pda_id > ''].size
    growth_geog_summary['gg_non_pda_hh'] = households_df[(households_df.gg_id > '') & (households_df.pda_id == '')].size
    growth_geog_summary['hra_hh'] = households_df[(households_df.sesit_id == 'hra') | (households_df.sesit_id == 'hradis')].size
    growth_geog_summary['tra_hh'] = households_df[households_df.tra_id > ''].size
    growth_geog_summary['hra_tra_hh'] = households_df[(households_df.sesit_id.isin(['hra', 'hradis'])) & (households_df.tra_id > '')].size
    # jobs in growth geographies
    growth_geog_summary['totemp'] = jobs_df.size
    growth_geog_summary['gg_jobs'] = jobs_df[jobs_df.gg_id > ''].size
    growth_geog_summary['pda_jobs'] = jobs_df[jobs_df.pda_id > ''].size
    growth_geog_summary['gg_non_pda_jobs'] = jobs_df[(jobs_df.gg_id > '') & (jobs_df.pda_id == '')].size
    growth_geog_summary['hra_jobs'] = jobs_df[(jobs_df.sesit_id == 'hra') | (jobs_df.sesit_id == 'hradis')].size
    growth_geog_summary['tra_jobs'] = jobs_df[(jobs_df.tra_id > '')].size
    growth_geog_summary['hra_tra_jobs'] = jobs_df[(jobs_df.sesit_id.isin(['hra', 'hradis'])) & (jobs_df.tra_id > '')].size

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    growth_geog_summary.to_csv(metrics_output_dir / f"{run_name}_growth_geography_summary_{year}.csv")
    
    # now calculate growth metrics
    if year != final_year:
        return
    
    year1 = pd.read_csv(metrics_output_dir / f"{run_name}_growth_geography_summary_{initial_summary_year}.csv")
    year2 = pd.read_csv(metrics_output_dir / f"{run_name}_growth_geography_summary_{final_year}.csv")

    growth_geog_growth = pd.DataFrame(index=[0])

    growth_geog_growth["tothh_growth"] = year2['tothh'] - year1['tothh']
    growth_geog_growth["totemp_growth"] = year2['totemp'] - year1['totemp']

    columns = ['gg_hh', 'pda_hh', 'gg_non_pda_hh', 'hra_hh', 'tra_hh', 'hra_tra_hh',
                'gg_jobs', 'pda_jobs', 'gg_non_pda_jobs', 'hra_jobs', 'tra_jobs', 'hra_tra_jobs']
    
    for col in columns:
        growth_geog_growth[col+"_"+str(initial_summary_year)] = year1[col]
        growth_geog_growth[col+"_"+str(final_year)] = year2[col]
        # growth in units 
        growth_geog_growth[col+'_growth'] = year2[col] - year1[col]
        
        # percent change in geography's households or jobs
        growth_geog_growth[col+'_pct_change'] = (year2[col] / year1[col] - 1).round(2)

        # percent geography's growth of all regional growth in household or jobs
        tot_growth = growth_geog_growth["tothh_growth"].sum() if "hh" in col else growth_geog_growth["totemp_growth"].sum()
        growth_geog_growth[col+'_pct_of_regional_growth'] = ((year2[col] - year1[col] / tot_growth) * 100).round(2)
    
    growth_geog_growth = growth_geog_growth.fillna(0)
    growth_geog_growth.to_csv(metrics_output_dir / f"{run_name}_growth_geography_growth_summary.csv")


@orca.step()
def deed_restricted_units_metrics(parcels, buildings, year, initial_summary_year, final_year, parcels_geography, run_name): 

    if year != initial_summary_year and year != final_year:
        return
    
    buildings_df = orca.merge_tables('buildings', [parcels, buildings, parcels_geography],
                                     columns=['residential_units', 'deed_restricted_units',
                                              'gg_id', 'pda_id', 'tra_id', 'sesit_id', 'coc_id'])
    
    # deed restricted units in a give year, by growth geography
    dr_units_summary = pd.DataFrame(index=['total'])
    dr_units_summary["res_units"] = buildings_df.residential_units.sum()
    dr_units_summary["dr_units"] = buildings_df.deed_restricted_units.sum()
    dr_units_summary["res_units_hra"] = buildings_df[(buildings_df.sesit_id == 'hra') | buildings_df.sesit_id == 'hradis'].residential_units.sum()
    dr_units_summary["dr_units_hra"] = buildings_df[(buildings_df.sesit_id == 'hra') | buildings_df.sesit_id == 'hradis'].deed_restricted_units.sum()
    dr_units_summary["res_units_coc"] = buildings_df[buildings_df.coc_id > ''].residential_units.sum()
    dr_units_summary["dr_units_coc"] = buildings_df[buildings_df.coc_id > ''].deed_restricted_units.sum()
    # percent deed-restricted units in a given year, by growth geography
    dr_units_summary["dr_units_pct"] = dr_units_summary["dr_units"] / dr_units_summary["res_units"]
    dr_units_summary["dr_units_pct_hra"] = dr_units_summary["dr_units_hra"] / dr_units_summary["res_units_hra"]
    dr_units_summary["dr_units_pct_coc"] = dr_units_summary["dr_units_coc"] / dr_units_summary["res_units_coc"]

    orca.add_table("dr_units_summary_"+str(year), dr_units_summary)

    if year != final_year:
        return
    
    dr_units_summary_y1 = orca.get_table(f"dr_units_summary_{initial_summary_year}").to_frame()
    dr_units_summary_y2 = orca.get_table(f"dr_units_summary_{final_year}").to_frame()
    
    dr_units_growth = pd.DataFrame(index=['total'])

    # percent of new units that are deed-restricted units, by growth geography
    dr_units_growth["pct_new_units_dr"] = ((dr_units_summary_y2["dr_units"] - dr_units_summary_y1["dr_units"]) /
                                            (dr_units_summary_y2["res_units"] - dr_units_summary_y1["res_units"])).round(2)
    dr_units_growth["pct_new_units_dr_hra"] = ((dr_units_summary_y2["dr_units_hra"] - dr_units_summary_y1["dr_units_hra"]) /
                                                (dr_units_summary_y2["res_units_hra"] - dr_units_summary_y1["res_units_hra"])).round(2)
    dr_units_growth["pct_new_units_dr_coc"] = ((dr_units_summary_y2["dr_units_coc"] - dr_units_summary_y1["dr_units_coc"]) /
                                                (dr_units_summary_y2["res_units_coc"] - dr_units_summary_y1["res_units_coc"])).round(2)

    dr_units_growth = dr_units_growth.fillna(0).transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    dr_units_growth.to_csv(metrics_output_dir / f"{run_name}_dr_units_metrics.csv")


@orca.step()
def household_income_metrics(year, initial_summary_year, final_year, parcels, buildings, households, 
                             parcels_geography, run_name):
    
    if year != initial_summary_year and year != final_year:
        return
    
    hh_df = orca.merge_tables('households', [parcels, buildings, households, parcels_geography], 
                              columns=['base_income_quartile', 'gg_id', 'pda_id', 'tra_id', 'sesit_id', 'coc_id'])
    
    ### low income households ###   
    hh_inc_summary = pd.DataFrame(index=['total'])

    #  total number of LIHH and low LIHH as a share of all HH
    hh_inc_summary['low_inc_hh'] = hh_df[hh_df.base_income_quartile == 1].size
    hh_inc_summary['low_inc_hh_tra_hra'] = hh_df[((hh_df.tra_id > '') & ((hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis'))) &
                                                (hh_df.base_income_quartile == 1)].size
    hh_inc_summary['low_inc_hh_tra'] = hh_df[(hh_df.tra_id > '') &
                                                (hh_df.base_income_quartile == 1)].size
    hh_inc_summary['low_inc_hh_hra'] = hh_df[((hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis')) &
                                                (hh_df.base_income_quartile == 1)].size
    hh_inc_summary['low_inc_hh_coc'] = hh_df[(hh_df.coc_id > '') &
                                                (hh_df.base_income_quartile == 1)].size
    
    # total number of LIHHs by growth geography and LIHH by growth geography as a share of all households
    hh_inc_summary['low_inc_hh_share'] = (hh_inc_summary['low_inc_hh'] / hh_df.size).round(2)
    hh_inc_summary['low_inc_hh_share_tra_hra'] = ((hh_inc_summary['low_inc_hh_tra_hra'] /
                                                    hh_df[(hh_df.tra_id > '') & ((hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis'))].size) 
                                                    if hh_df[(hh_df.tra_id > '') & ((hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis'))].size > 0 
                                                    else 0).round(2)
    hh_inc_summary['low_inc_hh_share_tra'] = ((hh_inc_summary['low_inc_hh_tra'] / hh_df[hh_df.tra_id > ''].size)
                                                if hh_df[hh_df.tra_id > ''].size > 0 else 0).round(2)
    hh_inc_summary['low_inc_hh_share_hra'] = ((hh_inc_summary['low_inc_hh_hra'] /
                                                hh_df[(hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis')].size)
                                                if hh_df[(hh_df.sesit_id == 'hra') | (hh_df.sesit_id == 'hradis')].size > 0 else 0).round(2)
    hh_inc_summary['low_inc_hh_share_coc'] = ((hh_inc_summary['low_inc_hh_coc'] / hh_df[hh_df.coc_id > ''].size)
                                             if hh_df[hh_df.coc_id > ''].size > 0 else 0).round(2)
    
    hh_inc_summary = hh_inc_summary.transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    hh_inc_summary.to_csv(metrics_output_dir / f"{run_name}_household_income_metrics_{year}.csv")

@orca.step()
def equity_metrics(year, initial_summary_year, final_year, parcels, buildings, households, parcel_tract_crosswalk, 
                   displacement_risk_tracts, coc_tracts, run_name):
    
    if year != initial_summary_year and year != final_year:
        return
    
    hh_df = orca.merge_tables('households', [parcels, buildings, households], columns=['base_income_quartile'])
    
    dis_tracts = displacement_risk_tracts.to_frame()
    dis_tracts = dis_tracts[dis_tracts.DispRisk == 1]
    dis_tracts["dis_tract_id"] = dis_tracts.Tract.copy()
    
    coc_tracts = coc_tracts.to_frame()
    coc_tracts = coc_tracts[coc_tracts.coc_flag_pba2050 == 1]
    coc_tracts["coc_tract_id"] = coc_tracts.tract_id.copy()

    hh_df = hh_df.merge(parcel_tract_crosswalk.to_frame(), on='parcel_id', how='left').merge(dis_tracts, 
                                                           left_on='tract_id', right_on='Tract', how='left').\
                                                           merge(coc_tracts, on='tract_id', how='left')

    # flag low income households in households table
    hh_df.loc[(hh_df.base_income_quartile == 1), "low_inc_hh"] = 1
    # add a household counter for all household
    hh_df["hh_count"] = 1
    # group households table by displacement tracts and coc tracts
    dis_tract_hhs = hh_df.groupby("dis_tract_id").sum()
    coc_tract_hhs = hh_df.groupby("coc_tract_id").sum()
    # add share of households that are low income for displacement tracts and coc tracts
    dis_tract_hhs['low_inc_share'] = dis_tract_hhs['low_inc_hh'] / dis_tract_hhs["hh_count"]
    coc_tract_hhs['low_inc_share'] = coc_tract_hhs['low_inc_hh'] / coc_tract_hhs["hh_count"]

    orca.add_table("dis_tract_hhs_"+str(year), dis_tract_hhs)
    orca.add_table("coc_tract_hhs_"+str(year), coc_tract_hhs)

    if year != final_year:
        return
    
    dis_tract_hhs_y1 = orca.get_table(f"dis_tract_hhs_{initial_summary_year}").to_frame()
    dis_tract_hhs_y2 = orca.get_table(f"dis_tract_hhs_{final_year}").to_frame()
    dis_tract_hhs_change = dis_tract_hhs_y1.merge(dis_tract_hhs_y2, left_index=True, right_index=True, suffixes=('_y1', '_y2'))
    dis_tract_hhs_change.name = 'dis_tracts'

    coc_tract_hhs_y1 = orca.get_table(f"coc_tract_hhs_{initial_summary_year}").to_frame()
    coc_tract_hhs_y2 = orca.get_table(f"coc_tract_hhs_{final_year}").to_frame()
    coc_tract_hhs_change = coc_tract_hhs_y1.merge(coc_tract_hhs_y2, left_index=True, right_index=True, suffixes=('_y1', '_y2'))
    coc_tract_hhs_change.name = 'coc_tracts'

    tract_hhs_change = pd.DataFrame(index=['total'])

    for df in [dis_tract_hhs_change, coc_tract_hhs_change]:

        df["low_inc_hh_change"] = (df['low_inc_hh_y2'] - df['low_inc_hh_y1'])
        df.loc[(df['low_inc_hh_change'] < 0), "low_inc_hh_loss"] = 1
        tract_hhs_change[df.name+"_pct_tracts_low_inc_loss"] = ((df[df.low_inc_hh_loss == 1].size / df.size) * 100).round(2)

        df["low_inc_hh_share_change"] = (df['low_inc_share_y1'] - df['low_inc_share_y2'])
        df.loc[(df['low_inc_hh_share_change'] > -.10), "ten_pct_low_inc_share_change"] = 1
        tract_hhs_change[df.name+"_pct_ten_pct_low_inc_share_change"] = \
                                            ((df[df.ten_pct_low_inc_share_change == 1].size / df.size) * 100).round(2)

    tract_hhs_change = tract_hhs_change.fillna(0).transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    tract_hhs_change.to_csv(metrics_output_dir / f"{run_name}_equity_metrics.csv")

    # TODO (short-term): how was a growth geography neighborhood determined?

@orca.step()
def jobs_housing_metrics(parcels, buildings, jobs, households, year, initial_summary_year, final_year, run_name):

    if year != initial_summary_year and year != final_year:
        return
    
    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs], columns=['empsix', 'county'])
    
    households_df = orca.merge_tables('households', [parcels, buildings, households], columns=['base_income_quartile', 'county'])

    jobs_housing_summary = pd.DataFrame(index=['jobs_housing_ratio'])
    # regional jobs-housing ratio
    jobs_housing_summary['Regional'] = (jobs_df.size / households_df.size).round(2)
    # county-level jobs-housing ratios
    parcels = parcels.to_frame()
    for county in parcels.county.unique():
        if pd.isna(county):
            continue
    jobs_housing_summary[str(county)] = (jobs_df[jobs_df.county == county].size / 
                                                            households_df[households_df.county == county].size).round(2)
        
    jobs_housing_summary = jobs_housing_summary.fillna(0).transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    jobs_housing_summary.to_csv(metrics_output_dir / f"{run_name}_jobs_housing_metrics_{year}.csv")


@orca.step()
def jobs_metrics(year, parcels, buildings, jobs, parcels_geography, initial_summary_year, final_year, run_name):
    
    if year != initial_summary_year and year != final_year:
        return

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs, parcels_geography], columns=['empsix', 'ppa_id'])

    jobs_summary = pd.DataFrame(index=['total'])
    jobs_summary['totemp'] = jobs_df.size
    jobs_summary['ppa_jobs'] = jobs_df[jobs_df.ppa_id > ''].size
    jobs_summary['mfg_jobs'] = jobs_df[jobs_df.empsix == 'MWTEMPN'].size
    jobs_summary['ppa_mfg_jobs'] = jobs_df[(jobs_df.ppa_id > '') & (jobs_df.empsix == 'MWTEMPN')].size
    orca.add_table("jobs_summary_"+str(year), jobs_summary)

    if year == final_year:
         # now calculate growth metrics
        jobs_summary_y1 = orca.get_table(f"jobs_summary_{initial_summary_year}").to_frame()
        jobs_summary_y2 = orca.get_table(f"jobs_summary_{final_year}").to_frame()
        
        # job growth
        jobs_growth_summary = pd.DataFrame(index=['total'])
        columns = ['totemp', 'ppa_jobs', 'mfg_jobs', 'ppa_mfg_jobs']
        for col in columns:
            jobs_growth_summary[col+'_pct_growth'] = ((jobs_summary_y2[col] / jobs_summary_y1[col] - 1) * 100).round(2)

        jobs_growth_summary = jobs_growth_summary.fillna(0).transpose()

        metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
        metrics_output_dir.mkdir(parents=True, exist_ok=True)
        jobs_growth_summary.to_csv(metrics_output_dir / f"{run_name}_jobs_metrics_{year}.csv")

    # TODO (short-term): where did low/mid/high wage jobs come from? confirm its from regional forecast output
        

@orca.step()
def slr_metrics(run_setup, parcels, buildings, parcels_geography, slr_parcel_inundation, households, year, final_year, run_name):

    # TODO (long-term): reconsider whether "2050 affected households" makes sense for the metric, 
    # since there should be no HH on these parcels

    if not run_setup['run_slr']:
        return

    if year != final_year:
        return
    
    hh_df = orca.merge_tables('households', [parcels, buildings, households])    
    hh_df = hh_df.merge(slr_parcel_inundation.to_frame(), on='parcel_id', how='left') 

    slr_metrics = pd.DataFrame(index=['total'])
    # households protected and unprotected from sea level rise
    slr_metrics["protected_households"] = hh_df[hh_df.inundation == 100].size
    slr_metrics["affected_households"] = hh_df[(hh_df.inundation == 12) | (hh_df.inundation == 24) | 
                                                (hh_df.inundation == 36) | (hh_df.inundation == 100)].size
    slr_metrics["pct_slr_hhs_protected"] = (slr_metrics["protected_households"] / slr_metrics["affected_households"]) * 100

    # Q1 households protected and unprotected from sea level rise
    slr_metrics["protected_q1_households"] = hh_df[(hh_df.base_income_quartile == 1) & (hh_df.inundation == 100)].size
    slr_metrics["affected_q1_households"] = hh_df[(hh_df.base_income_quartile == 1) & 
                                                   ((hh_df.inundation == 12) | (hh_df.inundation == 24) |
                                                    (hh_df.inundation == 36) | (hh_df.inundation == 100))].size
    slr_metrics["pct_slr_q1_hhs_protected"] = (slr_metrics["protected_q1_households"] / slr_metrics["affected_q1_households"]) * 100   
    
    # CoC households protected and unprotected from sea level rise
    slr_metrics["protected_coc_households"] = hh_df[(hh_df.coc_id > '') & (hh_df.inundation == 100)].size
    slr_metrics["affected_coc_households"] = hh_df[(hh_df.coc_id > '') & 
                                                    ((hh_df.inundation == 12) | (hh_df.inundation == 24) |
                                                     (hh_df.inundation == 36) | (hh_df.inundation == 100))].size
    slr_metrics["pct_slr_coc_hhs_protected"] = (slr_metrics["protected_coc_households"] / slr_metrics["affected_coc_households"]) * 100

    slr_metrics = slr_metrics.fillna(0).transpose()  # Assigned this back to slr_metrics; previously it wasn't saving the results anywhere - DSL
    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    slr_metrics.to_csv(metrics_output_dir / f"{run_name}_slr_metrics_{year}.csv")


@orca.step()
def earthquake_metrics(run_setup, parcels_geography, buildings_w_eq_codes, eq_retrofit_lookup, households, year, final_year, run_name):

    if not run_setup['run_eq']:
        return
    
    if year != final_year:
        return
    
    # TODO (long-term)- this seems to rely on a buildings -> earthquake code assignment from a certain run (which?)
    # it could easily use the buildings table from the model run instead (existing buildings + stochastic developer buildings)

    parcels_geography = parcels_geography.to_frame()
    buildings_w_eq_codes = buildings_w_eq_codes.to_frame()
    eq_retrofit_lookup = eq_retrofit_lookup.to_frame()

    # select the buldings that were retrofit based on their eq code
    retrofit_buildings = buildings_w_eq_codes.merge(eq_retrofit_lookup, left="earthquake_code", right_on="building_eq_code", how ="inner")
    retrofit_buildings = retrofit_buildings.merge(parcels_geography, on='parcel_id', how='left')
    # get the total cost of the retrofit per buildings based on building's number of units
    retrofit_buildings['cost_retrofit_total'] = retrofit_buildings['residential_units'] * retrofit_buildings['cost_retrofit']

    eq_metrics = pd.DataFrame(index=['total'])

    eq_metrics['num_units_retrofit'] = retrofit_buildings['residential_units'].sum()
    eq_metrics['total_cost_retrofit'] = retrofit_buildings['cost_retrofit_total'].sum()

    eq_metrics['num_coc_units_retrofit'] = retrofit_buildings[retrofit_buildings.coc_id > ''].residential_units.sum()
    eq_metrics['coc_cost_retrofit'] = retrofit_buildings[retrofit_buildings.coc_id > ''].cost_retrofit_total.sum()

    # now calculate protected/affected households
    # TODO (long-term): not sure whether all affetcted buildings are protected in the earthquake modeling or not
    # since there is randomness in the earthquake demolish model, which selects based on the fragility coming
    # from earthquake codes, but not every building with an earthquake code and fragility is a retrofit earthquake code
    # you'd want to use with the model's earthquake output files to compare households in retrofit buildings versus
    # households in all demolished buildings 
    households = households.merge(retrofit_buildings, on='building_id', how='left')
    
    eq_metrics["protected_households"] = households.residential_units.sum()
    eq_metrics["affected_households"] = households.residential_units.sum()
    eq_metrics["pct_eq_hhs_protected"] = (eq_metrics["protected_households"] / eq_metrics["affected_households"]) * 100

    # Q1 households protected and unprotected from sea level rise
    eq_metrics["protected_q1_households"] = households[(households.base_income_quartile == 1)].size
    eq_metrics["affected_q1_households"] = households[(households.base_income_quartile == 1)].size
    eq_metrics["pct_eq_q1_hhs_protected"] = (eq_metrics["protected_q1_households"] / eq_metrics["affected_q1_households"]) * 100   
    
    # CoC households protected and unprotected from sea level rise
    eq_metrics["protected_coc_households"] = households[(households.coc_id > '')].size
    eq_metrics["affected_coc_households"] = households[(households.coc_id > '')].size
    eq_metrics["pct_slr_coc_hhs_protected"] = (eq_metrics["protected_coc_households"] / eq_metrics["affected_coc_households"]) * 100

    eq_metrics = eq_metrics.transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    eq_metrics.to_csv(metrics_output_dir / f"{run_name}_earthquake_metrics_{year}.csv")
    

@orca.step()
def wildfire_metrics():
    # TODO (short-term): confirm this was done off-model
    # TODO (long-term): track wildfire areas in BAUS, outside of just growth geographies
    # this would allow us to understand how wildfire areas and households, jobs, and development relate
    return


@orca.step()
def greenfield_metrics(buildings, parcels, year, initial_summary_year, final_year, run_name):

    if year != initial_summary_year and year != final_year:
        return
    
    # TODO (long-term)- update the urbanized area  used, this uses "Urbanize_Footprint" shapefile joined to parcels
    # most greenfield occurs in the baseyear here since the shapefile is older than the input data
    # so also update the start year for the metric
    buildings_uf_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['urbanized', 'year_built', 'acres', 'residential_units', 'non_residential_sqft'])
    
    # new buildings outside of the urban footprint (or urbanized flag)
    buildings_out_uf = buildings_uf_df[(buildings_uf_df.year_built > 2010) & (buildings_uf_df.urbanized != 1)]
    orca.add_table("buildings_outside_urban_footprint_"+str(year), buildings_out_uf)
    
    if year != final_year:
        return
    
    greenfield_metric = pd.DataFrame(index=['total'])

    # this uses observed data (Vital Signs?)
    greenfield_metric["annual_greenfield_development_acres_2015"] = 6642/2
    # this uses the model calculation
    # We should rename these variables and potentially update 2015 observed data because initial_summary_year is not always 2015 - DSL
    buildings_out_uf_2015 = orca.get_table(f"buildings_outside_urban_footprint_{initial_summary_year}").to_frame()
    buildings_out_uf_2050 = orca.get_table(f"buildings_outside_urban_footprint_{final_year}").to_frame()
    greenfield_metric["annual_greenfield_dev_acres_2050"] = (((buildings_out_uf_2050["acres"].sum() - buildings_out_uf_2015["acres"].sum()) /
                                                              (final_year - initial_summary_year))).round(0)
    greenfield_metric = greenfield_metric.transpose()

    metrics_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "metrics"
    metrics_output_dir.mkdir(parents=True, exist_ok=True)
    greenfield_metric.to_csv(metrics_output_dir / f"{run_name}_greenfield_metric.csv")

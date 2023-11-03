from __future__ import print_function

import os
import orca
import pandas as pd
from urbansim.utils import misc

# ensure output directories are created before attempting to write to them

folder_stub = 'hazards_summaries'
target_path = os.path.join(orca.get_injectable("outputs_dir"), folder_stub)
os.makedirs(target_path, exist_ok=True)

@orca.step()
def hazards_slr_summary(run_setup, run_name, year):

    if not run_setup['run_slr']:
        return
    
    if len(orca.get_table("slr_demolish")) < 1:
        return
    
    slr_summary = pd.DataFrame(index=[0])

    # impacted parcels
    slr_summary["impacted_parcels"] = len(orca.get_table("destroy_parcels"))

    # impacted buildings
    if year == 2035:
        slr_demolish_tot = orca.get_table("slr_demolish").to_frame()
        orca.add_table("slr_demolish_tot", slr_demolish_tot)
    else:
        slr_demolish_tot = orca.get_table("slr_demolish_tot").to_frame()
        slr_demolish_tot.append(orca.get_table("slr_demolish").to_frame())

    slr_summary["impacted_units"] = slr_demolish_tot['residential_units'].sum()
    slr_summary["impacted_sqft"] = slr_demolish_tot['building_sqft'].sum()

    # impacted households
    if year == 2035:
        unplaced_hh_tot = orca.get_injectable("hh_unplaced_slr")
        orca.add_injectable("unplaced_hh_tot", unplaced_hh_tot)
    else:
        unplaced_hh_tot = orca.get_injectable("unplaced_hh_tot")
        unplaced_hh_tot.append(orca.get_injectable("hh_unplaced_slr"))

    slr_summary["impacted_hh"] = unplaced_hh_tot.size
    for quartile in [1, 2, 3, 4]:
        slr_summary["impacted_hhq"+str(quartile)] = (unplaced_hh_tot["base_income_quartile"] == quartile).sum()

    # employees by sector
    if year == 2035:
        unplaced_jobs_tot = orca.get_injectable("jobs_unplaced_slr")
        orca.add_injectable("unplaced_jobs_tot", unplaced_jobs_tot)
    else:
        unplaced_jobs_tot = orca.get_injectable("unplaced_jobs_tot")
        unplaced_jobs_tot.append(orca.get_injectable("jobs_unplaced_slr"))

    for empsix in ['AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
        slr_summary["impacted_jobs_"+str(empsix)] = (unplaced_jobs_tot["empsix"] == empsix).sum()

    slr_summary.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "hazards_summaries/%s_slr_summary_%d.csv" % (run_name, year)))


@orca.step()
def hazards_eq_summary(run_setup, run_name, year, parcels, buildings):

    if not run_setup['run_eq']:
        return
    
    if year != 2035:
        return
    
    code = orca.get_injectable("code").to_frame()
    code.value_counts().to_csv(os.path.join(orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_codes_summary_%d.csv"
                                    % (run_name, year)))

    fragilities = orca.get_injectable("fragilities").to_frame()
    fragilities.value_counts().to_csv(os.path.join(orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_fragilities_summary_%d.csv"
                                    % (run_name, year)))

    eq_summary = pd.DataFrame(index=[0])

    eq_buildings = orca.get_injectable("eq_buildings").to_frame()
    eq_summary["buildings_impacted"] = eq_buildings.size
    eq_demolish = orca.get_table("eq_demolish").to_frame()
    eq_summary['res_units_impacted'] = eq_demolish['residential_units'].sum()
    eq_summary['sqft_impacted'] = eq_demolish['building_sqft'].sum()

    existing_buildings = orca.get_injectable("existing_buildings").to_frame()
    eq_summary["existing_buildings_impacted"] = existing_buildings.size
    new_buildings = orca.get_injectable("new_buildings").to_frame()
    eq_summary["new_buildings_impacted"] = new_buildings.size

    fire_buildings = orca.get_injectable("fire_buildings").to_frame()
    eq_summary["fire_buildings_impacted"] = fire_buildings.size

    # income quartile counts
    hh_unplaced_eq = orca.get_injectable("hh_unplaced_eq").to_frame()
    for quartile in [1, 2, 3, 4]:
        eq_summary["impacted_hhq"+str(quartile)] = (hh_unplaced_eq["base_income_quartile"] == quartile).sum()

    # employees by sector
    jobs_unplaced_eq = orca.get_injectable("jobs_unplaced_eq").to_frame()
    for empsix in ['AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
        eq_summary["impacted_jobs_"+str(empsix)] = (jobs_unplaced_eq["empsix"] == empsix).sum()
    
    eq_summary.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_summary_%d.csv"
                                    % (run_name, year)))

    # print out demolished buildings by TAZ
    eq_demolish_taz = misc.reindex(parcels.zone_id, eq_demolish.parcel_id)
    eq_demolish['taz'] = eq_demolish_taz
    eq_demolish['count'] = 1
    eq_demolish = eq_demolish.drop(['parcel_id', 'year_built', 'redfin_sale_year'], axis=1)
    eq_demolish = eq_demolish.groupby(['taz']).sum()
    eq_demolish.to_csv(os.path.join(orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_demolish_buildings_%d.csv"
                                    % (run_name, year)))

    # print out retrofit buildings by TAZ
    if not run_setup['eq_mitigation']:
        return
    retrofit_bldgs_tot = orca.get_table("retrofit_bldgs_tot").to_frame()
    retrofit_bldgs_tot['taz'] = misc.reindex(parcels.zone_id, retrofit_bldgs_tot.parcel_id)
    retrofit_bldgs_tot['count'] = 1
    retrofit_bldgs_tot = retrofit_bldgs_tot.groupby(['taz']).sum()
    retrofit_bldgs_tot = retrofit_bldgs_tot[['taz', 'residential_units', 'residential_sqft',
        'non_residential_sqft', 'building_sqft', 'stories','redfin_sale_price', 'non_residential_rent',
        'deed_restricted_units', 'residential_price', 'count']]
    retrofit_bldgs_tot.to_csv(os.path.join(
                orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_retrofit_buildings_%d.csv" % (run_name, year)))

    # print out buildings by TAZ around earthquake years
    if year not in [2030, 2035, 2050]:
        return
    buildings = buildings.to_frame()
    buildings['taz'] = misc.reindex(parcels.zone_id, buildings.parcel_id)
    buildings['count'] = 1
    buildings = buildings.groupby(['taz']).sum()
    buildings = buildings[['taz', 'count', 'residential_units', 'residential_sqft', 'non_residential_sqft',
                        'building_sqft', 'stories', 'redfin_sale_price', 'non_residential_rent', 'deed_restricted_units',
                        'residential_price']]
    buildings.to_csv(os.path.join(
        orca.get_injectable("outputs_dir"), "hazards_summaries/%s_eq_buildings_list_%d.csv" % (run_name, year)))
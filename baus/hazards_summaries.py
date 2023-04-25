from __future__ import print_function

import os
import orca
import pandas as pd
from urbansim.utils import misc
from baus import datasources

@orca.step()
def hazards_slr_summary(run_setup, run_number, year):

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
            hs['hhincq1'] = (hh_unplaced_slr_cum["base_income_quartile"] == 1).sum()
            hs['hhincq2'] = (hh_unplaced_slr_cum["base_income_quartile"] == 2).sum()
            hs['hhincq3'] = (hh_unplaced_slr_cum["base_income_quartile"] == 3).sum()
            hs['hhincq4'] = (hh_unplaced_slr_cum["base_income_quartile"] == 4).sum()
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
def hazards_eq_summary(run_setup, run_number, year, parcels, buildings):

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
            hh_summary['hhincq1'] = (hh_unplaced_eq["base_income_quartile"] == 1).sum()
            hh_summary['hhincq2'] = (hh_unplaced_eq["base_income_quartile"] == 2).sum()
            hh_summary['hhincq3'] = (hh_unplaced_eq["base_income_quartile"] == 3).sum()
            hh_summary['hhincq4'] = (hh_unplaced_eq["base_income_quartile"] == 4).sum()
            hh_summary.to_string(f, index=False)

            write("")

            # employees by sector
            write("Number of impacted jobs by sector")
            jobs_unplaced_eq = orca.get_injectable("jobs_unplaced_eq")
            jobs_summary = pd.DataFrame(index=[0])
            jobs_summary['agrempn'] = (jobs_unplaced_eq["empsix"] == 'AGREMPN').sum()
            jobs_summary['mwtempn'] = (jobs_unplaced_eq["empsix"] == 'MWTEMPN').sum()
            jobs_summary['retempn'] = (jobs_unplaced_eq["empsix"] == 'RETEMPN').sum()
            jobs_summary['fpsempn'] = (jobs_unplaced_eq["empsix"] == 'FPSEMPN').sum()
            jobs_summary['herempn'] = (jobs_unplaced_eq["empsix"] == 'HEREMPN').sum()
            jobs_summary['othempn'] = (jobs_unplaced_eq["empsix"] == 'OTHEMPN').sum()
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
                retrofit_bldgs_tot.to_csv(os.path.join(
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
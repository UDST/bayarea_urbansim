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
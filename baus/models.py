from __future__ import print_function

import os
import sys
import yaml

import numpy as np
import pandas as pd

import orca
import pandana.network as pdna
from urbansim.developer import sqftproforma
from urbansim.developer.developer import Developer as dev
from urbansim.utils import misc, networks
from urbansim_defaults import models, utils

from baus import datasources, subsidies, summaries, variables
from baus.utils import \
    add_buildings, geom_id_to_parcel_id, groupby_random_choice, \
    parcel_id_to_geom_id, round_series_match_target


@orca.step()
def elcm_simulate(jobs, buildings, aggregations, elcm_config):
    buildings.local["non_residential_rent"] = \
        buildings.local.non_residential_rent.fillna(0)
    return utils.lcm_simulate(elcm_config, jobs, buildings, aggregations,
                              "building_id", "job_spaces",
                              "vacant_job_spaces", cast=True)


@orca.step()
def households_transition(households, household_controls, year, settings):
    s = orca.get_table('households').base_income_quartile.value_counts()
    print("Distribution by income before:\n", (s/s.sum()))
    ret = utils.full_transition(households,
                                household_controls,
                                year,
                                settings['households_transition'],
                                "building_id")
    s = orca.get_table('households').base_income_quartile.value_counts()
    print("Distribution by income after:\n", (s/s.sum()))
    return ret


@orca.table(cache=True)
def employment_relocation_rates():
    df = pd.read_csv(os.path.join("data", "employment_relocation_rates.csv"))
    df = df.set_index("zone_id").stack().reset_index()
    df.columns = ["zone_id", "empsix", "rate"]
    return df


@orca.table(cache=True)
def household_relocation_rates(scenario, policy):
    if scenario in policy['reloc_fr2_enable']:
        df = pd.read_csv(os.path.join("data",
                                      "household_relocation_rates_fr2.csv"))
        orca.add_injectable("hh_reloc", 'activated')
    else:
        df = pd.read_csv(os.path.join("data",
                                      "household_relocation_rates.csv"))
        orca.add_injectable("hh_reloc", 'not activated')
    return df


# this is a list of parcel_ids which are to be treated as static
@orca.injectable()
def static_parcels(settings, parcels):
    # list of geom_ids to not relocate
    static_parcels = settings["static_parcels"]
    # geom_ids -> parcel_ids
    return geom_id_to_parcel_id(
        pd.DataFrame(index=static_parcels), parcels).index.values


def _proportional_jobs_model(
    target_ratio,  # ratio of jobs of this sector to households
    sector,        # empsix sector
    groupby_col,   # ratio will be matched at this level of geog
    hh_df,
    jobs_df,
    locations_series,
    target_jobs=None  # pass this if you want to compute target jobs
):

    if target_jobs is None:
        # compute it if not passed
        target_jobs = hh_df[groupby_col].value_counts() * target_ratio
        target_jobs = target_jobs.astype('int')

    current_jobs = jobs_df[
        jobs_df.empsix == sector][groupby_col].value_counts()
    need_more_jobs = target_jobs - current_jobs
    need_more_jobs = need_more_jobs[need_more_jobs > 0]
    need_more_jobs_total = int(need_more_jobs.sum())

    available_jobs = \
        jobs_df.query("empsix == '%s' and building_id == -1" % sector)

    print("Need more jobs total: %d" % need_more_jobs_total)
    print("Available jobs: %d" % len(available_jobs))

    if len(available_jobs) == 0:
        # corner case
        return pd.Series()

    if len(available_jobs) >= need_more_jobs_total:

        # have enough jobs to assign, truncate available jobs
        available_jobs = available_jobs.head(need_more_jobs_total)

    else:

        # don't have enough jobs - random sample locations to partially
        # match the need (won't succed matching the entire need)
        need_more_jobs = round_series_match_target(
            need_more_jobs, len(available_jobs), 0)
        need_more_jobs_total = need_more_jobs.sum()

    assert need_more_jobs_total == len(available_jobs)

    if need_more_jobs_total <= 0:
        return pd.Series()

    print("Need more jobs\n", need_more_jobs)

    excess = need_more_jobs.sub(locations_series.value_counts(), fill_value=0)
    print("Excess demand\n", excess[excess > 0])

    # there's an issue with groupby_random_choice where it can't choose from
    # a set of locations that don't exist - e.g. we have 2 jobs in a certain
    # city but not locations to put them in.  we need to drop this demand
    drop = need_more_jobs.index.difference(locations_series.unique())
    print("We don't have any locations for these locations:\n", drop)
    need_more_jobs = need_more_jobs.drop(drop).astype('int')

    # choose random locations within jurises to match need_more_jobs totals
    choices = groupby_random_choice(locations_series, need_more_jobs,
                                    replace=True)

    # these might not be the same length after dropping a few lines above
    available_jobs = available_jobs.head(len(choices))

    return pd.Series(choices.index, available_jobs.index)


@orca.step()
def accessory_units(year, buildings, parcels):
    add_units = pd.read_csv("data/accessory_units.csv",
                            index_col="juris")[str(year)]
    buildings_juris = misc.reindex(parcels.juris, buildings.parcel_id)
    res_buildings = buildings_juris[buildings.general_type == "Residential"]
    add_buildings = groupby_random_choice(res_buildings, add_units)
    add_buildings = pd.Series(add_buildings.index).value_counts()
    buildings.local.loc[add_buildings.index, "residential_units"] += \
        add_buildings.values


@orca.step()
def proportional_elcm(jobs, households, buildings, parcels,
                      year, run_number):

    juris_assumptions_df = pd.read_csv(os.path.join(
        "data",
        "juris_assumptions.csv"
    ), index_col="juris")

    # not a big fan of this - jobs with building_ids of -1 get dropped
    # by the merge so you have to grab the columns first and fill in
    # juris iff the building_id is != -1
    jobs_df = jobs.to_frame(["building_id", "empsix"])
    df = orca.merge_tables(
        target='jobs',
        tables=[jobs, buildings, parcels],
        columns=['juris', 'zone_id'])
    jobs_df["juris"] = df["juris"]
    jobs_df["zone_id"] = df["zone_id"]

    hh_df = orca.merge_tables(
        target='households',
        tables=[households, buildings, parcels],
        columns=['juris', 'zone_id', 'county'])

    # the idea here is to make sure we don't lose local retail and gov't
    # jobs - there has to be some amount of basic services to support an
    # increase in population

    buildings_df = orca.merge_tables(
        target='buildings',
        tables=[buildings, parcels],
        columns=['juris', 'zone_id', 'general_type', 'vacant_job_spaces'])

    buildings_df = buildings_df.rename(columns={
      'zone_id_x': 'zone_id', 'general_type_x': 'general_type'})

    # location options are vacant job spaces in retail buildings - this will
    # overfill certain location because we don't have enough space
    building_subset = buildings_df[buildings_df.general_type == "Retail"]
    location_options = building_subset.juris.repeat(
        building_subset.vacant_job_spaces.clip(0))

    print("Running proportional jobs model for retail")

    s = _proportional_jobs_model(
        # we now take the ratio of retail jobs to households as an input
        # that is manipulable by the modeler - this is stored in a csv
        # per jurisdiction
        juris_assumptions_df.minimum_forecast_retail_jobs_per_household,
        "RETEMPN",
        "juris",
        hh_df,
        jobs_df,
        location_options
    )

    jobs.update_col_from_series("building_id", s)

    # first read the file from disk - it's small so no table source
    taz_assumptions_df = pd.read_csv(os.path.join(
        "data",
        "taz_growth_rates_gov_ed.csv"
    ), index_col="Taz")

    # we're going to multiply various aggregations of populations by factors
    # e.g. high school jobs are multiplied by county pop and so forth - this
    # is the dict of the aggregations of household counts
    mapping_d = {
        "TAZ Pop": hh_df["zone_id"].dropna().astype('int').value_counts(),
        "County Pop": taz_assumptions_df.County.map(
            hh_df["county"].value_counts()),
        "Reg Pop": len(hh_df)
    }
    # the factors are set up in relation to pop, not hh count
    pop_to_hh = .43

    # don't need county anymore
    del taz_assumptions_df["County"]

    # multipliers are in first row (not counting the headers)
    multipliers = taz_assumptions_df.iloc[0]
    # done with the row
    taz_assumptions_df = taz_assumptions_df.iloc[1:]

    # this is weird but Pandas was giving me a strange error when I tried
    # to change the type of the index directly
    taz_assumptions_df = taz_assumptions_df.reset_index()
    taz_assumptions_df["Taz"] = taz_assumptions_df.Taz.astype("int")
    taz_assumptions_df = taz_assumptions_df.set_index("Taz")

    # now go through and multiply each factor by the aggregation it applied to
    target_jobs = pd.Series(0, taz_assumptions_df.index)
    for col, mult in zip(taz_assumptions_df.columns, multipliers):
        target_jobs += (taz_assumptions_df[col].astype('float') *
                        mapping_d[mult] * pop_to_hh).fillna(0)

    target_jobs = target_jobs.astype('int')

    print("Running proportional jobs model for gov/edu")

    # location options are vacant job spaces in retail buildings - this will
    # overfill certain location because we don't have enough space
    building_subset = buildings_df[
        buildings.general_type.isin(["Office", "School"])]
    location_options = building_subset.zone_id.repeat(
        building_subset.vacant_job_spaces.clip(0))

    # now do the same thing for gov't jobs
    s = _proportional_jobs_model(
        None,  # computing jobs directly
        "OTHEMPN",
        "zone_id",
        hh_df,
        jobs_df,
        location_options,
        target_jobs=target_jobs
    )

    jobs.update_col_from_series("building_id", s, cast=True)


@orca.step()
def jobs_relocation(jobs, employment_relocation_rates, years_per_iter,
                    settings, static_parcels, buildings):

    # get buildings that are on those parcels
    static_buildings = buildings.index[
        buildings.parcel_id.isin(static_parcels)]

    df = pd.merge(jobs.to_frame(["zone_id", "empsix"]),
                  employment_relocation_rates.local,
                  on=["zone_id", "empsix"],
                  how="left")

    df.index = jobs.index

    # get the move rate for each job
    rate = (df.rate * years_per_iter).clip(0, 1.0)
    # get random floats and move jobs if they're less than the rate
    move = np.random.random(len(rate)) < rate

    # also don't move jobs that are on static parcels
    move &= ~jobs.building_id.isin(static_buildings)

    # get the index of the moving jobs
    index = jobs.index[move]

    # set jobs that are moving to a building_id of -1 (means unplaced)
    jobs.update_col_from_series("building_id",
                                pd.Series(-1, index=index))


@orca.step()
def household_relocation(households, household_relocation_rates,
                         settings, static_parcels, buildings):

    # get buildings that are on those parcels
    static_buildings = buildings.index[
        buildings.parcel_id.isin(static_parcels)]

    df = pd.merge(households.to_frame(["zone_id", "base_income_quartile",
                                       "tenure"]),
                  household_relocation_rates.local,
                  on=["zone_id", "base_income_quartile", "tenure"],
                  how="left")

    df.index = households.index

    # get random floats and move households if they're less than the rate
    move = np.random.random(len(df.rate)) < df.rate

    # also don't move households that are on static parcels
    move &= ~households.building_id.isin(static_buildings)

    # get the index of the moving jobs
    index = households.index[move]
    print("{} households are relocating".format(len(index)))

    # set households that are moving to a building_id of -1 (means unplaced)
    households.update_col_from_series("building_id",
                                      pd.Series(-1, index=index), cast=True)


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time
@orca.step()
def scheduled_development_events(buildings, development_projects,
                                 demolish_events, summary, year, parcels,
                                 mapping, years_per_iter, parcels_geography,
                                 building_sqft_per_job, vmt_fee_categories,
                                 static_parcels):
    # first demolish
    demolish = demolish_events.to_frame().\
        query("%d <= year_built < %d" % (year, year + years_per_iter))
    print("Demolishing/building %d buildings" % len(demolish))
    l1 = len(buildings)
    buildings = utils._remove_developed_buildings(
        buildings.to_frame(buildings.local_columns),
        demolish,
        unplace_agents=["households", "jobs"])
    orca.add_injectable('static_parcels',
                        np.append(static_parcels,
                                  demolish.loc[demolish.action == 'build',
                                               'parcel_id']))
    orca.add_table("buildings", buildings)
    buildings = orca.get_table("buildings")
    print("Demolished %d buildings" % (l1 - len(buildings)))
    print("    (this number is smaller when parcel has no existing buildings)")

    # then build
    dps = development_projects.to_frame().\
        query("%d <= year_built < %d" % (year, year + years_per_iter))

    if len(dps) == 0:
        return

    new_buildings = utils.scheduled_development_events(
        buildings, dps,
        remove_developed_buildings=False,
        unplace_agents=['households', 'jobs'])
    new_buildings["form"] = new_buildings.building_type.map(
        mapping['building_type_map']).str.lower()
    new_buildings["job_spaces"] = new_buildings.non_residential_sqft / \
        new_buildings.building_type.fillna("OF").map(building_sqft_per_job)
    new_buildings["job_spaces"] = new_buildings.job_spaces.\
        fillna(0).astype('int')
    new_buildings["geom_id"] = parcel_id_to_geom_id(new_buildings.parcel_id)
    new_buildings["SDEM"] = True
    new_buildings["subsidized"] = False

    new_buildings["zone_id"] = misc.reindex(
        parcels.zone_id, new_buildings.parcel_id)
    new_buildings["vmt_res_cat"] = misc.reindex(
        vmt_fee_categories.res_cat, new_buildings.zone_id)
    del new_buildings["zone_id"]
    new_buildings["pda"] = parcels_geography.pda_id.loc[
        new_buildings.parcel_id].values
    new_buildings["juris_trich"] = parcels_geography.juris_trich.loc[
        new_buildings.parcel_id].values

    summary.add_parcel_output(new_buildings)


@orca.injectable(autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
    s = demand / supply
    settings = orca.get_injectable('settings')
    print("Number of submarkets where demand exceeds supply:", len(s[s > 1.0]))
    # print "Raw relationship of supply and demand\n", s.describe()
    supply_correction = settings["price_equilibration"]
    clip_change_high = supply_correction["kwargs"]["clip_change_high"]
    t = s
    t -= 1.0
    t = t / t.max() * (clip_change_high-1)
    t += 1.0
    s.loc[s > 1.0] = t.loc[s > 1.0]
    return s, (s <= 1.0).all()


# this if the function for mapping a specific building that we build to a
# specific building type
@orca.injectable(autocall=False)
def form_to_btype_func(building):
    mapping = orca.get_injectable('mapping')
    form = building.form
    dua = building.residential_units / (building.parcel_size / 43560.0)
    # precise mapping of form to building type for residential
    if form is None or form == "residential":
        if dua < 16:
            return "HS"
        elif dua < 32:
            return "HT"
        return "HM"
    return mapping["form_to_btype"][form][0]


@orca.injectable(autocall=False)
def add_extra_columns_func(df):
    for col in ["residential_price", "non_residential_rent"]:
        df[col] = 0

    if "deed_restricted_units" not in df.columns:
        df["deed_restricted_units"] = 0
    else:
        print("Number of deed restricted units built = %d" %
              df.deed_restricted_units.sum())

    df["redfin_sale_year"] = 2012
    df["redfin_sale_price"] = np.nan

    if "residential_units" not in df:
        df["residential_units"] = 0

    if "parcel_size" not in df:
        df["parcel_size"] = \
            orca.get_table("parcels").parcel_size.loc[df.parcel_id]

    if orca.is_injectable("year") and "year_built" not in df:
        df["year_built"] = orca.get_injectable("year")

    if orca.is_injectable("form_to_btype_func") and \
            "building_type" not in df:
        form_to_btype_func = orca.get_injectable("form_to_btype_func")
        df["building_type"] = df.apply(form_to_btype_func, axis=1)

    return df


@orca.step()
def alt_feasibility(parcels, settings,
                    parcel_sales_price_sqft_func,
                    parcel_is_allowed_func):
    kwargs = settings['feasibility']
    config = sqftproforma.SqFtProFormaConfig()
    config.parking_rates["office"] = 1.5
    config.parking_rates["retail"] = 1.5
    config.building_efficiency = .85
    config.parcel_coverage = .85
    # use the cap rate from settings.yaml
    config.cap_rate = settings["cap_rate"]

    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=config,
                          **kwargs)

    f = subsidies.policy_modifications_of_profit(
        orca.get_table('feasibility').to_frame(),
        parcels)

    orca.add_table("feasibility", f)


@orca.step()
def residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func, parcels_geography,
                          limits_settings, final_year,
                          regional_controls):

    kwargs = settings['residential_developer']
    rc = regional_controls.to_frame()
    target_vacancy = rc.loc[year].st_res_vac

    num_units = dev.compute_units_to_build(
        len(households),
        buildings["residential_units"].sum(),
        target_vacancy)

    targets = []
    typ = "Residential"
    # now apply limits - limits are assumed to be yearly, apply to an
    # entire jurisdiction and be in terms of residential_units or job_spaces
    if typ in limits_settings:

        juris_name = parcels_geography.juris_name.\
            reindex(parcels.index).fillna('Other')

        juris_list = limits_settings[typ].keys()
        for juris, limit in limits_settings[typ].items():

            # the actual target is the limit times the number of years run
            # so far in the simulation (plus this year), minus the amount
            # built in previous years - in other words, you get rollover
            # and development is lumpy

            current_total = parcels.total_residential_units[
                (juris_name == juris) & (parcels.newest_building >= 2010)]\
                .sum()

            target = (year - 2010 + 1) * limit - current_total
            # make sure we don't overshoot the total development of the limit
            # for the horizon year - for instance, in Half Moon Bay we have
            # a very low limit and a single development in a far out year can
            # easily build over the limit for the total simulation
            max_target = (final_year - 2010 + 1) * limit - current_total

            if target <= 0:
                continue

            targets.append((juris_name == juris, target, max_target, juris))
            num_units -= target

        # other cities not in the targets get the remaining target
        targets.append((~juris_name.isin(juris_list), num_units, None, "none"))

    else:
        # otherwise use all parcels with total number of units
        targets.append((parcels.index == parcels.index,
                        num_units, None, "none"))

    for parcel_mask, target, final_target, juris in targets:

        print("Running developer for %s with target of %d" %
              (str(juris), target))

        # this was a fairly heinous bug - have to get the building wrapper
        # again because the buildings df gets modified by the run_developer
        # method below
        buildings = orca.get_table('buildings')

        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size[parcel_mask],
            parcels.ave_sqft_per_unit[parcel_mask],
            parcels.total_residential_units[parcel_mask],
            feasibility,
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            num_units_to_build=int(target),
            profit_to_prob_func=subsidies.profit_to_prob_func,
            **kwargs)

        buildings = orca.get_table('buildings')

        if new_buildings is not None:
            new_buildings["subsidized"] = False

        if final_target is not None and new_buildings is not None:
            # make sure we don't overbuild the target for the whole simulation
            overshoot = new_buildings.net_units.sum() - final_target

            if overshoot > 0:
                index = new_buildings.tail(1).index[0]
                index = int(index)
                # make sure we don't get into a negative unit situation
                current_units = buildings.local.loc[index, "residential_units"]
                # only can reduce by as many units as we have
                overshoot = min(overshoot, current_units)
                # used below - this is the pct we need to reduce the building
                overshoot_pct = \
                    (current_units - overshoot) / float(current_units)

                buildings.local.loc[index, "residential_units"] -= overshoot

                # we also need to fix the other columns so they make sense
                for col in ["residential_sqft", "building_sqft",
                            "deed_restricted_units"]:
                    val = buildings.local.loc[index, col]
                    # reduce by pct but round to int
                    buildings.local.loc[index, col] = int(val * overshoot_pct)

        summary.add_parcel_output(new_buildings)


@orca.step()
def retail_developer(jobs, buildings, parcels, nodes, feasibility,
                     settings, summary, add_extra_columns_func, net):

    dev_settings = settings['non_residential_developer']
    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    target = all_units * float(dev_settings['type_splits']["Retail"])
    # target here is in sqft
    target *= settings["building_sqft_per_job"]["HS"]

    feasibility = feasibility.to_frame().loc[:, "retail"]
    feasibility = feasibility.dropna(subset=["max_profit"])

    feasibility["non_residential_sqft"] = \
        feasibility.non_residential_sqft.astype("int")

    feasibility["retail_ratio"] = parcels.retail_ratio
    feasibility = feasibility.reset_index()

    # create features
    f1 = feasibility.retail_ratio / feasibility.retail_ratio.max()
    f2 = feasibility.max_profit / feasibility.max_profit.max()

    # combine features in probability function - it's like combining expense
    # of building the building with the market in the neighborhood
    p = f1 * 1.5 + f2
    p = p.clip(lower=1.0/len(p)/10)

    print("Attempting to build {:,} retail sqft".format(target))

    # order by weighted random sample
    feasibility = feasibility.sample(frac=1.0, weights=p)

    bldgs = buildings.to_frame(buildings.local_columns + ["general_type"])

    devs = []

    for dev_id, d in feasibility.iterrows():

        if target <= 0:
            break

        # any special logic to filter these devs?

        # remove new dev sqft from target
        target -= d.non_residential_sqft

        # add redeveloped sqft to target
        filt = "general_type == 'Retail' and parcel_id == %d" % \
            d["parcel_id"]
        target += bldgs.query(filt).non_residential_sqft.sum()

        devs.append(d)

    if len(devs) == 0:
        return

    # record keeping - add extra columns to match building dataframe
    # add the buidings and demolish old buildings, and add to debug output
    devs = pd.DataFrame(devs, columns=feasibility.columns)

    print("Building {:,} retail sqft in {:,} projects".format(
        devs.non_residential_sqft.sum(), len(devs)))
    if target > 0:
        print("   WARNING: retail target not met")

    devs["form"] = "retail"
    devs = add_extra_columns_func(devs)

    add_buildings(buildings, devs)

    summary.add_parcel_output(devs)


@orca.step()
def office_developer(feasibility, jobs, buildings, parcels, year,
                     settings, summary, form_to_btype_func, scenario,
                     add_extra_columns_func, parcels_geography,
                     limits_settings):

    dev_settings = settings['non_residential_developer']

    # I'm going to try a new way of computing this because the math the other
    # way is simply too hard.  Basically we used to try and apportion sectors
    # into the demand for office, retail, and industrial, but there's just so
    # much dirtyness to the data, for instance 15% of jobs are in residential
    # buildings, and 15% in other buildings, it's just hard to know how much
    # to build, we I think the right thing to do is to compute the number of
    # job spaces that are required overall, and then to apportion that new dev
    # into the three non-res types with a single set of coefficients
    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    print("Total units to build = %d" % all_units)
    if all_units <= 0:
        return

    for typ in ["Office"]:

        print("\nRunning for type: ", typ)

        num_units = all_units * float(dev_settings['type_splits'][typ])

        targets = []
        # now apply limits - limits are assumed to be yearly, apply to an
        # entire jurisdiction and be in terms of residential_units or
        # job_spaces
        if year > 2015 and typ in limits_settings:

            juris_name = parcels_geography.juris_name.\
                reindex(parcels.index).fillna('Other')

            juris_list = limits_settings[typ].keys()
            for juris, limit in limits_settings[typ].items():

                # the actual target is the limit times the number of years run
                # so far in the simulation (plus this year), minus the amount
                # built in previous years - in other words, you get rollover
                # and development is lumpy

                current_total = parcels.total_job_spaces[
                    (juris_name == juris) &
                    (parcels.newest_building > 2015)].sum()

                target = (year - 2015 + 1) * limit - current_total

                if target <= 0:
                    print("Already met target for juris = %s" % juris)
                    print("    target = %d, current_total = %d" %
                          (target, current_total))
                    continue

                targets.append((juris_name == juris, target, juris))
                num_units -= target

            # other cities not in the targets get the remaining target
            targets.append((~juris_name.isin(juris_list), num_units, "none"))

        else:
            # otherwise use all parcels with total number of units
            targets.append((parcels.index == parcels.index, num_units, "none"))

        for parcel_mask, target, juris in targets:

            print("Running developer for %s with target of %d" %
                  (str(juris), target))
            print("Parcels in play:\n", pd.Series(parcel_mask).value_counts())

            # this was a fairly heinous bug - have to get the building wrapper
            # again because the buildings df gets modified by the run_developer
            # method below
            buildings = orca.get_table('buildings')

            new_buildings = utils.run_developer(
                typ.lower(),
                jobs,
                buildings,
                "job_spaces",
                parcels.parcel_size[parcel_mask],
                parcels.ave_sqft_per_unit[parcel_mask],
                parcels.total_job_spaces[parcel_mask],
                feasibility,
                year=year,
                form_to_btype_callback=form_to_btype_func,
                add_more_columns_callback=add_extra_columns_func,
                residential=False,
                num_units_to_build=int(target),
                profit_to_prob_func=subsidies.profit_to_prob_func,
                **dev_settings['kwargs'])

            if new_buildings is not None:
                new_buildings["subsidized"] = False

            summary.add_parcel_output(new_buildings)


@orca.step()
def developer_reprocess(buildings, year, years_per_iter, jobs,
                        parcels, summary, parcel_is_allowed_func):
    # this takes new units that come out of the developer, both subsidized
    # and non-subsidized and reprocesses them as required - please read
    # comments to see what this means in detail

    # 20% of base year buildings which are "residential" have job spaces - I
    # mean, there is a ratio of job spaces to res units in residential
    # buildings of 1 to 5 - this ratio should be kept for future year
    # buildings
    s = buildings.general_type == "Residential"
    res_units = buildings.residential_units[s].sum()
    job_spaces = buildings.job_spaces[s].sum()

    to_add = res_units * .05 - job_spaces
    if to_add > 0:
        print("Adding %d job_spaces" % to_add)
        res_units = buildings.residential_units[s]
        # bias selection of places to put job spaces based on res units
        print(res_units.describe())
        print(res_units[res_units < 0])
        add_indexes = np.random.choice(res_units.index.values, size=to_add,
                                       replace=True,
                                       p=(res_units/res_units.sum()))
        # collect same indexes
        add_indexes = pd.Series(add_indexes).value_counts()
        # this is sqft per job for residential bldgs
        add_sizes = add_indexes * 400
        print("Job spaces in res before adjustment: ",
              buildings.job_spaces[s].sum())
        buildings.local.loc[add_sizes.index,
                            "non_residential_sqft"] += add_sizes.values
        print("Job spaces in res after adjustment: ",
              buildings.job_spaces[s].sum())

    # the second step here is to add retail to buildings that are greater than
    # X stories tall - presumably this is a ground floor retail policy
    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = old_buildings.query(
       '%d == year_built and stories >= 4' % year)

    print("Attempting to add ground floor retail to %d devs" %
          len(new_buildings))
    retail = parcel_is_allowed_func("retail")
    new_buildings = new_buildings[retail.loc[new_buildings.parcel_id].values]
    print("Disallowing dev on these parcels:")
    print("    %d devs left after retail disallowed" % len(new_buildings))

    # this is the key point - make these new buildings' nonres sqft equal
    # to one story of the new buildings
    new_buildings.non_residential_sqft = new_buildings.building_sqft / \
        new_buildings.stories * .8

    new_buildings["residential_units"] = 0
    new_buildings["residential_sqft"] = 0
    new_buildings["deed_restricted_units"] = 0
    new_buildings["building_sqft"] = new_buildings.non_residential_sqft
    new_buildings["stories"] = 1
    new_buildings["building_type"] = "RB"

    # this is a fairly arbitrary rule, but we're only adding ground floor
    # retail in areas that are underserved right now - this is defined as
    # the location where the retail ratio (ratio of income to retail sqft)
    # is greater than the median
    ratio = parcels.retail_ratio.loc[new_buildings.parcel_id]
    new_buildings = new_buildings[ratio.values > ratio.median()]

    print("Adding %d sqft of ground floor retail in %d locations" %
          (new_buildings.non_residential_sqft.sum(), len(new_buildings)))

    all_buildings = dev.merge(old_buildings, new_buildings)
    orca.add_table("buildings", all_buildings)

    new_buildings["form"] = "retail"
    # this is sqft per job for retail use - this is all rather
    # ad-hoc so I'm hard-coding
    new_buildings["job_spaces"] = \
        (new_buildings.non_residential_sqft / 445.0).astype('int')
    new_buildings["net_units"] = new_buildings.job_spaces
    summary.add_parcel_output(new_buildings)

    # got to get the frame again because we just added rows
    buildings = orca.get_table('buildings')
    buildings_df = buildings.to_frame(
        ['year_built', 'building_sqft', 'general_type'])
    sqft_by_gtype = buildings_df.query('year_built >= %d' % year).\
        groupby('general_type').building_sqft.sum()
    print("New square feet by general type in millions:\n",
          sqft_by_gtype / 1000000.0)


def proportional_job_allocation(parcel_id):
    # this method takes a parcel and increases the number of jobs on the
    # parcel in proportion to the ratio of sectors that existed in the base yr
    # this is because elcms can't get the distribution right in some cases, eg
    # to keep mostly gov't jobs in city hall, etc - these are largely
    # institutions and not subject to the market

    # get buildings on this parcel
    buildings = orca.get_table("buildings").to_frame(
        ["parcel_id", "job_spaces", "zone_id", "year_built"]).\
        query("parcel_id == %d" % parcel_id)

    # get jobs in those buildings
    all_jobs = orca.get_table("jobs").local
    jobs = all_jobs[
        all_jobs.building_id.isin(buildings.query("year_built <= 2015").index)]

    # get job distribution by sector for this parcel
    job_dist = jobs.empsix.value_counts()

    # only add jobs to new buildings records
    for index, building in buildings.query("year_built > 2015").iterrows():

        num_new_jobs = building.job_spaces - len(
            all_jobs.query("building_id == %d" % index))

        if num_new_jobs == 0:
            continue

        sectors = np.random.choice(job_dist.index, size=num_new_jobs,
                                   p=job_dist/job_dist.sum())
        new_jobs = pd.DataFrame({"empsix": sectors, "building_id": index})
        # make sure index is incrementing
        new_jobs.index = new_jobs.index + 1 + np.max(all_jobs.index.values)

        print("Adding {} new jobs to parcel {} with proportional model".format(
            num_new_jobs, parcel_id))
        print(new_jobs.head())
        all_jobs = all_jobs.append(new_jobs)
        orca.add_table("jobs", all_jobs)


@orca.step()
def static_parcel_proportional_job_allocation(static_parcels):
    for parcel_id in static_parcels:
        proportional_job_allocation(parcel_id)


def make_network(name, weight_col, max_distance):
    st = pd.HDFStore(os.path.join(misc.data_dir(), name), "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[[weight_col]])
    net.precompute(max_distance)
    return net


def make_network_from_settings(settings):
    return make_network(
        settings["name"],
        settings.get("weight_col", "weight"),
        settings['max_distance']
    )


@orca.injectable(cache=True)
def net(settings):
    nets = {}
    pdna.reserve_num_graphs(len(settings["build_networks"]))

    # yeah, starting to hardcode stuff, not great, but can only
    # do nearest queries on the first graph I initialize due to crummy
    # limitation in pandana
    for key in settings["build_networks"].keys():
        nets[key] = make_network_from_settings(
            settings['build_networks'][key]
        )

    return nets


@orca.step()
def local_pois(settings):
    # because of the aforementioned limit of one netowrk at a time for the
    # POIS, as well as the large amount of memory used, this is now a
    # preprocessing step
    n = make_network(
        settings['build_networks']['walk']['name'],
        "weight", 3000)

    n.init_pois(
        num_categories=1,
        max_dist=3000,
        max_pois=1)

    cols = {}

    locations = pd.read_csv(os.path.join(misc.data_dir(), 'bart_stations.csv'))
    n.set_pois("tmp", locations.lng, locations.lat)
    cols["bartdist"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    locname = 'pacheights'
    locs = orca.get_table('landmarks').local.query("name == '%s'" % locname)
    n.set_pois("tmp", locs.lng, locs.lat)
    cols["pacheights"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    df.index.name = "node_id"
    df.to_csv('local_poi_distances.csv')


@orca.step()
def neighborhood_vars(net):
    nodes = networks.from_yaml(net["walk"], "neighborhood_vars.yaml")
    nodes = nodes.replace(-np.inf, np.nan)
    nodes = nodes.replace(np.inf, np.nan)
    nodes = nodes.fillna(0)

    print(nodes.describe())
    orca.add_table("nodes", nodes)


@orca.step()
def regional_vars(net):
    nodes = networks.from_yaml(net["drive"], "regional_vars.yaml")
    nodes = nodes.fillna(0)

    nodes2 = pd.read_csv('data/regional_poi_distances.csv',
                         index_col="tmnode_id")
    nodes = pd.concat([nodes, nodes2], axis=1)

    print(nodes.describe())
    orca.add_table("tmnodes", nodes)


@orca.step()
def regional_pois(settings, landmarks):
    # because of the aforementioned limit of one netowrk at a time for the
    # POIS, as well as the large amount of memory used, this is now a
    # preprocessing step
    n = make_network(
        settings['build_networks']['drive']['name'],
        "CTIMEV", 75)

    n.init_pois(
        num_categories=1,
        max_dist=75,
        max_pois=1)

    cols = {}
    for locname in ["embarcadero", "stanford", "pacheights"]:
        locs = landmarks.local.query("name == '%s'" % locname)
        n.set_pois("tmp", locs.lng, locs.lat)
        cols[locname] = n.nearest_pois(75, "tmp", num_pois=1)[1]

    df = pd.DataFrame(cols)
    print(df.describe())
    df.index.name = "tmnode_id"
    df.to_csv('regional_poi_distances.csv')


@orca.step()
def price_vars(net):
    nodes2 = networks.from_yaml(net["walk"], "price_vars.yaml")
    nodes2 = nodes2.fillna(0)
    print(nodes2.describe())
    nodes = orca.get_table('nodes')
    nodes = nodes.to_frame().join(nodes2)
    orca.add_table("nodes", nodes)

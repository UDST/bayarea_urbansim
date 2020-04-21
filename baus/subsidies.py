from __future__ import print_function

import sys
import time
import orca
import pandas as pd
import numpy as np
from functools import reduce
from urbansim import accounts
from urbansim_defaults import utils
from six import StringIO
from urbansim.utils import misc
from baus.utils import add_buildings
from urbansim.developer import sqftproforma


# this method is a custom profit to probability function where we test the
# combination of different metrics like return on cost and raw profit
def profit_to_prob_func(df):
    # the clip is because we still might build negative profit buildings
    # (when we're subsidizing them) and choice doesn't allow negative
    # probability options
    max_profit = df.max_profit.clip(1)

    factor = float(orca.get_injectable("settings")[
        "profit_vs_return_on_cost_combination_factor"])

    df['return_on_cost'] = max_profit / df.total_cost

    # now we're going to make two pdfs and weight them
    ROC_p = df.return_on_cost.values / df.return_on_cost.sum()
    profit_p = max_profit / max_profit.sum()
    p = 1.0 * ROC_p + factor * profit_p

    return p / p.sum()


@orca.injectable(cache=True)
def coffer(policy):
    d = {
        "vmt_res_acct":  accounts.Account("vmt_res_acct"),
        "vmt_com_acct":  accounts.Account("vmt_com_acct")
    }

    for key, acct in policy["acct_settings"]["lump_sum_accounts"].items():
        d[acct["name"]] = accounts.Account(acct["name"])

    return d


@orca.injectable(cache=True)
def acct_settings(policy):
    return policy["acct_settings"]


@orca.step()
def lump_sum_accounts(policy, year, buildings, coffer,
                      summary, years_per_iter, scenario):

    s = policy["acct_settings"]["lump_sum_accounts"]

    for key, acct in s.items():

        if scenario not in acct["enable_in_scenarios"]:
            continue

        if "alternate_geography_scenarios" in acct and \
                scenario in acct["alternate_geography_scenarios"]:
            acct["receiving_buildings_filter"] = \
                acct["alternate_buildings_filter"]

        amt = float(acct["total_amount"])

        amt *= years_per_iter

        metadata = {
            "description": "%s subsidies" % acct["name"],
            "year": year
        }
        # the subaccount is meaningless here (it's a regional account) -
        # but the subaccount number is referred to below
        coffer[acct["name"]].add_transaction(amt, subaccount=1,
                                             metadata=metadata)


# this will compute the reduction in revenue from a project due to
# inclustionary housing - the calculation will be described in thorough
# comments alongside the code
def inclusionary_housing_revenue_reduction(feasibility, units):

    print("Computing adjustments due to inclusionary housing")

    # AMI by jurisdiction
    #
    # in practice deed restrictions are done by household size but we aren't
    # going to deed restrict them by household size so it makes sense not to
    # do that here - if we did this by household size like we do in the real
    # world we'd need to have a better representation of what household size
    # is in which unit type

    households = orca.get_table("households")
    buildings = orca.get_table("buildings")
    parcels_geography = orca.get_table("parcels_geography")
    h = orca.merge_tables("households",
                          [households, buildings, parcels_geography],
                          columns=["juris_name", "income"])
    AMI = h.groupby(h.juris_name).income.quantile(.5)

    # per Aksel Olsen (@akselx)
    # take 90% of AMI and multiple by 33% to get the max amount a
    # household can pay per year, divide by 12 to get monthly amt,
    # subtract condo fee

    monthly_condo_fee = 250
    monthly_affordable_payment = AMI * .9 * .33 / 12 - monthly_condo_fee

    def value_can_afford(monthly_payment):
        # this is a 10 year average freddie mac interest rate
        ten_year_average_interest = .055
        return np.npv(ten_year_average_interest/12, [monthly_payment]*30*12)

    value_can_afford = {k: value_can_afford(v) for k, v in
                        monthly_affordable_payment.to_dict().items()}
    value_can_afford = pd.Series(value_can_afford)

    # account for interest and property taxes
    interest_and_prop_taxes = .013
    value_can_afford /= 1+interest_and_prop_taxes

    # there's a lot more nuance to inclusionary percentages than this -
    # e.g. specific neighborhoods get specific amounts -
    # http://sf-moh.org/modules/showdocument.aspx?documentid=7253

    pct_inclusionary = orca.get_injectable("inclusionary_housing_settings")
    juris_name = parcels_geography.juris_name.loc[feasibility.index]
    pct_affordable = juris_name.map(pct_inclusionary).fillna(0)
    value_can_afford = juris_name.map(value_can_afford)

    num_affordable_units = (units * pct_affordable).fillna(0).astype("int")

    ave_price_per_unit = \
        feasibility[('residential', 'building_revenue')] / units

    revenue_diff_per_unit = (ave_price_per_unit - value_can_afford).fillna(0)
    print("Revenue difference per unit (not zero values)")
    print(revenue_diff_per_unit[revenue_diff_per_unit > 0].describe())

    revenue_reduction = revenue_diff_per_unit * num_affordable_units

    s = num_affordable_units.groupby(parcels_geography.juris_name).sum()
    print("Feasibile affordable units by jurisdiction")
    print(s[s > 0].sort_values())

    return revenue_reduction, num_affordable_units


# this adds fees to the max_profit column of the feasibility dataframe
# fees are usually spatially specified and are per unit so that calculation
# is done here as well
def policy_modifications_of_profit(feasibility, parcels):

    print("Making policy modifications to profitability")

    # this first section adds parcel unit-based fees

    units = feasibility[('residential', 'residential_sqft')] / \
        parcels.ave_sqft_per_unit
    fees = (units * parcels.fees_per_unit).fillna(0)
    print("Sum of residential fees: ", fees.sum())

    feasibility[("residential", "fees")] = fees
    feasibility[("residential", "max_profit")] -= fees

    #  now non residential fees per sqft
    for use in ["retail", "office"]:

        if (use, 'non_residential_sqft') not in feasibility.columns:
            continue

        sqft = feasibility[(use, 'non_residential_sqft')]
        fees = (sqft * parcels.fees_per_sqft).fillna(0)
        print("Sum of non-residential fees (%s): %.0f" % (use, fees.sum()))

        feasibility[(use, "fees")] = fees
        feasibility[(use, "max_profit")] -= fees

    # this section adds inclusionary housing reduction in revenue
    revenue_reduction, num_affordable_units = \
        inclusionary_housing_revenue_reduction(feasibility, units)

    assert np.all(num_affordable_units <= units.fillna(0))

    print("Describe of inclusionary revenue reduction:\n",
          revenue_reduction[revenue_reduction > 0].describe())

    print("Describe of number of affordable units:\n",
          num_affordable_units[num_affordable_units > 0].describe())

    feasibility[("residential", "policy_based_revenue_reduction")] = \
        revenue_reduction
    feasibility[("residential", "max_profit")] -= revenue_reduction
    feasibility[("residential", "deed_restricted_units")] = \
        num_affordable_units
    feasibility[("residential", "inclusionary_units")] = \
        num_affordable_units

    policy = orca.get_injectable("policy")

    if "sb743_settings" in policy["acct_settings"]:

        sb743_settings = policy["acct_settings"]["sb743_settings"]

        if sb743_settings["enable"]:

            pct_modifications = feasibility[("residential", "vmt_res_cat")].\
                map(sb743_settings["sb743_pcts"]) + 1

            print("Modifying profit for SB743:\n",
                  pct_modifications.describe())

            feasibility[("residential", "max_profit")] *= pct_modifications

    if "land_value_tax_settings" in policy["acct_settings"]:

        s = policy["acct_settings"]["land_value_tax_settings"]

        if orca.get_injectable("scenario") in s["enable_in_scenarios"]:

            bins = s["bins"]
            pcts = bins["pcts"]
            # need to boud the breaks with a reasonable low and high goalpost
            breaks = [-1]+bins["breaks"]+[2]

            pzc = orca.get_table("parcels_zoning_calculations")
            s = pzc.zoned_build_ratio
            # map the breakpoints defined in yaml to the pcts defined there
            pct_modifications = pd.cut(s, breaks, labels=pcts).\
                astype('float') + 1
            # if some parcels got skipped, fill them in with no modification
            pct_modifications = \
                pct_modifications.reindex(pzc.index).fillna(1.0)

            print("Modifying profit for Land Value Tax:\n",
                  pct_modifications.describe())

            feasibility[("residential", "max_profit")] *= pct_modifications

    if "profitability_adjustment_policies" in policy["acct_settings"]:

        for key, policy in \
                policy["acct_settings"][
                    "profitability_adjustment_policies"].items():

            if orca.get_injectable("scenario") in \
                    policy["enable_in_scenarios"]:

                parcels_geography = orca.get_table("parcels_geography")

                if "alternate_geography_scenarios" in policy and \
                        orca.get_injectable("scenario") in \
                        policy["alternate_geography_scenarios"]:
                    formula = policy["alternate_adjustment_formula"]
                else:
                    formula = policy["profitability_adjustment_formula"]

                pct_modifications = \
                    parcels_geography.local.eval(formula)
                pct_modifications += 1.0

                print("Modifying profit for %s:\n" % policy["name"],
                      pct_modifications.describe())

                feasibility[("residential", "max_profit")] *= pct_modifications

    print("There are %d affordable units if all feasible projects are built" %
          feasibility[("residential", "deed_restricted_units")].sum())

    return feasibility


@orca.step()
def calculate_vmt_fees(policy, year, buildings, vmt_fee_categories, coffer,
                       summary, years_per_iter, scenario):

    vmt_settings = policy["acct_settings"]["vmt_settings"]

    # this is the frame that knows which devs are subsidized
    df = summary.parcel_output

    df = df.query("%d <= year_built < %d and subsidized != True" %
                  (year, year + years_per_iter))

    if not len(df):
        return

    print("%d projects pass the vmt filter" % len(df))

    total_fees = 0

    if scenario in vmt_settings["res_for_res_scenarios"]:

        df["res_for_res_fees"] = df.vmt_res_cat.map(
            vmt_settings["res_for_res_fee_amounts"])
        total_fees += (df.res_for_res_fees * df.residential_units).sum()
        print("Applying vmt fees to %d units" % df.residential_units.sum())

    if scenario in vmt_settings["com_for_res_scenarios"]:

        if scenario in vmt_settings["alternate_geography_scenarios"]:
            df["com_for_res_fees"] = df.vmt_res_cat.map(
                vmt_settings["alternate_com_for_res_fee_amounts"])
        else:
            df["com_for_res_fees"] = df.vmt_res_cat.map(
                vmt_settings["com_for_res_fee_amounts"])
        total_fees += (df.com_for_res_fees * df.non_residential_sqft).sum()
        print("Applying vmt fees to %d commerical sqft" %
              df.non_residential_sqft.sum())

    print("Adding total vmt fees for res amount of $%.2f" % total_fees)

    metadata = {
        "description": "VMT development fees",
        "year": year
    }
    # the subaccount is meaningless here (it's a regional account) -
    # but the subaccount number is referred to below
    coffer["vmt_res_acct"].add_transaction(total_fees, subaccount=1,
                                           metadata=metadata)

    total_fees = 0

    if scenario in vmt_settings["com_for_com_scenarios"]:

        if scenario in vmt_settings["alternate_geography_scenarios"]:
            df["com_for_com_fees"] = df.vmt_res_cat.map(
                vmt_settings["alternate_com_for_com_fee_amounts"])
        else:
            df["com_for_com_fees"] = df.vmt_res_cat.map(
                vmt_settings["com_for_com_fee_amounts"])
        total_fees += (df.com_for_com_fees * df.non_residential_sqft).sum()
        print("Applying vmt fees to %d commerical sqft" %
              df.non_residential_sqft.sum())

    print("Adding total vmt fees for com amount of $%.2f" % total_fees)

    coffer["vmt_com_acct"].add_transaction(total_fees, subaccount="regional",
                                           metadata=metadata)


@orca.step()
def subsidized_office_developer(feasibility, coffer, acct_settings, year,
                                add_extra_columns_func, buildings, summary):

    # get the total subsidy for subsidizing office
    total_subsidy = coffer["vmt_com_acct"].\
        total_transactions_by_subacct("regional")

    # get the office feasibility frame and sort by profit per sqft
    feasibility = feasibility.to_frame().loc[:, "office"]
    feasibility = feasibility.dropna(subset=["max_profit"])

    feasibility["pda_id"] = feasibility.pda

    if "alternate_geography_scenarios" in acct_settings["vmt_settings"] \
            and orca.get_injectable("scenario") in \
            acct_settings["vmt_settings"]["alternate_geography_scenarios"]:
        formula = acct_settings["vmt_settings"]["alternate_buildings_filter"]
    else:
        formula = acct_settings["vmt_settings"]["receiving_buildings_filter"]

    # filter to receiving zone
    feasibility = feasibility.\
        query(formula)

    feasibility["non_residential_sqft"] = \
        feasibility.non_residential_sqft.astype("int")

    feasibility["max_profit_per_sqft"] = feasibility.max_profit / \
        feasibility.non_residential_sqft

    # sorting by our choice metric as we're going to pick our devs
    # in order off the top
    feasibility = feasibility.sort_values(['max_profit_per_sqft'])

    # make parcel_id available
    feasibility = feasibility.reset_index()

    print("%.0f subsidy with %d developments to choose from" % (
        total_subsidy, len(feasibility)))

    devs = []

    for dev_id, d in feasibility.iterrows():

        # the logic here is that we allow each dev to have a "normal"
        # profit per square foot and once it does we guarantee that it
        # gets build - we assume the planning commission for each city
        # enables this to happen.  If a project gets enough profit already
        # we just allow it to compete on the open market - e.g. in the
        # non-subsidized office developer

        NORMAL_PROFIT_PER_SQFT = 70  # assume price is around $700/sqft

        if d.max_profit_per_sqft >= NORMAL_PROFIT_PER_SQFT:
            #  competes in open market
            continue
        else:
            amt = (NORMAL_PROFIT_PER_SQFT - d.max_profit_per_sqft) * \
                d.non_residential_sqft

            if amt > total_subsidy:
                # we don't have enough money to buy yet
                continue

        metadata = {
            "description": "Developing subsidized office building",
            "year": year,
            "non_residential_sqft": d["non_residential_sqft"],
            "index": dev_id
        }

        coffer["vmt_com_acct"].add_transaction(-1*amt, subaccount="regional",
                                               metadata=metadata)

        total_subsidy -= amt

        devs.append(d)

    if len(devs) == 0:
        return

    # record keeping - add extra columns to match building dataframe
    # add the buidings and demolish old buildings, and add to debug output
    devs = pd.DataFrame(devs, columns=feasibility.columns)

    print("Building {:,} subsidized office sqft in {:,} projects".format(
        devs.non_residential_sqft.sum(), len(devs)))

    devs["form"] = "office"
    devs = add_extra_columns_func(devs)

    add_buildings(buildings, devs)

    summary.add_parcel_output(devs)


def run_subsidized_developer(feasibility, parcels, buildings, households,
                             acct_settings, settings, account, year,
                             form_to_btype_func, add_extra_columns_func,
                             summary, create_deed_restricted=False,
                             policy_name="Unnamed"):
    """
    The subsidized residential developer model.

    Parameters
    ----------
    feasibility : DataFrame
        A DataFrame that is returned from run_feasibility for a given form
    parcels : DataFrameWrapper
        The standard parcels DataFrameWrapper (mostly just for run_developer)
    buildings : DataFrameWrapper
        The standard buildings DataFrameWrapper (passed to run_developer)
    households : DataFrameWrapper
        The households DataFrameWrapper (passed to run_developer)
    acct_settings : Dict
        A dictionary of settings to parameterize the model.  Needs these keys:
        sending_buildings_subaccount_def - maps buildings to subaccounts
        receiving_buildings_filter - filter for eligible buildings
    settings : Dict
        The overall settings
    account : Account
        The Account object to use for subsidization
    year : int
        The current simulation year (will be added as metadata)
    form_to_btype_func : function
        Passed through to run_developer
    add_extra_columns_func : function
        Passed through to run_developer
    summary : Summary
        Used to add parcel summary information
    create_deed_restricted : bool
        Bool for whether to create deed restricted units with the subsidies
        or not.  The logic at the time of this writing is to keep track of
        partial units so that when partial units sum to greater than a unit,
        that unit will be deed restricted.

    Returns
    -------
    Nothing

    Subsidized residential developer is designed to run before the normal
    residential developer - it will prioritize the developments we're
    subsidizing (although this is not strictly required - running this model
    after the market rate developer will just create a temporarily larger
    supply of units, which will probably create less market rate development in
    the next simulated year)
    the steps for subsidizing are essentially these

    1 run feasibility with only_built set to false so that the feasibility of
        unprofitable units are recorded
    2 temporarily filter to ONLY unprofitable units to check for possible
        subsidized units (normal developer takes care of market-rate units)
    3 compute the number of units in these developments
    4 divide cost by number of units in order to get the subsidy per unit
    5 filter developments to parcels in "receiving zone" similar to the way we
        identified "sending zones"
    6 iterate through subaccounts one at a time as subsidy will be limited
        to available funds in the subaccount (usually by jurisdiction)
    7 sort ascending by subsidy per unit so that we minimize subsidy (but total
        subsidy is equivalent to total building cost)
    8 cumsum the total subsidy in the buildings and locate the development
        where the subsidy is less than or equal to the amount in the account -
        filter to only those buildings (these will likely be built)
    9 pass the results as "feasible" to run_developer - this is sort of a
        boundary case of developer but should run OK
    10 for those developments that get built, make sure to subtract from
        account and keep a record (on the off chance that demand is less than
        the subsidized units, run through the standard code path, although it's
        very unlikely that there would be more subsidized housing than demand)
    """
    # step 2
    feasibility = feasibility.replace([np.inf, -np.inf], np.nan)
    feasibility = feasibility[feasibility.max_profit < 0]

    # step 3
    feasibility['ave_sqft_per_unit'] = parcels.ave_sqft_per_unit
    feasibility['residential_units'] = \
        np.floor(feasibility.residential_sqft / feasibility.ave_sqft_per_unit)

    # step 3B
    # can only add units - don't subtract units - this is an approximation
    # of the calculation that will be used to do this in the developer model
    feasibility = feasibility[
        feasibility.residential_units > feasibility.total_residential_units]

    # step 3C
    # towards the end, because we're about to sort by subsidy per unit, some
    # large projects never get built, because it could be a 100 unit project
    # times a 500k subsidy per unit.  thus we're going to try filtering by
    # the maximum subsidy for a single development here
    feasibility = feasibility[feasibility.max_profit > -50*1000000]

    # step 4
    feasibility['subsidy_per_unit'] = \
        -1 * feasibility['max_profit'] / feasibility['residential_units']
    # assumption that even if the developer says this property is almost
    # profitable, even the administration costs are likely to cost at least
    # 10k / unit
    feasibility['subsidy_per_unit'] = feasibility.subsidy_per_unit.clip(10000)

    # step 5
    if "alternate_buildings_filter" in acct_settings and \
            orca.get_injectable("scenario") in \
            acct_settings["alternate_geography_scenarios"]:
        feasibility = feasibility.\
            query(acct_settings["alternate_buildings_filter"])
    elif "receiving_buildings_filter" in acct_settings:
        feasibility = feasibility.\
            query(acct_settings["receiving_buildings_filter"])
    else:
        # otherwise all buildings are valid
        pass

    new_buildings_list = []
    sending_bldgs = acct_settings["sending_buildings_subaccount_def"]
    feasibility["regional"] = 1
    feasibility["subaccount"] = feasibility.eval(sending_bldgs)
    # step 6
    for subacct, amount in account.iter_subaccounts():
        print("Subaccount: ", subacct)

        df = feasibility[feasibility.subaccount == subacct]
        print("Number of feasible projects in receiving zone:", len(df))

        if len(df) == 0:
            continue

        # step 7
        df = df.sort_values(['subsidy_per_unit'], ascending=True)
        # df.to_csv('subsidized_units_%d_%s_%s.csv' %
        #           (orca.get_injectable("year"), account.name, subacct))

        # step 8
        print("Amount in subaccount: ${:,.2f}".format(amount))
        num_bldgs = int((-1*df.max_profit).cumsum().searchsorted(amount))

        if num_bldgs == 0:
            continue

        # technically we only build these buildings if there's demand
        # print "Building {:d} subsidized buildings".format(num_bldgs)
        df = df.iloc[:int(num_bldgs)]

        df.columns = pd.MultiIndex.from_tuples(
            [("residential", col) for col in df.columns])
        # disable stdout since developer is a bit verbose for this use case
        sys.stdout, old_stdout = StringIO(), sys.stdout

        kwargs = settings['residential_developer']
        # step 9
        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size,
            parcels.ave_sqft_per_unit,
            parcels.total_residential_units,
            orca.DataFrameWrapper("feasibility", df),
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            profit_to_prob_func=profit_to_prob_func,
            **kwargs)
        sys.stdout = old_stdout
        buildings = orca.get_table("buildings")

        if new_buildings is None:
            continue

        # keep track of partial subsidized untis so that we always get credit
        # for a partial unit, even if it's not built in this specific building
        partial_subsidized_units = 0

        # step 10
        for index, new_building in new_buildings.iterrows():

            amt = new_building.max_profit
            metadata = {
                "description": "Developing subsidized building",
                "year": year,
                "residential_units": new_building.residential_units,
                "building_id": index
            }
            account.add_transaction(amt, subaccount=subacct,
                                    metadata=metadata)

            if create_deed_restricted:

                revenue_per_unit = new_building.building_revenue / \
                    new_building.residential_units
                total_subsidy = abs(new_building.max_profit)
                subsidized_units = total_subsidy / revenue_per_unit + \
                    partial_subsidized_units
                # right now there are inclusionary requirements
                already_subsidized_units = new_building.deed_restricted_units

                # get remainder
                partial_subsidized_units = subsidized_units % 1
                # round off for now
                subsidized_units = int(subsidized_units) + \
                    already_subsidized_units
                # cap at number of residential units
                subsidized_units = min(subsidized_units,
                                       new_building.residential_units)

                buildings.local.loc[index, "deed_restricted_units"] =\
                    int(round(subsidized_units))

                # also correct the debug output
                new_buildings.loc[index, "deed_restricted_units"] =\
                    int(round(subsidized_units))

        assert np.all(buildings.local.deed_restricted_units.fillna(0) <=
                      buildings.local.residential_units.fillna(0))

        print("Amount left after subsidy: ${:,.2f}".
              format(account.total_transactions_by_subacct(subacct)))

        new_buildings_list.append(new_buildings)

    total_len = reduce(lambda x, y: x+len(y), new_buildings_list, 0)
    if total_len == 0:
        print("No subsidized buildings")
        return

    new_buildings = pd.concat(new_buildings_list)
    print("Built {} total subsidized buildings".format(len(new_buildings)))
    print("    Total subsidy: ${:,.2f}".
          format(-1*new_buildings.max_profit.sum()))
    print("    Total subsidized units: {:.0f}".
          format(new_buildings.residential_units.sum()))

    new_buildings["subsidized"] = True
    new_buildings["policy_name"] = policy_name

    summary.add_parcel_output(new_buildings)


@orca.step()
def subsidized_residential_feasibility(
        parcels, settings,
        add_extra_columns_func, parcel_sales_price_sqft_func,
        parcel_is_allowed_func, parcels_geography):

    kwargs = settings['feasibility'].copy()
    kwargs["only_built"] = False
    kwargs["forms_to_test"] = ["residential"]

    config = sqftproforma.SqFtProFormaConfig()
    # use the cap rate from settings.yaml
    config.cap_rate = settings["cap_rate"]

    # step 1
    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=config,
                          **kwargs)

    feasibility = orca.get_table("feasibility").to_frame()
    # get rid of the multiindex that comes back from feasibility
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)
    # join to parcels_geography for filtering
    feasibility = feasibility.join(parcels_geography.to_frame(),
                                   rsuffix='_y')

    # add the multiindex back
    feasibility.columns = pd.MultiIndex.from_tuples(
            [("residential", col) for col in feasibility.columns])

    feasibility = policy_modifications_of_profit(feasibility, parcels)

    orca.add_table("feasibility", feasibility)

    df = orca.get_table("feasibility").to_frame()
    df = df.stack(level=0).reset_index(level=1, drop=True)
    # this uses a surprising amount of disk space, don't write out for now
    # df.to_csv("runs/run{}_feasibility_{}.csv".format(
    #    orca.get_injectable("run_number"),
    #    orca.get_injectable("year")))


@orca.step()
def subsidized_residential_developer_vmt(
        households, buildings, add_extra_columns_func,
        parcels_geography, year, acct_settings, parcels,
        settings, summary, coffer, form_to_btype_func, feasibility):

    feasibility = feasibility.to_frame()
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)

    run_subsidized_developer(feasibility,
                             parcels,
                             buildings,
                             households,
                             acct_settings["vmt_settings"],
                             settings,
                             coffer["vmt_res_acct"],
                             year,
                             form_to_btype_func,
                             add_extra_columns_func,
                             summary,
                             create_deed_restricted=True,
                             policy_name="VMT")


@orca.step()
def subsidized_residential_developer_lump_sum_accts(
        households, buildings, add_extra_columns_func,
        parcels_geography, year, acct_settings, parcels,
        policy, summary, coffer, form_to_btype_func,
        scenario, settings):

    for key, acct in policy["acct_settings"]["lump_sum_accounts"].items():

        # quick return in order to save performance time
        if scenario not in acct["enable_in_scenarios"]:
            continue

        print("Running the subsidized developer for acct: %s" % acct["name"])

        # need to rerun the subsidized feasibility every time and get new
        # results - this is not ideal and is a story to fix in pivotal, but the
        # only cost is in time - the results should be the same
        orca.eval_step("subsidized_residential_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()
        feasibility = feasibility.stack(level=0).\
            reset_index(level=1, drop=True)

        run_subsidized_developer(feasibility,
                                 parcels,
                                 buildings,
                                 households,
                                 acct,
                                 settings,
                                 coffer[acct["name"]],
                                 year,
                                 form_to_btype_func,
                                 add_extra_columns_func,
                                 summary,
                                 create_deed_restricted=acct[
                                    "subsidize_affordable"],
                                 policy_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())

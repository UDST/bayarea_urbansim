from urbansim.utils import misc
import os
import sys
import orca
import datasources
import variables
from urbansim import accounts
from urbansim.developer import sqftproforma
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
import pandas as pd
from cStringIO import StringIO


@orca.step('rsh_simulate')
def rsh_simulate(residential_units, unit_aggregations):
    return utils.hedonic_simulate("rsh.yaml", residential_units, 
    								unit_aggregations, "unit_residential_price")

# creating a residential rental hedonic
@orca.step('rrh_estimate')
def rh_cl_estimate(craigslist, aggregations):
    return utils.hedonic_estimate("rrh.yaml", craigslist, aggregations)


@orca.step('rrh_simulate')
def rh_cl_simulate(residential_units, unit_aggregations):
    return utils.hedonic_simulate("rrh.yaml", residential_units, 
    								unit_aggregations, "unit_residential_rent")


# This augments urbansim_defaults/utils.simple_relocation()
def relocation_with_filters(choosers, relocation_rates, fieldname):
    """
    Run a rate-based relocation model with filters

    Parameters
    ----------
    households : DataFrameWrapper or DataFrame
        Table of households that might relocate
    relocation_rates : dictionary
        Agent filters and associated rates of relocation, in the format
        {key: {'rate': real, 'filter': str}, ...}
    fieldname : str
        The field name in the resulting dataframe to set to -1 (to unplace
        new agents)

    Returns
    -------
    Nothing
    """
    print "Rate-based relocation model with filters"
    df = choosers.to_frame()
    
    for key in relocation_rates:
        rate = relocation_rates[key]['rate']
        print "Category: %s" % key
        print "Relocation rate: %s" % rate
        
        choosers_subset = df.query(relocation_rates[key]['filter'])
        
        print "Total agents: %d" % len(choosers_subset)
        print "Total currently unplaced: %d" % \
              choosers_subset[fieldname].value_counts().get(-1, 0)
        
        print "Assigning for relocation..."
        chooser_ids = np.random.choice(choosers_subset.index, size=int(rate *
                                       len(choosers_subset)), replace=False)
        choosers.update_col_from_series(fieldname,
                                        pd.Series(-1, index=chooser_ids))
        
        # this count is not strictly correct because some agents that were already 
        # unplaced will also be assigned for relocation 
        print "Additional unplaced: %d" % len(chooser_ids)


# This can be run as an alternative to the households_relocation model
@orca.step('households_relocation_filtered')
def households_relocation_filtered(households, settings):
    rate = settings['rates']['households_relocation_filtered']
    return relocation_with_filters(households, rate, "unit_id")


# Overriding the urbansim_defaults households_relocation in order to do deed
# restrictions.  This is a rather minor point - we just need to null out
# relocating unit ids rather than building ids
@orca.step('households_relocation')
def households_relocation(households, settings):
    rate = settings['rates']['households_relocation']
    return utils.simple_relocation(households, rate, "unit_id")


# Overriding the urbansim_defaults households_transition in order to do deed
# restrictions.  This is a rather minor point - we just need to add empty
# unit ids rather than building ids at the end of the process
@orca.step('households_transition')
def households_transition(households, household_controls, year, settings):
    return utils.full_transition(households,
                                 household_controls,
                                 year,
                                 settings['households_transition'],
                                 "unit_id")


# Overriding the urbansim_defaults hlcm_simulation in order to do deed
# restrictions.  This is the first unit-based hlcm.
@orca.step('hlcm_simulate')
def hlcm_simulate(households, residential_units, settings):
    # this actually triggers adding the unit_id to households
    _ = orca.get_table('residential_units')
    # because the households have been refreshed, need to get them again
    households = orca.get_table("households")
    # for this hlcm, we need to add the buildings to the set of merge tables
    aggregations = [orca.get_table(tbl) for tbl in \
        ["buildings", "nodes", "logsums"]]
    return utils.lcm_simulate("hlcm.yaml", households, residential_units,
                              aggregations,
                              "unit_id",
                              "num_units",
                              "vacant_units",
                              settings.get("enable_supply_correction", None))


# this is the low income hlcm - which allows the choice of
# deed restricted units
@orca.step('hlcm_li_simulate')
def hlcm_li_simulate(households, residential_units, settings):
    aggregations = [orca.get_table(tbl) for tbl in \
        ["buildings", "nodes", "logsums"]]
    # note supply correction is turned off since that's inappropraite
    # for subsidized housing
    return utils.lcm_simulate("hlcm_li.yaml", households, residential_units,
                              aggregations,
                              "unit_id",
                              "num_units",
                              "vacant_units")


# adding HLCM's for owners vs renters
@orca.step('hlcm_owner_estimate')
def hlcm_owner_estimate(households, residential_units, unit_aggregations):
    return utils.lcm_estimate("hlcm_owner.yaml", households, "unit_id",
                              residential_units, unit_aggregations)


@orca.step('hlcm_owner_simulate')
def hlcm_owner_simulate(households, residential_units, unit_aggregations, settings):
    return utils.lcm_simulate("hlcm_owner.yaml", households, residential_units,
                              unit_aggregations, "unit_id", "num_units", "vacant_units",
                              settings.get("enable_supply_correction", None))


@orca.step('hlcm_renter_estimate')
def hlcm_renter_estimate(households, residential_units, unit_aggregations):
    return utils.lcm_estimate("hlcm_renter.yaml", households, "unit_id",
                              residential_units, unit_aggregations)


@orca.step('hlcm_renter_simulate')
def hlcm_renter_simulate(households, residential_units, unit_aggregations, settings):
    return utils.lcm_simulate("hlcm_renter.yaml", households, residential_units,
                              unit_aggregations, "unit_id", "num_units", "vacant_units",
                              settings.get("enable_supply_correction", None))


# translate residential rental/ownership income into consistent terms so the two forms 
# can compete against each other in the developer model
@orca.step('price_to_rent_precompute')
def cap_rate_precompute(nodes):
    # this cap rate assumption comes from the empirical ratio of smoothed, fitted 
    # residential rents and prices in the base year
    cap_rate = 0.063
    nodes = orca.get_table('nodes')
    df = nodes.to_frame(nodes.local_columns)
    # output requirements:
    # - needs to be yearly equivalent rent because of how the feasibility model works
    # - (can no longer use the residential_to_yearly conversion option in 
    #   utils.run_feasibility() because it assumes a single residential form type)
    # - thus, need to change settings.feasibility.residential_to_yearly to False
    #   in the settings.yaml file
    # - names of the final columns need to match names of the use types, because of
    #   how the parcel_sales_price_sqft_func() callback is written (see variables.py)
    df['residential_ownerocc'] = df.residential_price.multiply(cap_rate)
    df['residential_rented'] = df.residential_rent.multiply(12)
    orca.add_table('nodes', df)


# override to separate residential use type into owner-occupied vs rented
# - kwargs settings come from the yaml file and are used by utils.run_feasibility()
# - pfc settings are used by the eventual SqFtProForma() class, and were not previously
#   customized in the baseline version of bayarea_urbansim
@orca.step('feasibility')
def feasibility(parcels, settings,
                parcel_sales_price_sqft_func,
                parcel_is_allowed_func):
    
    # options are documented in urbansim.developer.sqftproforma.SqFtProFormaConfig()
    # - all we're doing is splitting residential use type in two, extending building
    #   form types as necessary, and re-applying standard residential characteristics
    # - also requires amendments to settings.form_to_btype in the yaml file
    
    pfc = sqftproforma.SqFtProFormaConfig()
    pfc.uses = ['retail', 'industrial', 'office', \
    			'residential_ownerocc', 'residential_rented']
    pfc.residential_uses = [False, False, False, True, True]
    pfc.forms = {
            'retail': {
            	"retail": 1.0 
            },
            'industrial': {
            	"industrial": 1.0 
            },
            'office': {
            	"office": 1.0 
            },
            'residential_ownerocc': {
            	"residential_ownerocc": 1.0 
            },
            'residential_rented': {
            	"residential_rented": 1.0 
            },
            'mixedresidential_ownerocc': {
            	"retail": 0.1, 
            	"residential_ownerocc": 0.9 
            },
            'mixedresidential_rented': {
            	"retail": 0.1, 
            	"residential_rented": 0.9
            },
            'mixedoffice_ownerocc': {
            	"office": 0.7, 
            	"residential_ownerocc": 0.3
            },
            'mixedoffice_rented': {
            	"office": 0.7, 
            	"residential_rented": 0.3
            }
        }
            
    pfc.parking_rates = {"retail": 2.0, "industrial": .6, "office": 1.0, \
            "residential_ownerocc": 1.0, "residential_rented": 1.0}
            
    pfc.costs['residential_ownerocc'] = pfc.costs['residential']
    pfc.costs['residential_rented'] = pfc.costs['residential']
    del pfc.costs['residential']
    pfc.heights_for_costs = [15, 55, 120, np.inf, np.inf]

    # the price and permissibility callbacks are defined in variables.py
    kwargs = settings['feasibility']
    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=pfc,
                          **kwargs)


# override to divide building forms by tenure
@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func):
    kwargs = settings['residential_developer']
    new_buildings = utils.run_developer(
        ['residential_ownerocc', 'residential_rented'],
        households,
        buildings,
        'residential_units',
        parcels.parcel_size,
        parcels.ave_sqft_per_unit,
        parcels.total_residential_units,
        feasibility,
        year=year,
        form_to_btype_callback=form_to_btype_func,
        add_more_columns_callback=add_extra_columns_func,
        **kwargs)
    
    summary.add_parcel_output(new_buildings)
    
    
@orca.injectable("supply_and_demand_multiplier_func", autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
    s = demand / supply
    settings = orca.get_injectable('settings')
    print "Number of submarkets where demand exceeds supply:", len(s[s>1.0])
    #print "Raw relationship of supply and demand\n", s.describe()
    supply_correction = settings["enable_supply_correction"]
    clip_change_high = supply_correction["kwargs"]["clip_change_high"]
    t = s
    t -= 1.0
    t = t / t.max() * (clip_change_high-1)
    t += 1.0
    s.loc[s > 1.0] = t.loc[s > 1.0]
    #print "Shifters for current iteration\n", s.describe()
    return s, (s <= 1.0).all()


# FIX THIS
# this is the function for mapping a specific building that we build to a
# specific building type
@orca.injectable("form_to_btype_func", autocall=False)
def form_to_btype_func(building):
    settings = orca.get_injectable('settings')
    form = building.form
    dua = building.residential_units / (building.parcel_size / 43560.0)
    # precise mapping of form to building type for residential
    if form is None or form == "residential":
        if dua < 16:
            return 1
        elif dua < 32:
            return 2
        return 3
    return settings["form_to_btype"][form][0]


@orca.injectable("coffer", cache=True)
def coffer():
    return {
        "prop_tax_acct": accounts.Account("prop_tax_act")
    }


@orca.injectable("acct_settings", cache=True)
def acct_settings(settings):
    return settings["acct_settings"]


def tax_buildings(buildings, acct_settings, account, year):
    """
    Tax buildings and add the tax to an Account object by subaccount

    Parameters
    ----------
    buildings : DataFrameWrapper
        The standard buildings DataFrameWrapper
    acct_settings : Dict
        A dictionary of settings to parameterize the model.  Needs these keys:
        sending_buildings_subaccount_def - maps buildings to subaccounts
        sending_buildings_filter - filter for eligible buildings
        sending_buildings_tax - a Pandas eval expression to compute the tax
    accounts : Account
        The Account object to use for subsidization
    year : int
        The current simulation year (will be added as metadata)

    Returns
    -------
    Nothing
    """
    buildings = buildings.query(acct_settings["sending_buildings_filter"])

    tax = buildings.eval(acct_settings["sending_buildings_tax"])

    subaccounts = buildings[acct_settings["sending_buildings_subaccount_def"]]

    tot_tax_by_subaccount = tax.groupby(subaccounts).sum()

    for subacct, amt in tot_tax_by_subaccount.iteritems():
        metadata={
            "description": "Collecting property tax",
            "year": year
        }
        account.add_transaction(amt, subaccount=subacct,
                                                metadata=metadata)

    print "Sample rows from property tax accts:"
    print account.to_frame().\
        sort(columns=["amount"], ascending=False).head(7)


@orca.step("calc_prop_taxes")
def property_taxes(buildings, parcels_geography, acct_settings, coffer, year):
    buildings = sim.merge_tables('buildings', [buildings, parcels_geography])
    tax_buildings(buildings, acct_settings, coffer["prop_tax_acct"], year)


@orca.injectable("add_extra_columns_func", autocall=False)
def add_extra_columns(df):
    for col in ["residential_price", "non_residential_price"]:
        df[col] = 0
    df["redfin_sale_year"] = 2012
    return df


def run_subsidized_developer(feasibility, parcels, buildings, households,
                             acct_settings, settings, account, year,
                             form_to_btype_func, add_extra_columns_func,
                             summary):
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
    accounts : Account
        The Account object to use for subsidization
    year : int
        The current simulation year (will be added as metadata)
    form_to_btype_func : function
        Passed through to run_developer
    add_extra_columns_func : function
        Passed through to run_developer
    summary : Summary
        Used to add parcel summary information

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
    10 for those developments that get built, make sure to subtract from account
        and keep a record (on the off chance that demand is less than the
        subsidized units, run through the standard code path, although it's
        very unlikely that there would be more subsidized housing than demand)
    """
    # step 2
    # feasibility = feasibility.replace([np.inf, -np.inf], np.nan)
    feasibility = feasibility[feasibility.max_profit < 0]

    # step 3
    feasibility['ave_sqft_per_unit'] = parcels.ave_sqft_per_unit
    feasibility['residential_units'] = np.floor(feasibility.residential_sqft /
        feasibility.ave_sqft_per_unit).replace(0, 1)

    # step 4
    feasibility['subsidy_per_unit'] = \
        feasibility['max_profit'] / feasibility['residential_units']

    # step 5
    if "receiving_buildings_filter" in acct_settings:
        feasibility = feasibility.\
            query(acct_settings["receiving_buildings_filter"])
    else:
        # otherwise all buildings are valid
        pass

    new_buildings_list = []
    sending_bldgs = acct_settings["sending_buildings_subaccount_def"]
    feasibility["subaccount"] = feasibility.eval(sending_bldgs)
    # step 6
    for subacct, amount in account.iter_subaccounts():
        print "Subaccount: ", subacct
        df = feasibility[feasibility.subaccount == subacct]
        if len(df) == 0:
            continue

        # step 7
        df = df.sort(columns=['subsidy_per_unit'], ascending=False)

        # step 8
        print "Amount in subaccount: ${:,.2f}".format(amount)
        num_bldgs = int((-1*df.max_profit).cumsum().searchsorted(amount))

        if num_bldgs == 0:
            continue

        print "Building {:d} subsidized buildings".format(num_bldgs)
        df = df.iloc[:int(num_bldgs)]

        # disable stdout since developer is a bit verbose for this use case
        sys.stdout, old_stdout = StringIO(), sys.stdout

        kwargs = settings['residential_developer']
        # step 9
        new_buildings = utils.run_developer(
            None,
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
            **kwargs)
        sys.stdout = old_stdout
        buildings = orca.get_table("buildings")

        if new_buildings is None:
            continue

        # step 10
        for index, new_building in new_buildings.iterrows():
            amt = new_building.max_profit
            metadata={
                "description": "Developing subsidized building",
                "year": year,
                "residential_units": new_building.residential_units,
                "building_id": index
            }
            account.add_transaction(amt, subaccount=subacct,
                                    metadata=metadata)
        print "Amount left after subsidy: ${:,.2f}".\
            format(account.total_transactions_by_subacct(subacct))

        new_buildings_list.append(new_buildings)

    new_buildings = pd.concat(new_buildings_list)
    print "Built {} total subsidized buildings".format(len(new_buildings))
    print "    Total subsidy: ${:,.2f}".format(-1*new_buildings.max_profit.sum())
    print "    Total subsidzed units: {:.0f}".\
        format(new_buildings.residential_units.sum())
    new_buildings["subsidized"] = True
    summary.add_parcel_output(new_buildings)


@orca.step('subsidized_residential_developer')
def subsidized_residential_developer(households, buildings,
                          parcels, parcels_geography, year, acct_settings,
                          settings, summary, coffer, form_to_btype_func,
                          add_extra_columns_func, parcel_sales_price_sqft_func,
                          parcel_is_allowed_func):
    
    if "disable" in acct_settings and acct_settings["disable"] == True:
        # allow disabling model from settings rather than
        # having to remove the model name from the model list
        return

    kwargs = settings['feasibility'].copy()
    kwargs["only_built"] = False
    kwargs["forms_to_test"] = ["residential"]
    # step 1
    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          **kwargs)
    feasibility = orca.get_table("feasibility").to_frame()
    # get rid of the multiindex that comes back from feasibility
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)
    # join to parcels_geography for filtering
    feasibility = feasibility.join(parcels_geography.to_frame())

    run_subsidized_developer(feasibility,
                             parcels,
                             buildings,
                             households,
                             acct_settings,
                             settings,
                             coffer["prop_tax_acct"],
                             year,
                             form_to_btype_func,
                             add_extra_columns_func,
                             summary)


@orca.step("travel_model_output")
def travel_model_output(parcels, households, jobs, buildings,
                        zones, homesales, year, summary, coffer, run_number):
    households = households.to_frame()
    jobs = jobs.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()
    homesales = homesales.to_frame()

    # put this here as a custom bay area indicator
    zones['residential_sales_price_sqft'] = parcels.\
        residential_sales_price_sqft.groupby(parcels.zone_id).quantile()
    zones['residential_purchase_price_sqft'] = parcels.\
        residential_purchase_price_sqft.groupby(parcels.zone_id).quantile()
    if 'residential_price_hedonic' in buildings.columns:
        zones['residential_sales_price_hedonic'] = buildings.\
            residential_price_hedonic.\
            groupby(buildings.zone_id).quantile().\
            reindex(zones.index).fillna(0)
    else:
        zones['residential_sales_price_hedonic'] = 0

    zones['tothh'] = households.\
        groupby('zone_id').size()
    zones['hhpop'] = households.\
        groupby('zone_id').persons.sum()

    zones['sfdu'] = \
        buildings.query("building_type_id == 1 or building_type_id == 2").\
        groupby('zone_id').residential_units.sum()
    zones['mfdu'] = \
        buildings.query("building_type_id == 3 or building_type_id == 12").\
        groupby('zone_id').residential_units.sum()

    zones['hhincq1'] = households.query("income < 25000").\
        groupby('zone_id').size()
    zones['hhincq2'] = households.query("income >= 25000 and income < 45000").\
        groupby('zone_id').size()
    zones['hhincq3'] = households.query("income >= 45000 and income < 75000").\
        groupby('zone_id').size()
    zones['hhincq4'] = households.query("income >= 75000").\
        groupby('zone_id').size()

    # attempting to get at total zonal developed acres
    zones['NEWdevacres'] = \
        (buildings.query("building_sqft > 0").
         groupby('zone_id').lot_size_per_unit.sum()) / 43560

    zones['totemp'] = jobs.\
        groupby('zone_id').size()
    zones['agrempn'] = jobs.query("empsix == 'AGREMPN'").\
        groupby('zone_id').size()
    zones['mwtempn'] = jobs.query("empsix == 'MWTEMPN'").\
        groupby('zone_id').size()
    zones['retempn'] = jobs.query("empsix == 'RETEMPN'").\
        groupby('zone_id').size()
    zones['fpsempn'] = jobs.query("empsix == 'FPSEMPN'").\
        groupby('zone_id').size()
    zones['herempn'] = jobs.query("empsix == 'HEREMPN'").\
        groupby('zone_id').size()
    zones['othempn'] = jobs.query("empsix == 'OTHEMPN'").\
        groupby('zone_id').size()

    orca.add_table("travel_model_output", zones, year)

    summary.add_zone_output(zones, "travel_model_outputs", year)
    summary.write_zone_output()
    add_xy_config = {
        "xy_table": "parcels",
        "foreign_key": "parcel_id",
    	"x_col": "x",
     	"y_col": "y"
    }
    summary.write_parcel_output(add_xy=add_xy_config)
    
    subsidy_file = "runs/run{}_subsidy_summary.csv".format(run_number)
    coffer["prop_tax_acct"].to_frame().to_csv(subsidy_file)

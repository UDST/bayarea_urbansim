from urbansim.utils import misc
import os
import sys
import orca
import datasources
import variables
from urbansim import accounts
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
import pandas as pd
from cStringIO import StringIO


@orca.step('rsh_simulate')
def rsh_simulate(buildings, aggregations, settings):
    utils.hedonic_simulate("rsh.yaml", buildings, aggregations,
                           "residential_price")
    if "rsh_simulate" in settings:
        low = float(settings["rsh_simulate"]["low"])
        high = float(settings["rsh_simulate"]["high"])
        buildings.update_col("residential_price",
                             buildings.residential_price.clip(low, high))
        print "Clipped rsh_simulate produces\n", buildings.residential_price.describe()


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time
@orca.step("scheduled_development_events")
def scheduled_development_events(buildings, development_projects, development_events,
                                 summary, year, parcels, settings, parcel_id_to_geom_id,
                                 building_sqft_per_job):
    
    # first demolish
    '''
    dem = development_events.to_frame()\
        .query("year_built == %d and action == 'demolish'" % year).set_index('geom_id')
    dem = geom_id_to_parcel_id(dem, parcels).reset_index()
    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = pd.DataFrame({parcel_id: dem.parcel_ids})
    orca.add_table("buildings",
        utils._remove_developed_buildings(old_buildings, new_buildings, 
                                          unplace_agents=["households", "jobs"]))
    '''

    # then build
    dps = development_projects.to_frame().query("year_built == %d" % year)

    if len(dps) == 0:
        return

    new_buildings = utils.scheduled_development_events(buildings, dps,
                                       remove_developed_buildings=False,
                                       unplace_agents=['households', 'jobs'])
    new_buildings["form"] = new_buildings.building_type_id.map(settings['building_type_map'])\
        .str.lower()
    new_buildings["job_spaces"] = new_buildings.building_sqft / \
        new_buildings.building_type_id.fillna(-1).map(building_sqft_per_job)
    new_buildings["job_spaces"] = new_buildings.job_spaces.astype('int')
    new_buildings["geom_id"] = parcel_id_to_geom_id(new_buildings.parcel_id)
    new_buildings["SDEM"] = True

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


# this if the function for mapping a specific building that we build to a
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
    buildings = orca.merge_tables('buildings', [buildings, parcels_geography])
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
@orca.step("geographic_summary")
def pda_output(parcels, households, jobs, buildings, taz_to_superdistrict, run_number, year):
    households_df = orca.merge_tables('households',[parcels, taz_to_superdistrict, buildings, households],columns=['pda','zone_id','DISTRICT','puma5','persons','income'])
    jobs_df = orca.merge_tables('jobs',[parcels, taz_to_superdistrict, buildings, jobs],columns=['pda','DISTRICT','zone_id','empsix'])
    buildings_df = orca.merge_tables('buildings',[parcels, taz_to_superdistrict, buildings],columns=['pda','DISTRICT','building_type_id','zone_id','residential_units','building_sqft'])
    buildings_df = buildings_df.rename(columns = {'zone_id_x':'zone_id'}) #because merge_tables returns multiple zone_id_'s, but not the one we need
    geographies = ['DISTRICT','pda']
    for geography in geographies:
        summary_table = pd.pivot_table(households_df,
         values=['persons'],
         index=[geography],
         aggfunc=[np.size, np.sum])
        summary_table.columns = ['tothh','hhpop']
        summary_table['hhincq1'] = households_df.query("income < 25000").\
            groupby(geography).size()
        summary_table['hhincq2'] = households_df.query("income >= 25000 and income < 45000").\
            groupby(geography).size()
        summary_table['hhincq3'] = households_df.query("income >= 45000 and income < 75000").\
            groupby(geography).size()
        summary_table['hhincq4'] = households_df.query("income >= 75000").\
            groupby(geography).size()
        summary_table['sfdu'] = \
            buildings_df.query("building_type_id == 1 or building_type_id == 2").\
            groupby(geography).residential_units.sum()
        summary_table['mfdu'] = \
            buildings_df.query("building_type_id == 3 or building_type_id == 12").\
            groupby(geography).residential_units.sum()
        summary_table['totemp'] = jobs_df.\
            groupby(geography).size()
        summary_table['agrempn'] = jobs_df.query("empsix == 'AGREMPN'").\
            groupby(geography).size()
        summary_table['mwtempn'] = jobs_df.query("empsix == 'MWTEMPN'").\
            groupby(geography).size()
        summary_table['retempn'] = jobs_df.query("empsix == 'RETEMPN'").\
            groupby(geography).size()
        summary_table['fpsempn'] = jobs_df.query("empsix == 'FPSEMPN'").\
            groupby(geography).size()
        summary_table['herempn'] = jobs_df.query("empsix == 'HEREMPN'").\
            groupby(geography).size()
        summary_table['othempn'] = jobs_df.query("empsix == 'OTHEMPN'").\
            groupby(geography).size()
        summary_csv = "runs/run{}_{}_summaries_{}.csv".format(run_number, geography, year)
        summary_table.to_csv(summary_csv)

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
    if sys.platform != 'win32':
        summary.write_zone_output()
    add_xy_config = {
        "xy_table": "parcels",
        "foreign_key": "parcel_id",
        "x_col": "x",
        "y_col": "y"
    }
    # otherwise it loses precision
    summary.parcel_output["geom_id"] = summary.parcel_output.geom_id.astype('str')
    summary.write_parcel_output(add_xy=add_xy_config)
    
    subsidy_file = "runs/run{}_subsidy_summary.csv".format(run_number)
    coffer["prop_tax_acct"].to_frame().to_csv(subsidy_file)

    #travel model csv stuff
    travel_model_csv = "runs/run{}_taz_summaries_{}.csv".format(run_number, year)
    travel_model_output = zones
    #list of columns that we need to fill eventually for valid travel model file:
    template_columns = ['age0519','age2044','age4564','age65p','areatype','ciacre','collfte','collpte','county','district','empres','gqpop','hhlds','hsenroll','oprkcst','prkcst','resacre','sd','sftaz','shpop62p','terminal','topology','totacre','totpop','zero','zone']
    for x in template_columns: #fill those columns with NaN until we have values for them
        travel_model_output[x] = np.nan
    travel_model_output.columns = [x.upper() for x in travel_model_output.columns] #uppercase columns to match travel model template
    travel_model_output.to_csv(travel_model_csv)

import urbansim.sim.simulation as sim
from urbansim.utils import misc
import os
import datasources
# import variables
from urbansim import accounts
from urbansim_defaults import models
from urbansim_defaults import utils


@sim.injectable("supply_and_demand_multiplier_func", autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
    s = demand / supply
    settings = sim.settings
    print "Number of submarkets where demand exceeds supply:", len(s[s>1.0])
    print "Raw relationship of supply and demand\n", s.describe()
    supply_correction = settings["enable_supply_correction"]
    clip_change_high = supply_correction["kwargs"]["clip_change_high"]
    t = s
    t -= 1.0
    t = t / t.max() * (clip_change_high-1)
    t += 1.0
    s.loc[s > 1.0] = t.loc[s > 1.0]
    print "Shifters for current iteration\n", s.describe()
    return s, (s <= 1.0).all()


# this if the function for mapping a specific building that we build to a
# specific building type
@sim.injectable("form_to_btype_func", autocall=False)
def form_to_btype_func(building):
    settings = sim.settings
    form = building.form
    dua = building.residential_units / (building.parcel_size / 43560.0)
    # precise mapping of form to building type for residential
    if form == "residential":
        if dua < 16:
            return 1
        elif dua < 32:
            return 2
        return 3
    return settings["form_to_btype"][form][0]


@sim.injectable("coffer", cache=True)
def coffer():
    return {
        "prop_tax_acct": accounts.Account("prop_tax_act")
    }


@sim.injectable("acct_settings", cache=True)
def acct_settings(settings):
    return settings["acct_settings"]


@sim.model("calc_prop_taxes")
def property_taxes(buildings, parcels_geography, acct_settings, coffer, year):
    buildings = sim.merge_tables('buildings', [buildings, parcels_geography])

    buildings = buildings.query(acct_settings["sending_buildings_filter"])

    tax = buildings.eval(acct_settings["sending_buildings_tax"])

    subaccounts = buildings[acct_settings["sending_buildings_subaccount_def"]]

    tot_tax_by_subaccount = tax.groupby(subaccounts).sum()

    for subacct, amt in tot_tax_by_subaccount.iteritems():
        metadata={
            "description": "Collecting property tax",
            "year": year
        }
        coffer["prop_tax_acct"].add_transaction(amt, subaccount=subacct,
                                                metadata=metadata)

    print "Sample rows from property tax accts:"
    print coffer["prop_tax_acct"].to_frame().\
        sort(columns=["amount"], ascending=False).head(7)


@sim.model('subsidized_residential_developer')
def subsidized_residential_developer(feasibility, households, buildings,
                          parcels, parcels_geography, year, acct_settings,
                          settings, summary, coffer, form_to_btype_func):
    """
    The subsidized residential developer model.

    Subsidized residential developer is designed to run before the normal
    residential developer - it will prioritize the developments we're
    subsidizing (although maybe this should be an option of the scenario)
    the steps for subsidizing are essentially these

    1 run feasibility with only_built set to false so that the feasibility of
        unprofitable units are recorded
    2 temporarily filter to only unprofitable units to check for possible
        subsidized units (normal developer takes care of market-rate units)
    3 filter developments to parcels in "receiving zone" similar to the way we
        identified "sending zones"
    4 groupby subaccounts and iterate through subaccounts one at a time
    5 compute the number of units in these developments
    6 divide cost by number of units in order to get the subsidy per unit
    7 sort ascending by cost per unit so that we minimize subsidy (but total
        subsidy is equivalent to total building cost)
    8 cumsum the total subsidy in the buildings and locate the development
        where the subsidy is less than or equal to the amount in the account -
        filter to only those buildings
    9 pass the results as "feasible" to run_developer - this is sort of a
        boundary case of developer but should run OK
    10 for those developments that get built, make sure to subtract from account
        and keep a record (on the off chance that demand is less than the
        subsidized units, run through the standard code path, although it's
        very unlikely that there would be more subsidy than demand)
    11 finally filter feasibility to only profitable units for later use in the
        market-rate developer model
    """
    # step 1 should have already been done - set only_built to False in
    # settings.yaml under the feasibility key
    full_feasibility = sim.merge_tables('feasibility', [feasibility,
                                                        parcels_geography])
    print full_feasibility.describe()

    # step 2
    subsidized_feasibility = full_feasibility.query('max_profit < 0')
    print subsidized_feasibility.describe()

    # step 3
    if "receiving_buildings_filter" in acct_settings:
        subsidized_feasibility = subsidized_feasibility.\
            query(acct_settings["receiving_buildings_filter"])
    else:
        # otherwise all buildings are valid
        pass
    print subsidized_feasibility.describe()

    new_buildings_list = []
    # step 4
    subaccounts = subsidized_feasibility[\
        acct_settings["sending_buildings_subaccount_def"]]
    for subacct, df in subsidized_feasibility.groupby(subaccounts):
        print subacct
        print df.describe()

        # step 5
        df['residential_units'] = np.round(df.residential_sqft /
                                           df.ave_sqft_per_unit)

        # step 6
        df['subsidy_per_unit'] = df['building_cost'] / df['residential_units']
        print df.head(5)

        # step 7
        df = df.order(columns=['subsidy_per_units'], ascending=False)
        print df.head(5)

        # step 8
        amount = coffer["prop_tax_acct"].get_balance(subaccount)
        num_bldgs = df.building_cost.cumsum().searchsorted(amount)
        print "Building %d subsidized buildings" % num_bldgs
        df = df.loc[df.index.values[num_bldgs]]

        # step 9
        kwargs = settings['residential_developer']
        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size,
            parcels.ave_sqft_per_unit,
            parcels.total_residential_units,
            df,
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns,
            **kwargs)

        # step 10
        for new_building in new_buildings:
            amt = new_building.building_cost * -1
            metadata={
                "description": "Developing subsidized building",
                "year": year,
                "building_id": new_building.index
            }
            coffer["prop_tax_acct"].add_transaction(amt, subaccount=subacct,
                                                    metadata=metadata)

        new_buildings_list.append(new_buildings)

    new_buildings = pd.concat(new_building_list)
    summary.add_parcel_output(new_buildings)

    # step 11
    df = full_feasibility.query('max_profit >= 0')
    sim.add_table("feasibility", df)


@sim.model("travel_model_output")
def travel_model_output(parcels, households, jobs, buildings,
                        zones, homesales, year, summary):
    households = households.to_frame()
    jobs = jobs.to_frame()
    buildings = buildings.to_frame()
    zones = zones.to_frame()
    homesales = homesales.to_frame()

    # put this here as a custom bay area indicator
    zones['residential_sales_price_empirical'] = homesales.groupby('zone_id').\
        sale_price_flt.quantile()
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

    sim.add_table("travel_model_output", zones, year)

    summary.add_zone_output(zones, "travel_model_outputs", year)
    summary.write_zone_output()
    add_xy_config = {
        "xy_table": "parcels",
        "foreign_key": "parcel_id",
    	"x_col": "x",
     	"y_col": "y",
    	"from_epsg": 3740,
    	"to_epsg": 4326
    }
    summary.write_parcel_output(add_xy=add_xy_config)

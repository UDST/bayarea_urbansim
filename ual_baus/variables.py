import numpy as np
import pandas as pd
from urbansim.utils import misc
import orca
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# NODE VARIABLES
#####################

@orca.column('nodes', 'poverty_rate', cache=True)
def poverty_rate(nodes):
    return nodes.poor.divide(nodes.population).fillna(0)

@orca.column('nodes', 'pct_black', cache=True)
def pct_black(nodes):
    return nodes.blacks.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_hisp', cache=True)
def pct_hisp(nodes):
    return nodes.hispanics.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_asian', cache=True)
def pct_asian(nodes):
    return nodes.asians.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_white', cache=True)
def pct_white(nodes):
    return nodes.whites.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_nonwhite', cache=True)
def pct_nonwhite(nodes):
    return nodes.nonwhites.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_renters', cache=True)
def pct_renters(nodes):
    return nodes.renters.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_singles', cache=True)
def pct_singles(nodes):
    return nodes.singles.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_two_persons', cache=True)
def pct_two_persons(nodes):
    return nodes.two_persons.divide(nodes.population).fillna(0)*100

@orca.column('nodes', 'pct_three_plus', cache=True)
def pct_three_plus(nodes):
    return nodes.three_plus.divide(nodes.population).fillna(0)*100


#####################
# REDSIDENTIAL UNIT VARIABLES
#####################

@orca.column('residential_units', 'unit_annual_rent')
def unit_annual_rent(buildings, residential_units):
    sqft = misc.reindex(buildings.sqft_per_unit, residential_units.index)
    return residential_units.unit_residential_rent.multiply(sqft).multiply(12).fillna(0)


#####################
# HOUSEHOLD VARIABLES
#####################

# overriding to remove NaNs so we can use zone_id in regressions
@orca.column('households', 'zone')
def zone(households):
    return households.zone_id.fillna(-1)

@orca.column('households', 'positive_income')
def positive_income(households):
    inc = households.income
    inc[inc < 1] = 1
    return inc.fillna(1)

@orca.column('households', 'hhs1')
def hhs1(households):
    hhs1 = households.persons == 1
    return hhs1.fillna(1)

@orca.column('households', 'hhs2')
def hhs2(households):
    hhs2 = households.persons == 2
    return hhs2.fillna(1)

@orca.column('households', 'hhs3p')
def hhs3p(households):
    hhs3p = households.persons > 2
    return hhs3p.fillna(1)

@orca.column('households', 'hhsize_cat')
def hhsize_cat(households):
    hhs = households.persons
    hhs[hhs > 3] = 3
    return hhs.fillna(1)

@orca.column('households', 'hhsize_incq')
def hhsize_incq(households):
    hhsq = households.hhsize_cat*10+households.income_quartile
    return hhsq.fillna(1)

@orca.column('households', 'hh_monthly_rent')
def hh_monthly_rent(households, buildings, residential_units):
    rent = misc.reindex(residential_units.unit_residential_rent, households.unit_id)
    sqft = misc.reindex(buildings.sqft_per_unit, households.unit_id)
    return rent.multiply(sqft).fillna(0)

@orca.column('households', 'rent_burden')
def rent_burden(households):
    rb = households.hh_monthly_rent.multiply(12).divide(households.positive_income)
    return rb.replace([np.inf, -np.inf], np.nan).fillna(0)


#####################
# BUILDING VARIABLES
#####################

# now that price is on units, override default and aggregate UP to buildings
@orca.column('buildings', 'residential_price')
def residential_price(buildings, residential_units):
    return residential_units.unit_residential_price.\
        groupby(residential_units.building_id).median().\
        reindex(buildings.index).fillna(0)


@orca.column('buildings', 'residential_rent')
def residential_rent(buildings, residential_units):
    return residential_units.unit_residential_rent.\
        groupby(residential_units.building_id).median().\
        reindex(buildings.index).fillna(0)


#####################
# COSTAR VARIABLES
#####################

@orca.column('costar', 'general_type')
def general_type(costar):
    return costar.PropertyType


@orca.column('costar', 'rent')
def rent(costar):
    return costar.averageweightedrent


@orca.column('costar', 'stories')
def stories(costar):
    return costar.number_of_stories


@orca.column('costar', 'node_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)


@orca.column('costar', 'zone_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.zone_id, costar.parcel_id)


#####################
# JOBS VARIABLES
#####################

@orca.column('jobs', 'naics', cache=True)
def naics(jobs):
    return jobs.sector_id


@orca.column('jobs', 'empsix', cache=True)
def empsix(jobs, settings):
    return jobs.naics.map(settings['naics_to_empsix'])


@orca.column('jobs', 'empsix_id', cache=True)
def empsix_id(jobs, settings):
    return jobs.empsix.map(settings['empsix_name_to_id'])


#####################
# BUILDINGS VARIABLES
#####################

@orca.column('buildings', 'sqft_per_unit', cache=True)
def unit_sqft(buildings):
    return (buildings.building_sqft / buildings.residential_units.replace(0, 1)).clip(400, 6000)

#@orca.column('buildings', 'sqft_per_unit', cache=True)
#def annual_rent_per_unit(buildings):
#    return (unit_sqft * residential_rent * 12)

#####################
# PARCELS VARIABLES
#####################

# these are actually functions that take parameters, but are parcel-related
# so are defined here
@orca.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use):
    # Fletcher had some code here to switch network aggregation with zone + quantile
    # aggregation, but I'm rolling it back for simplicity  -Sam
    return misc.reindex(orca.get_table('nodes')[use],
                        orca.get_table('parcels').node_id)


@orca.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    # This concept of an arbitrary list of residential uses -- rather than assuming
    # there's a use called 'residential' -- should be implemented throughout
    residential_uses = ['residential_ownerocc', 'residential_rented']
    #residential_uses = ['residential']
    if use in residential_uses: 
        s *= 1.2
    return s


@orca.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    settings = orca.get_injectable('settings')
    form_to_btype = settings["form_to_btype"]
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [orca.get_table('zoning_baseline')
               ['type%d' % typ] > 0 for typ in form_to_btype[form]]
    s = pd.concat(allowed, axis=1).max(axis=1).\
        reindex(orca.get_table('parcels').index).fillna(False)

    #if form == "residential":
    #    # allow multifam in pdas 
    #    s[orca.get_table('parcels').pda.notnull()] = 1 

    return s


# actual columns start here
@orca.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario, scenario_inputs):
    return utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_far", "far_up").\
        reindex(parcels.index)


@orca.column('parcels', 'zoned_du', cache=True)
def zoned_du(parcels):
    GROSS_AVE_UNIT_SIZE = 1000
    s = parcels.max_dua * parcels.parcel_acres
    s2 = parcels.max_far * parcels.parcel_size / GROSS_AVE_UNIT_SIZE
    return s.fillna(s2).reindex(parcels.index).fillna(0).round().astype('int')


@orca.column('parcels', 'zoned_du_underbuild')
def zoned_du_underbuild(parcels):
    s = (parcels.zoned_du - parcels.total_residential_units).clip(lower=0)
    ratio = (s / parcels.total_residential_units).replace(np.inf, 1)
    # if the ratio of additional units to existing units is not at least .5
    # we don't build it - I mean we're not turning a 10 story building into an
    # 11 story building
    s = s[ratio > .5].reindex(parcels.index).fillna(0)
    return s


@orca.column('parcels')
def nodev(zoning_baseline):
    return zoning_baseline.nodev


@orca.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario, scenario_inputs):
    s =  utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_dua", "dua_up").\
        reindex(parcels.index)
    s[parcels.pda.notnull() & (parcels.county_id != 75)] = s.fillna(16).clip(16)
    return s


@orca.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index)


@orca.column('parcels', 'residential_purchase_price_sqft')
def residential_purchase_price_sqft(parcels):
    return parcels.building_purchase_price_sqft


# don't know where this is used, but fixed the type code  -Sam 
@orca.column('parcels', 'residential_sales_price_sqft')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential_ownerocc")


# don't know where this is used, but fixed the type code  -Sam
# for debugging reasons this is split out into its own function
@orca.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft():
    return parcel_average_price("residential_ownerocc")


@orca.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels):
    return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'land_cost')
def land_cost(parcels):
    return parcels.building_purchase_price + parcels.parcel_size * 12.21


@orca.column('parcels', 'county')
def county(parcels, settings):
    return parcels.county_id.map(settings["county_id_map"])


@orca.column('parcels', 'cost_shifters')
def cost_shifters(parcels, settings):
    return parcels.county.map(settings["cost_shifters"])


@orca.column('parcels', 'price_shifters')
def price_shifters(parcels, settings):
    return parcels.pda.map(settings["pda_price_shifters"]).fillna(1.0)


@orca.column('parcels', 'node_id')
def node_id(parcels):
    return parcels._node_id

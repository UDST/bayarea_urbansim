import numpy as np
import pandas as pd
from urbansim.utils import misc
import orca
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


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


@orca.column('households', 'income_decile', cache=True)
def income_decile(households):
    s = pd.Series(pd.qcut(households.income, 10, labels=False),
                  index=households.index)
    # convert income quartile from 0-9 to 1-10
    s = s.add(1)
    return s


@orca.column('buildings', cache=True)
def modern_condo(buildings):
    # this is to try and differentiate between new construction in the city vs in the burbs
    return ((buildings.year_built > 2000) * (buildings.building_type_id == 3)).astype('int')


#####################
# PARCELS VARIABLES
#####################

# these are actually functions that take parameters, but are parcel-related
# so are defined here
@orca.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use, quantile=.5):
    # I'm testing out a zone aggregation rather than a network aggregation
    # because I want to be able to determine the quantile of the distribution
    # I also want more spreading in the development and not keep it so localized
    if use == "residential":
        buildings = orca.get_table('buildings')
        s = misc.reindex(buildings.
                            residential_price[buildings.general_type ==
                                              "Residential"].
                            groupby(buildings.zone_id).quantile(.8),
                            orca.get_table('parcels').zone_id).clip(150, 1250)
        cost_shifters = orca.get_table("parcels").cost_shifters
        price_shifters = orca.get_table("parcels").price_shifters
        return s / cost_shifters * price_shifters

    if 'nodes' not in orca.list_tables():
        return pd.Series(0, orca.get_table('parcels').index)

    return misc.reindex(orca.get_table('nodes')[use],
                        orca.get_table('parcels').node_id)


@orca.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use == "residential": s *= 1.0
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


@orca.column('parcels')
def parcel_nodev_rules(parcels):
    # removes no dev parcels, parcels with buildings < 1940,
    # and single family homes on less then half an acre
    s = (parcels.nodev == 1) | (parcels.oldest_building < 1940) | \
        ((parcels.total_residential_units == 1) & (parcels.parcel_acres < .5))
    return s.reindex(parcels.index).fillna(0).astype('int')


@orca.column('parcels', 'zoned_du', cache=True)
def zoned_du(parcels):
    GROSS_AVE_UNIT_SIZE = 1000
    s = parcels.max_dua * parcels.parcel_acres
    s2 = parcels.max_far * parcels.parcel_size / GROSS_AVE_UNIT_SIZE
    s3 = parcel_is_allowed('residential')
    return (s.fillna(s2)*s3).reindex(parcels.index).fillna(0).astype('int')


@orca.column('parcels', 'zoned_du_underbuild')
def zoned_du_underbuild(parcels):
    s = (parcels.zoned_du - parcels.total_residential_units).clip(lower=0)
    ratio = (s / parcels.total_residential_units).replace(np.inf, 1)
    # if the ratio of additional units to existing units is not at least .5
    # we don't build it - I mean we're not turning a 10 story building into an
    # 11 story building
    s = s[ratio > .5].reindex(parcels.index).fillna(0)
    return s.astype('int')


@orca.column('parcels')
def zoned_du_underbuild_nodev(parcels):
    return (parcels.zoned_du_underbuild * parcels.parcel_nodev_rules).astype('int')


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


@orca.column('parcels', 'residential_sales_price_sqft')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential")


# for debugging reasons this is split out into its own function
@orca.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft():
    return parcel_average_price("residential")


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

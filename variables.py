import numpy as np
import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# NODE VARIABLES
#####################


@sim.column('nodes', 'poverty_rate', cache=True)
def poverty_rate(nodes):
    return nodes.poor.divide(nodes.population).fillna(0)


#####################
# HOUSEHOLD VARIABLES
#####################


# overriding these to remove NaNs, in order to fix bug where HLCM won't estimate
@sim.column('households', 'zone_id', cache=True)
def zone_id(households, buildings):
    return misc.reindex(buildings.zone_id, households.building_id).fillna(-1)


@sim.column('households', 'node_id', cache=True)
def node_id(households, buildings):
    return misc.reindex(buildings.node_id, households.building_id).fillna(-1)


#####################
# BUILDING VARIABLES
#####################


# now that price is on units, override default and aggregate UP to buildings
@sim.column('buildings', 'residential_price')
def residential_price(buildings, residential_units):
    return residential_units.unit_residential_price.\
        groupby(residential_units.building_id).median().\
        reindex(buildings.index).fillna(0)


@sim.column('buildings', 'residential_rent')
def residential_rent(buildings, residential_units):
    return residential_units.unit_residential_rent.\
        groupby(residential_units.building_id).median().\
        reindex(buildings.index).fillna(0)


# override to remove NaNs, in order for HLCM to estimate
@sim.column('buildings', 'node_id', cache=True)
def node_id(buildings, parcels):
    return misc.reindex(parcels.node_id, buildings.parcel_id).fillna(-1)


# a handful were not matching, so filling with zeros for now
@sim.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    return buildings.building_type_id.fillna(-1).map(building_sqft_per_job).fillna(0)


#####################
# COSTAR VARIABLES
#####################


@sim.column('costar', 'general_type')
def general_type(costar):
    return costar.PropertyType


@sim.column('costar', 'rent')
def rent(costar):
    return costar.averageweightedrent


@sim.column('costar', 'stories')
def stories(costar):
    return costar.number_of_stories


@sim.column('costar', 'node_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)


@sim.column('costar', 'zone_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.zone_id, costar.parcel_id)


#####################
# JOBS VARIABLES
#####################


@sim.column('jobs', 'naics', cache=True)
def naics(jobs):
    return jobs.sector_id


@sim.column('jobs', 'empsix', cache=True)
def empsix(jobs, settings):
    return jobs.naics.map(settings['naics_to_empsix'])


@sim.column('jobs', 'empsix_id', cache=True)
def empsix_id(jobs, settings):
    return jobs.empsix.map(settings['empsix_name_to_id'])


#####################
# PARCELS VARIABLES
#####################


# these are actually functions that take parameters, but are parcel-related
# so are defined here
@sim.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use):
    # Fletcher had some code here to switch network aggregation with zone + quantile
    # aggregation, but I'm rolling it back for simplicity (-Sam)
    return misc.reindex(sim.get_table('nodes')[use],
                        sim.get_table('parcels').node_id)


@sim.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use in ["residential_ownerocc", "residential_rented"]: 
        s *= 1.2
    return s


@sim.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    settings = sim.settings
    form_to_btype = settings["form_to_btype"]
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [sim.get_table('zoning_baseline')
               ['type%d' % typ] > 0 for typ in form_to_btype[form]]
    return pd.concat(allowed, axis=1).max(axis=1).\
        reindex(sim.get_table('parcels').index).fillna(False)


# actual columns start here
@sim.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario, scenario_inputs):
    return utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_far", "far_up").\
        reindex(parcels.index)


@sim.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario, scenario_inputs):
    return utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_dua", "dua_up").\
        reindex(parcels.index)


@sim.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index)


@sim.column('parcels', 'residential_purchase_price_sqft')
def residential_purchase_price_sqft(parcels):
    return parcels.building_purchase_price_sqft


# don't know where this is used, but fixed the type code  -Sam 
@sim.column('parcels', 'residential_sales_price_sqft')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential_ownerocc")


# don't know where this is used, but fixed the type code  -Sam
# for debugging reasons this is split out into its own function
@sim.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft():
    return parcel_average_price("residential_ownerocc") * .9


@sim.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels):
    return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'land_cost')
def land_cost(parcels):
    return parcels.building_purchase_price + parcels.parcel_size * 12.21


@sim.column('parcels', 'county')
def county(parcels, settings):
    return parcels.county_id.map(settings["county_id_map"])


@sim.column('parcels', 'cost_shifters')
def price_shifters(parcels, settings):
    return parcels.county.map(settings["cost_shifters"])


@sim.column('parcels', 'node_id')
def node_id(parcels):
    return parcels._node_id

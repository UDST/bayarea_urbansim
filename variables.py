import numpy as np
import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


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
def parcel_average_price(use, quantile=.5):
    # I'm testing out a zone aggregation rather than a network aggregation
    # because I want to be able to determine the quantile of the distribution
    # I also want more spreading in the development and not keep it so localized
    if use == "residential":
        buildings = sim.get_table('buildings')
        return misc.reindex(buildings.
                            residential_price[buildings.general_type ==
                                              "Residential"].
                            groupby(buildings.zone_id).quantile(quantile),
                            sim.get_table('parcels').zone_id).clip(150, 1250)

    if 'nodes' not in sim.list_tables():
        return pd.Series(0, sim.get_table('parcels').index)

    return misc.reindex(sim.get_table('nodes')[use],
                        sim.get_table('parcels').node_id)


@sim.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use == "residential": s *= 1.2
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
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario, scenario_inputs):
    return utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_dua", "dua_up").\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index).fillna(0)


@sim.column('parcels', 'residential_purchase_price_sqft')
def residential_purchase_price_sqft(parcels):
    return parcels.building_purchase_price_sqft


@sim.column('parcels', 'residential_sales_price_sqft')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential")


# for debugging reasons this is split out into its own function
@sim.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft():
    return parcel_average_price("residential") * .81


@sim.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels):
    return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'land_cost')
def land_cost(parcels):
    return parcels.building_purchase_price + parcels.parcel_size * 12.21


@sim.column('parcels', 'node_id')
def node_id(parcels):
    return parcels._node_id

import numpy as np
import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# LOCAL ZONES VARIABLES
#####################


@sim.column('logsums', 'empirical_price')
def empirical_price(homesales, logsums):
    # put this here as a custom bay area indicator
    s = homesales.sale_price_flt.groupby(homesales.zone_id).quantile()
    # if price isn't present fill with median price
    s = s.reindex(logsums.index).fillna(s.quantile())
    s[s < 200] = 200
    return s


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
    return jobs.naics11cat


@sim.column('jobs', 'empsix', cache=True)
def empsix(jobs, settings):
    return jobs.naics.map(settings['naics_to_empsix'])


@sim.column('jobs', 'empsix_id', cache=True)
def empsix_id(jobs, settings):
    return jobs.empsix.map(settings['empsix_name_to_id'])


#####################
# HOMESALES VARIABLES
#####################


@sim.column('homesales', 'sale_price_flt')
def sale_price_flt(homesales):
    col = homesales.Sale_price.str.replace('$', '').\
        str.replace(',', '').astype('f4') / homesales.sqft_per_unit
    col[homesales.sqft_per_unit == 0] = 0
    return col


@sim.column('homesales', 'year_built')
def year_built(homesales):
    return homesales.Year_built


@sim.column('homesales', 'lot_size_per_unit')
def lot_size_per_unit(homesales):
    return homesales.Lot_size


@sim.column('homesales', 'sqft_per_unit')
def sqft_per_unit(homesales):
    return homesales.SQft


@sim.column('homesales', 'city')
def city(homesales):
    return homesales.City


@sim.column('homesales', 'zone_id')
def zone_id(parcels, homesales):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


@sim.column('homesales', 'node_id')
def node_id(parcels, homesales):
    return misc.reindex(parcels.node_id, homesales.parcel_id)


#####################
# PARCELS VARIABLES
#####################


# move tpp_id from parcels_geography to parcels
@sim.column('parcels')
def tpp_id(parcels_geography):
    return parcels_geography.tpp_id.fillna(-1)


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
                            sim.get_table('parcels').zone_id)
    
    # experimenting using the zone base aggregation for nonres buildings too
    buildings = sim.get_table('buildings')
    # camel case
    use = use[0].upper() + use[1:]
    s =  misc.reindex(buildings.
                        non_residential_price[buildings.general_type ==
                                              use].
                        groupby(buildings.zone_id).quantile(quantile),
                        sim.get_table('parcels').zone_id)
    # where the parcels aren't in a tpp, increase the revenue generation
    # a bit - this is calibration because non-res prefers the tpas too much
    # s[sim.get_table('parcels_geography').tpp_id.reindex(s.index).isnull()] += 30.0
    return s
    '''
    if 'nodes' not in sim.list_tables():
        return pd.Series(0, sim.get_table('parcels').index)

    return misc.reindex(sim.get_table('nodes')[use],
                        sim.get_table('parcels').node_id)
    '''


@sim.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    # this was the factor used in the parking simulations
    if use == "residential": s *= 1.7
    # if use == "residential": s = s.clip(60)
    return s


@sim.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    settings = sim.settings
    form_to_btype = settings["form_to_btype"]
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [sim.get_table('zoning_baseline')
               ['type%d' % typ] == 't' for typ in form_to_btype[form]]
    return pd.concat(allowed, axis=1).max(axis=1).\
        reindex(sim.get_table('parcels').index).fillna(False)


# actual columns start here
@sim.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario, scenario_inputs):
    s = utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_far", "far_up").\
        reindex(parcels.index).fillna(0)
    # clip the fars outside of san francisco to a max of 6
    # the oakland fars are unreasonably high and the simulation is
    # putting too many buildings there
    t = s[parcels.county_id != 38].clip(0, 3.5)
    s.loc[t.index] = t.values
    return s


@sim.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario, scenario_inputs):
    return utils.conditional_upzone(scenario, scenario_inputs,
                                    "max_dua", "dua_up").\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index).fillna(0)


@sim.column('parcels', 'residential_purchase_price_sqft',
            cache=True, cache_scope='iteration')
def residential_purchase_price_sqft(parcels):
    return parcels.building_purchase_price_sqft


@sim.column('parcels', 'residential_sales_price_sqft',
            cache=True, cache_scope='iteration')
def residential_sales_price_sqft(parcel_sales_price_sqft_func):
    return parcel_sales_price_sqft_func("residential")


# for debugging reasons this is split out into its own function
@sim.column('parcels', 'building_purchase_price_sqft',
            cache=True, cache_scope='iteration')
def building_purchase_price_sqft():
    return parcel_average_price("residential") * .81


@sim.column('parcels', 'building_purchase_price',
            cache=True, cache_scope='iteration')
def building_purchase_price(parcels):
    return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'land_cost',
            cache=True, cache_scope='iteration')
def land_cost(parcels):
    return parcels.building_purchase_price + parcels.parcel_size * 12.21


@sim.column('parcels', 'node_id', cache=True)
def node_id(parcels):
    return parcels._node_id

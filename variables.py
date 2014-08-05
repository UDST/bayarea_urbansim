import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import dataset
import utils


#####################
# BUILDINGS VARIABLES
#####################


@sim.column('buildings', '_node_id', cache=True)
def _node_id(buildings, parcels):
    return misc.reindex(parcels._node_id, buildings.parcel_id)


@sim.column('buildings', '_node_id0', cache=True)
def _node_id0(buildings, parcels):
    return misc.reindex(parcels._node_id0, buildings.parcel_id)


@sim.column('buildings', 'zone_id', cache=True)
def zone_id(buildings, parcels):
    return misc.reindex(parcels.zone_id, buildings.parcel_id)


@sim.column('buildings', 'general_type', cache=True)
def general_type(buildings, building_type_map):
    return buildings.building_type_id.map(building_type_map)


@sim.column('buildings', 'unit_sqft', cache=True)
def unit_sqft(buildings):
    return buildings.building_sqft / buildings.residential_units.replace(0, 1)


@sim.column('buildings', 'unit_lot_size', cache=True)
def unit_lot_size(buildings, parcels):
    return misc.reindex(parcels.parcel_size, buildings.parcel_id) / \
        buildings.residential_units.replace(0, 1)


@sim.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    return buildings.building_type_id.fillna(-1).map(building_sqft_per_job)


@sim.column('buildings', 'job_spaces', cache=True)
def job_spaces(buildings):
    return (buildings.non_residential_sqft /
            buildings.sqft_per_job).fillna(0).astype('int')


@sim.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0)


@sim.column('buildings', 'vacant_job_spaces')
def vacant_job_spaces(buildings, jobs):
    return buildings.job_spaces.sub(
        jobs.building_id.value_counts(), fill_value=0)


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


#####################
# APARTMENTS VARIABLES
#####################


@sim.column('apartments', '_node_id')
def _node_id(parcels, apartments):
    return misc.reindex(parcels._node_id, apartments.parcel_id)


@sim.column('apartments', 'rent')
def rent(apartments):
    return (apartments.MinOfLowRent + apartments.MaxOfHighRent) / \
        2.0 / apartments.unit_sqft


@sim.column('apartments', 'unit_sqft')
def unit_sqft(apartments):
    return apartments.AvgOfSquareFeet


#####################
# HOUSEHOLDS VARIABLES
#####################


@sim.column('households', 'income_quartile', cache=True)
def income_quartile(households):
    return pd.Series(pd.qcut(households.income, 4).labels,
                     index=households.index)


@sim.column('households', 'zone_id', cache=True)
def zone_id(households, buildings):
    return misc.reindex(buildings.zone_id, households.building_id)


@sim.column('households', '_node_id', cache=True)
def _node_id(households, buildings):
    return misc.reindex(buildings._node_id, households.building_id)


@sim.column('households', '_node_id0', cache=True)
def _node_id0(households, buildings):
    return misc.reindex(buildings._node_id0, households.building_id)


#####################
# JOBS VARIABLES
#####################


@sim.column('jobs', '_node_id', cache=True)
def _node_id(jobs, buildings):
    return misc.reindex(buildings._node_id, jobs.building_id)


@sim.column('jobs', '_node_id0', cache=True)
def _node_id0(jobs, buildings):
    return misc.reindex(buildings._node_id0, jobs.building_id)


@sim.column('jobs', 'zone_id', cache=True)
def zone_id(jobs, buildings):
    return misc.reindex(buildings.zone_id, jobs.building_id)


@sim.column('jobs', 'naics', cache=True)
def naics(jobs):
    return jobs.naics11cat


#####################
# HOMESALES VARIABLES
#####################


@sim.column('homesales', 'sale_price_flt')
def sale_price_flt(homesales):
    return homesales.Sale_price.str.replace('$', '').\
        str.replace(',', '').astype('f4') / homesales.unit_sqft


@sim.column('homesales', 'year_built')
def year_built(homesales):
    return homesales.Year_built


@sim.column('homesales', 'unit_lot_size')
def unit_lot_size(homesales):
    return homesales.Lot_size


@sim.column('homesales', 'unit_sqft')
def unit_sqft(homesales):
    return homesales.SQft


@sim.column('homesales', 'city')
def city(homesales):
    return homesales.City


@sim.column('homesales', 'zone_id')
def zone_id(parcels, homesales):
    return misc.reindex(parcels.zone_id, homesales.parcel_id)


#####################
# PARCELS VARIABLES
#####################


def parcel_average_price(use):
    return misc.reindex(sim.get_table('nodes_prices')[use],
                        sim.get_table('parcels')._node_id)


def parcel_is_allowed(form):
    form_to_btype = sim.get_injectable("form_to_btype")
    # we have zoning by building type but want
    # to know if specific forms are allowed
    allowed = [sim.get_table('zoning_baseline')
               ['type%d' % typ] == 't' for typ in form_to_btype[form]]
    return pd.concat(allowed, axis=1).max(axis=1).\
        reindex(sim.get_table('parcels').index).fillna(False)


@sim.column('parcels', 'max_far', cache=True)
def max_far(parcels, scenario):
    return utils.conditional_upzone(scenario, "max_far", "far_up").\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, scenario):
    return utils.conditional_upzone(scenario, "max_far", "dua_up").\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_height', cache=True)
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index).fillna(0)


@sim.column('parcels', 'parcel_size', cache=True)
def parcel_size(parcels):
    return parcels.shape_area * 10.764


@sim.column('parcels', 'total_units', cache=True)
def total_units(parcels, buildings):
    return buildings.residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'total_job_spaces', cache=True)
def total_job_spaces(parcels, buildings):
    return buildings.job_spaces.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'total_sqft', cache=True)
def total_sqft(parcels, buildings):
    return buildings.building_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@sim.column('parcels', 'ave_unit_size')
def ave_unit_size(parcels, nodes):
    if len(nodes) == 0:
        # if nodes isn't generated yet
        return pd.Series(index=parcels.index)
    return misc.reindex(nodes.ave_unit_sqft, parcels._node_id)


@sim.column('parcels', 'land_cost')
def land_cost(parcels, nodes_prices):
    if len(nodes_prices) == 0:
        # if nodes_prices isn't generated yet
        return pd.Series(index=parcels.index)
    # TODO
    # this needs to account for cost for the type of building it is
    return (parcels.total_sqft * parcel_average_price("residential")).\
        reindex(parcels.index).fillna(0)

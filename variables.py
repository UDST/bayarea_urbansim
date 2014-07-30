import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import dataset


#####################
# BUILDINGS VARIABLES
#####################


@sim.column('buildings', '_node_id')
def _node_id(buildings, parcels):
    return misc.reindex(parcels._node_id, buildings.parcel_id)


@sim.column('buildings', '_node_id0')
def _node_id0(buildings, parcels):
    return misc.reindex(parcels._node_id0, buildings.parcel_id)


@sim.column('buildings', 'zone_id')
def zone_id(buildings, parcels):
    return misc.reindex(parcels.zone_id, buildings.parcel_id)


@sim.column('buildings', 'general_type')
def general_type(buildings, building_type_map):
    return buildings.building_type_id.map(building_type_map)


@sim.column('buildings', 'unit_sqft')
def unit_sqft(buildings):
    return buildings.building_sqft / buildings.residential_units


@sim.column('buildings', 'unit_lot_size')
def unit_lot_size(buildings, parcels):
    return misc.reindex(parcels.parcel_size, buildings.parcel_id) / buildings.residential_units


@sim.column('buildings', 'sqft_per_job')
def sqft_per_job(buildings, building_sqft_per_job):
    return misc.reindex(building_sqft_per_job.sqft_per_job, buildings.building_type_id.fillna(-1))


@sim.column('buildings', 'non_residential_units')
def non_residential_units(buildings):
    return (buildings.non_residential_sqft / buildings.sqft_per_job).fillna(0).astype('int')


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
    return (apartments.MinOfLowRent + apartments.MaxOfHighRent) / 2.0 / apartments.unit_sqft


@sim.column('apartments', 'unit_sqft')
def unit_sqft(apartments):
    return apartments.AvgOfSquareFeet


#####################
# HOUSEHOLDS VARIABLES
#####################


@sim.column('households', 'income_quartile')
def income_quartile(households):
    return pd.Series(pd.qcut(households.income, 4).labels, index=households.index)


@sim.column('households', 'zone_id')
def zone_id(households, buildings):
    return misc.reindex(buildings.zone_id, households.building_id)


@sim.column('households', '_node_id')
def _node_id(households, buildings):
    return misc.reindex(buildings._node_id, households.building_id)


@sim.column('households', '_node_id0')
def _node_id0(households, buildings):
    return misc.reindex(buildings._node_id0, households.building_id)


#####################
# JOBS VARIABLES
#####################


@sim.column('jobs', '_node_id')
def _node_id(jobs, buildings):
    return misc.reindex(buildings._node_id, jobs.building_id)


@sim.column('jobs', '_node_id0')
def _node_id0(jobs, buildings):
    return misc.reindex(buildings._node_id0, jobs.building_id)


@sim.column('jobs', 'zone_id')
def zone_id(jobs, buildings):
    return misc.reindex(buildings.zone_id, jobs.building_id)


@sim.column('jobs', 'naics')
def naics(jobs):
    return jobs.naics11cat


#####################
# HOMESALES VARIABLES
#####################


@sim.column('homesales', 'sale_price_flt')
def sale_price_flt(homesales):
    return homesales.Sale_price.str.replace('$', '').str.replace(',', '').astype('f4') / \
        homesales.unit_sqft


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
    # we have zoning by building type but want to know if specific forms are allowed
    allowed = [sim.get_table('zoning_baseline')['type%d' % typ] == 't' for typ in dataset.FORM_TO_BTYPE[form]]
    return pd.concat(allowed, axis=1).max(axis=1).reindex(sim.get_table('parcels').index).fillna(False)


@sim.column('parcels', 'max_far')
def max_far(parcels, zoning_baseline, zoning_test, scenario):
    max_far = zoning_baseline.max_far
    if scenario == "test":
        upzone = zoning_test.far_up.dropna()
        max_far = pd.concat([max_far, upzone], axis=1).max(skipna=True, axis=1)
    return max_far.reindex(parcels.index).fillna(0)


@sim.column('parcels', 'max_height')
def max_height(parcels, zoning_baseline):
    return zoning_baseline.max_height.reindex(parcels.index).fillna(0)


@sim.column('parcels', 'parcel_size')
def parcel_size(parcels):
    return parcels.shape_area * 10.764


@sim.column('parcels', 'ave_unit_sqft')
def ave_unit_sqft(parcels, nodes):
    return misc.reindex(nodes.ave_unit_sqft, parcels._node_id)


@sim.column('parcels', 'total_units')
def total_units(buildings):
    return buildings.residential_units.groupby(buildings.parcel_id).sum().fillna(0)


@sim.column('parcels', 'total_nonres_units')
def total_nonres_units(buildings):
    return buildings.non_residential_units.groupby(buildings.parcel_id).sum().fillna(0)


@sim.column('parcels', 'total_sqft')
def total_sqft(buildings):
    return buildings.building_sqft.groupby(buildings.parcel_id).sum().fillna(0)


# causing trouble trying to load 'nodes_prices.csv'
#@sim.column('parcels', 'land_cost')
#def land_cost(parcels):
#    # TODO
#    # this needs to account for cost for the type of building it is
#    return (parcels.total_sqft * parcel_average_price("residential")).reindex(parcels.index).fillna(0)

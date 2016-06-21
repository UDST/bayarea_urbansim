import orca
from urbansim_defaults import utils
from urbansim.utils import misc
from urbansim.models.relocation import RelocationModel
from urbansim.developer.developer import Developer as dev
import os
import yaml
import numpy as np
import pandas as pd


##########################################################################################
#
# (1) UAL ORCA STEPS FOR DATA MODEL INITIALIZATION
#
##########################################################################################


@orca.injectable('ual_settings', cache=True)
def ual_settings():
	""" 
	This step loads the UAL settings, which are kept separate for clarity.
	
	Data expectations
	-----------------
	- 'configs' folder contains a file called 'ual_settings.yaml'
	- 'os.path' is expected to provide the root level of the urbansim instance, so be sure 
	  to either (a) launch the python process from that directory, or (b) use os.chdir to 
	  switch to that directory before running any model steps
	"""
	with open(os.path.join(misc.configs_dir(), 'ual_settings.yaml')) as f:
		return yaml.load(f)


@orca.step('ual_data_diagnostics')
def ual_data_diagnostics():
	"""
	Lists some tables and columns for diagnostic purposes
	"""
	print
	print "RUNNING ual_data_diagnostics"
	print orca.list_tables()
	print orca.get_table('households').columns
	print orca.get_table('residential_units').columns
	
	hh = orca.get_table('residential_units').to_frame()
	print hh[['zone_id']].describe()
	print
	return


def _ual_create_empty_units(buildings):
	"""
	Create a table of empty units corresponding to the provided buildings.
	
	Parameters
	----------
	buildings : DataFrameWrapper or DataFrame
		Must contain an index to be used as a building identifier, and a count of 
		'residential_units' which will determine the number of units to create
	
	Returns
	-------
	df : DataFrame
		Table of units, to be processed within an orca step
	"""
	df = pd.DataFrame({
		'unit_residential_price': 0,
		'unit_residential_rent': 0,
		'num_units': 1,
		'building_id': np.repeat(buildings.index.values,
								 buildings.residential_units.values),
		# counter of the units in a building
		'unit_num': np.concatenate([np.arange(i) for i in \
									buildings.residential_units.values])
	}).sort_values(by=['building_id', 'unit_num']).reset_index(drop=True)
	df.index.name = 'unit_id'
	return df
	
	
@orca.step('ual_initialize_residential_units')
def ual_initialize_residential_units(buildings, ual_settings):
	"""
	This initialization step creates and registers a table of synthetic residential units, 
	based on building info.
	
	Data expections
	---------------
	- 'buildings' table has following columns:
		- index that serves as its id
		- 'residential_units' (int, never missing)
		- 'zone_id' (int, non-missing??)
	- 'ual_settings' injectable contains list of tables called 'unit_aggregation_tables'
	
	Results
	-------
	- initializes a 'residential_units' table with the following columns:
		- 'unit_id' (index)
		- 'num_units' (int, always '1', needed when passing the table to utility functions
		  that expect it to look like a 'buildings' table)
		- 'unit_residential_price' (float, 0-filled)
		- 'unit_residential_rent' (float, 0-filled)
		- 'building_id' (int, non-missing, corresponds to index of 'buildings' table)
		- 'unit_num' (int, non-missing, unique within building) 
		- 'vacant_units' (int, 0 or 1, computed)
		- 'submarket_id' (int, non-missing, computed, corresponds to index of 'zones' table)
	- adds broadcasts linking 'residential_units' table to:
		- 'buildings' table
	- initializes a 'unit_aggregations' injectable containing tables as specified in 
		'ual_settings' -> 'unit_aggregation_tables'
	"""

	@orca.table('residential_units', cache=True)
	def residential_units(buildings):
		return _ual_create_empty_units(buildings)
	
	@orca.column('residential_units', 'vacant_units')
	def vacant_units(residential_units, households):
		return residential_units.num_units.sub(
			households.unit_id[households.unit_id != -1].value_counts(), fill_value=0)

	@orca.column('residential_units', 'submarket_id')
	def submarket_id(residential_units, buildings):
		# The submarket is used for supply/demand equilibration. It's the same as the 
		# zone_id, but in a separate column to avoid name conflicts when tables are merged.
		return misc.reindex(buildings.zone_id, residential_units.building_id)

	orca.broadcast('buildings', 'residential_units', cast_index=True, onto_on='building_id')
	
	@orca.injectable('unit_aggregations')
	def unit_aggregations(ual_settings):
		# This injectable provides a list of tables needed for hedonic and LCM model steps
		return [orca.get_table(tbl) for tbl in ual_settings['unit_aggregation_tables']]

		
@orca.step('ual_match_households_to_units')
def ual_match_households_to_units(households, residential_units):
	"""
	This initialization step adds a unit_id to the households table and populates it
	based on existing assignments of households to buildings.
	
	Data expectations
	-----------------
	- 'households' table has NO column 'unit_id'
	- 'households' table has column 'building_id' (int, '-1'-filled, corresponds to index 
		  of 'buildings' table)
	- 'residential_units' table has an index that serves as its id, and following columns:
		- 'building_id' (int, non-missing, corresponds to index of 'buildings' table)
		- 'unit_num' (int, non-missing, unique within building)
	
	Results
	-------
	- adds following column to 'households' table:
		- 'unit_id' (int, '-1'-filled, corresponds to index of 'residential_units' table)
	- adds a broadcast linking 'households' to 'residential_units'
	"""
	hh = households.to_frame(households.local_columns)
	units = residential_units.to_frame(['building_id', 'unit_num'])
	
	# This code block is from Fletcher
	unit_lookup = units.reset_index().set_index(['building_id', 'unit_num'])
	hh = hh.sort_values(by=['building_id'], ascending=True)
	building_counts = hh.building_id.value_counts().sort_index()
	hh['unit_num'] = np.concatenate([np.arange(i) for i in building_counts.values])
	unplaced = hh[hh.building_id == -1].index
	placed = hh[hh.building_id != -1].index
	indexes = [tuple(t) for t in hh.loc[placed, ['building_id', 'unit_num']].values]
	hh.loc[placed, 'unit_id'] = unit_lookup.loc[indexes].unit_id.values
	hh.loc[unplaced, 'unit_id'] = -1
	orca.add_table('households', hh)
	return

	orca.broadcast('residential_units', 'households', cast_index=True, onto_on='unit_id')


@orca.step('ual_assign_tenure_to_units')
def ual_assign_tenure_to_units(residential_units, households):
	"""
	This initialization step assigns tenure to residential units, based on the 'hownrent'
	attribute of the households occupying them. (Tenure for unoccupied units is assigned
	randomly.)
	
	Data expections
	---------------
	- 'residential_units' table has NO column 'hownrent'
	- 'households' table has following columns: 
		- 'hownrent' (int, missing values ok) 
		- 'unit_id' (int, '-1'-filled, corresponds to index of 'residential_units' table)
	
	Results
	-------
	- adds following column to 'residential_units' table:
		- 'hownrent' (int in range [1,2], non-missing)
	"""
	units = residential_units.to_frame(residential_units.local_columns)
	hh = households.to_frame(['hownrent', 'unit_id'])
	
	# 'Hownrent' is a PUMS field where 1=owns, 2=rents. Note that there's also a field
	# in the MTC households table called 'tenure', with min=1, max=4, mean=2. Not sure 
	# where this comes from or what the values indicate.
		
	units['hownrent'] = np.nan
	own = hh[(hh.hownrent == 1) & (hh.unit_id != -1)].unit_id.values
	rent = hh[(hh.hownrent == 2) & (hh.unit_id != -1)].unit_id.values
	units.loc[own, 'hownrent'] = 1
	units.loc[rent, 'hownrent'] = 2
		
	print "Initial unit tenure assignment: %d%% owner occupied, %d%% unfilled" % \
			(round(len(units[units.hownrent == 1])*100/len(units[units.hownrent.notnull()])), \
			 round(len(units[units.hownrent.isnull()])*100/len(units)))
			 
	# Fill remaining units with random tenure assignment 
	# TO DO: Make this weighted by existing allocation, rather than 50/50
	unfilled = units[units.hownrent.isnull()].index
	units.loc[unfilled, 'hownrent'] = np.random.randint(1, 3, len(unfilled))
	
	orca.add_table('residential_units', units)
	return


@orca.step('ual_load_rental_listings')
def ual_load_rental_listings():
	"""
	This initialization step loads the Craigslist rental listings data for hedonic 
	estimation. Not needed for simulation.
	
	Data expectations
	-----------------
	- injectable 'net' that can provide 'node_id' and 'tmnode_id' from lat-lon coordinates
	- some way to get 'zone_id' (currently using parcels table)
	- 'sfbay_craigslist.csv' file
	
	Results
	-------
	- creates new 'craigslist' table with the following columns:
		- 'price' (int, may be missing)
		- 'sqft_per_unit' (int, may be missing)
		- 'price_per_sqft' (float, may be missing)
		- 'bedrooms' (int, may be missing)
		- 'neighborhood' (string, ''-filled)
		- 'node_id' (int, may be missing, corresponds to index of 'nodes' table)
		- 'tmnode_id' (int, may be missing, corresponds to index of 'tmnodes' tables)
		- 'zone_id' (int, may be missing, corresponds to index of 'zones' table)
	- adds broadcasts linking 'craigslist' to 'nodes', 'tmnodes', 'logsums'
	"""
	@orca.table('craigslist', cache=True)
	def craigslist():
		df = pd.read_csv(os.path.join(misc.data_dir(), "sfbay_craigslist.csv"))
		net = orca.get_injectable('net')
		df['node_id'] = net['walk'].get_node_ids(df['lon'], df['lat'])
		df['tmnode_id'] = net['drive'].get_node_ids(df['lon'], df['lat'])
		# fill nans -- missing bedrooms are mostly studio apts
		df['bedrooms'] = df.bedrooms.replace(np.nan, 1)
		df['neighborhood'] = df.neighborhood.replace(np.nan, '')
		return df

	# Is it simpler to just do this in the table definition since it is never updated?
	@orca.column('craigslist', 'zone_id', cache=True)
	def zone_id(craigslist, parcels):
		return misc.reindex(parcels.zone_id, craigslist.node_id)
		
	orca.broadcast('nodes', 'craigslist', cast_index=True, onto_on='node_id')
	orca.broadcast('tmnodes', 'craigslist', cast_index=True, onto_on='tmnode_id')
	orca.broadcast('logsums', 'craigslist', cast_index=True, onto_on='zone_id')
	return



##########################################################################################
#
# (2) UAL ORCA STEPS FOR DATA MODEL MAINTENANCE
#
##########################################################################################


@orca.step('ual_reconcile_placed_households')
def reconcile_placed_households(households, residential_units):
	"""
	This data maintenance step keeps the building/unit/household correspondence up to 
	date by reconciling placed households.
	
	In the current data model, households should have both a 'building_id' and 'unit_id'
	when they have been matched with housing. But the existing HLCM models assign only
	a 'unit_id', so this model step updates the building id's accordingly. 
	
	Data expectations
	-----------------
	- 'households' table has the following columns:
		- index that serves as its id
		- 'unit_id' (int, '-1'-filled)
		- 'building_id' (int, '-1'-filled)
	- 'residential_units' table has the following columns:
		- index that serves as its id
		- 'building_id' (int, non-missing, corresponds to index of the 'buildings' table)
	
	Results
	-------
	- updates the 'households' table:
		- 'building_id' updated where it was -1 but 'unit_id' wasn't
	"""
	print "Reconciling placed households..."
	hh = households.to_frame(['unit_id', 'building_id'])
	units = residential_units.to_frame(['building_id'])
	
	# Filter for households missing a 'building_id' but not a 'unit_id'
	hh = hh[(hh.building_id == -1) & (hh.unit_id != -1)]
	
	# Join building id's
	hh = pd.merge(hh[['unit_id']], units, left_on='unit_id', right_index=True, how='left')
	
	print "%d movers updated" % len(hh)
	households.update_col_from_series('building_id', hh.building_id)
	return
	

@orca.step('ual_reconcile_unplaced_households')
def reconcile_unplaced_households(households):
	"""
	This data maintenance step keeps the building/unit/household correspondence up to 
	date by reconciling unplaced households.
	
	In the current data model, households should have both a 'building_id' and 'unit_id'
	of -1 when they are not matched with housing. But sometimes only of these is set when
	households are created or unplaced. If households have been unplaced from buildings, 
	this model step unplaces them from units as well. Or if they have been unplaced from 
	units, it unplaces them from buildings. 
	
	Data expectations
	-----------------
	- 'households' table has an index, and these columns:
		- 'unit_id' (int, '-1'-filled)
		- 'building_id' (int, '-1'-filled)
	
	Results
	-------
	- updates the 'households' table:
		- 'unit_id' = 'building_id' = -1 for the superset of rows where either column
		  initially had this vaue
	"""
	def _print_status():
		print "Households not in a unit: %d" % (households.unit_id == -1).sum()
		print "Househing missing a unit: %d" % households.unit_id.isnull().sum()
		print "Households not in a building: %d" % (households.building_id == -1).sum()
		print "Househing missing a building: %d" % households.building_id.isnull().sum()

	_print_status()
	print "Reconciling unplaced households..."
	hh = households.to_frame(['building_id', 'unit_id'])
	
	# Get indexes of households unplaced in buildings or in units
	bldg_unplaced = pd.Series(-1, index=hh[hh.building_id == -1].index)
	unit_unplaced = pd.Series(-1, index=hh[hh.unit_id == -1].index)
	
	# Update those households to be fully unplaced
	households.update_col_from_series('building_id', unit_unplaced)
	households.update_col_from_series('unit_id', bldg_unplaced)
	_print_status()
	return
	

@orca.step('ual_update_building_residential_price')
def ual_update_building_residential_price(buildings, residential_units, ual_settings):
	"""
	This data maintenance step updates the prices in the buildings table to reflect 
	changes to the unit-level prices. This allows model steps like 'price_vars' and 
	'feasibility' to read directly from the buildings table. 
	
	We currently set the building price per square foot to be the higher of the average
	(a) unit price per square foot or (b) unit price-adjusted rent per square foot.
	
	Data expectations
	-----------------
	- 'residential_units' table has following columns:
		- 'unit_residential_price' (float, 0-filled)
		- 'unit_residential_rent' (float, 0-filled)
		- 'building_id' (int, non-missing, corresponds to index of 'buildings' table)
	- 'buildings' table has following columns:
		- index that serves as its id
		- 'residential_price' (float, 0-filled)
	- 'ual_settings' injectable has a 'cap_rate' (float, range 0 to 1)
	
	Results
	-------
	- updates the 'buildings' table:
		- 'residential_price' = max avg of unit prices or rents
	"""
	cols = ['building_id', 'unit_residential_price', 'unit_residential_rent']
	means = residential_units.to_frame(cols).groupby(['building_id']).mean()
	
	# Convert monthly rent to equivalent sale price
	cap_rate = ual_settings.get('cap_rate')
	means['unit_residential_rent'] = means.unit_residential_rent * 12 / cap_rate
	
	# Calculate max of price or rent, by building
	means['max_potential'] = means.max(axis=1)
	print means.describe()
	
	# Update the buildings table
	buildings.update_col_from_series('residential_price', means.max_potential)
	return
	

@orca.step('ual_remove_old_units')
def ual_remove_old_units(buildings, residential_units):
	"""
	This data maintenance step removes units whose building_ids no longer exist.
	
	If new buildings have been created that re-use prior building_ids, we would fail to
	remove the associated units. Hopefully new buidlings do not duplicate prior ids,
	but this needs to be verified!

	Data expectations
	-----------------
	- 'buildings' table has an index that serves as its identifier
	- 'residential_units' table has a column 'building_id' corresponding to the index
	  of the 'buildings' table
	
	Results
	-------
	- removes rows from the 'residential_units' table if their 'building_id' no longer
	  exists in the 'buildings' table
	"""
	units = residential_units.to_frame(residential_units.local_columns)
	current_units = units[units.building_id.isin(buildings.index)]
	
	print "Removing %d residential units from %d buildings that no longer exist" % \
			((len(units) - len(current_units)), \
			(len(units.groupby('building_id')) - len(current_units.groupby('building_id'))))
	
	orca.add_table('residential_units', current_units) 
	return


@orca.step('ual_initialize_new_units')
def ual_initialize_new_units(buildings, residential_units):
	"""
	This data maintenance step initializes units for buildings that have been newly
	created, conforming to the data requirements of the 'residential_units' table.
	
	Data expectations
	-----------------
	- 'buildings' table has the following columns:
		- index that serves as its identifier
		- 'residential_units' (int, count of units in building)
	- 'residential_units' table has the following columns:
		- index named 'unit_id' that serves as its identifier
		- 'building_id' corresponding to the index of the 'buildings' table
	
	Results
	-------
	- extends the 'residential_units' table, following the same schema as the
	  'ual_initialize_residential_units' model step
	"""
	old_units = residential_units.to_frame(residential_units.local_columns)
	bldgs = buildings.to_frame(['residential_units'])
	
	# Filter for residential buildings not currently represented in the units table
	bldgs = bldgs[bldgs.residential_units > 0]
	new_bldgs = bldgs[~bldgs.index.isin(old_units.building_id)]
	
	# Create new units, merge them, and update the table
	new_units = _ual_create_empty_units(new_bldgs)
	all_units = dev.merge(old_units, new_units)
	all_units.index.name = 'unit_id'
	
	print "Creating %d residential units for %d new buildings" % \
			(len(new_units), len(new_bldgs))
	
	orca.add_table('residential_units', all_units)
	return
	

@orca.step('ual_assign_tenure_to_new_units')
def ual_assign_tenure_to_new_units(residential_units, ual_settings):
	"""
	This data maintenance step assigns tenure to new residential units. Tenure is
	determined by comparing the fitted sale price and fitted rent from the hedonic models,
	with rents adjusted to price-equivalent terms using a cap rate.

	We may want to make this more sophisticated in the future, or at least stochastic. 
	Also, it might be better to do this assignment based on the zonal average prices and 
	rents following supply/demand equilibration.
	
	Data expectations
	-----------------
	- 'residential_units' table has the following columns:
		- 'hownrent' (int in range 1 to 2, may be missing)
		- 'unit_residential_price' (float, non-missing)
		- 'unit_residential_rent' (float, non-missing)
	
	Results
	-------
	- fills missing values of 'hownrent'
	"""
	cols = ['hownrent', 'unit_residential_price', 'unit_residential_rent']
	units = residential_units.to_frame(cols)
	
	# Filter for units that are missing a tenure assignment
	units = units[~units.hownrent.isin([1,2])]
	
	# Convert monthly rent to equivalent sale price
	cap_rate = ual_settings.get('cap_rate')
	units['unit_residential_rent'] = units.unit_residential_rent * 12 / cap_rate
	
	# Assign tenure based on higher of price or adjusted rent
	rental_units = (units.unit_residential_rent > units.unit_residential_price)
	units.loc[~rental_units, 'hownrent'] = 1
	units.loc[rental_units, 'hownrent'] = 2
	
	print "Adding tenure assignment to %d new residential units" % len(units)
	print units.describe()

	residential_units.update_col_from_series('hownrent', units.hownrent)	
	


##########################################################################################
#
# (3) UAL ORCA STEPS FOR SIMULATION LOCIC
#
##########################################################################################


@orca.step('ual_rrh_estimate')
def ual_rrh_estimate(craigslist, aggregations):
	"""
	This model step estimates a residental rental hedonic using craigslist listings.
	
	Data expectations
	-----------------
	- 'craigslist' table and others, as defined in the yaml config
	"""
	return utils.hedonic_estimate(cfg = 'ual_rrh.yaml', 
								  tbl = craigslist, 
								  join_tbls = aggregations)


def _mtc_clip(table, col_name, settings, price_scale=1):
	# This is included to match the MTC hedonic model steps, with 'price_scale' 
	# adjusting the clip bounds from price to monthly rent if needed.
	
	if "rsh_simulate" in settings:
		low = float(settings["rsh_simulate"]["low"]) * price_scale
		high = float(settings["rsh_simulate"]["high"]) * price_scale
		table.update_col(col_name, table[col_name].clip(low, high))
		print "Clipping produces\n", table[col_name].describe()


@orca.step('ual_rsh_simulate')
def ual_rsh_simulate(residential_units, unit_aggregations, settings):
	"""
	This uses the MTC's model specification from rsh.yaml, but generates unit-level
	price predictions rather than building-level.
	
	Data expectations
	-----------------
	- tk
	"""
	utils.hedonic_simulate(cfg = 'rsh.yaml', 
						   tbl = residential_units, 
						   join_tbls = unit_aggregations, 
						   out_fname = 'unit_residential_price')
	
	_mtc_clip(residential_units, 'unit_residential_price', settings)
	return


@orca.step('ual_rrh_simulate')
def ual_rrh_simulate(residential_units, unit_aggregations, settings):
	"""
	This uses an altered hedonic specification to generate unit-level rent predictions.
	
	Data expectations
	-----------------
	- tk
	"""
	utils.hedonic_simulate(cfg = 'ual_rrh.yaml', 
						   tbl = residential_units,
						   join_tbls = unit_aggregations, 
						   out_fname = 'unit_residential_rent')

	_mtc_clip(residential_units, 'unit_residential_rent', settings, price_scale=0.05/12)
	return


@orca.step('ual_households_relocation')
def ual_households_relocation(households, ual_settings):
	"""
	This model step randomly assigns households for relocation, using probabilities
	that depend on their tenure status.
	
	Data expectations
	-----------------
	- 'households' table has following columns:
		- 'hownrent' (int in range [1,2], non-missing)
		- 'building_id' (int, '-1'-filled, corredponds to index of 'buildings' table
		- 'unit_id' (int, '-1'-filled, corresponds to index of 'residential_units' table
	- 'ual_settings.yaml' has:
		- 'relocation_rates' as specified in RelocationModel() documentation
		
	Results
	-------
	- assigns households for relocation by setting their 'building_id' and 'unit_id' to -1
	"""
	rates = pd.DataFrame.from_dict(ual_settings['relocation_rates'])

	print "Total agents: %d" % len(households)
	print "Total currently unplaced: %d" % (households.unit_id == -1).sum()
	print "Assigning for relocation..."
	
	# Initialize model, choose movers, and un-place them from buildings and units
	m = RelocationModel(rates)
	mover_ids = m.find_movers(households.to_frame(['unit_id', 'hownrent']))
	households.update_col_from_series('building_id', pd.Series(-1, index=mover_ids))
	households.update_col_from_series('unit_id', pd.Series(-1, index=mover_ids))
	
	print "Total currently unplaced: %d" % (households.unit_id == -1).sum()
	return


@orca.step('ual_hlcm_owner_estimate')
def ual_hlcm_owner_estimate(households, residential_units, unit_aggregations):
	return utils.lcm_estimate(cfg = "ual_hlcm_owner.yaml",
							  choosers = households, 
							  chosen_fname = "unit_id",
							  buildings = residential_units, 
							  join_tbls = unit_aggregations)


@orca.step('ual_hlcm_renter_estimate')
def ual_hlcm_renter_estimate(households, residential_units, unit_aggregations):
	return utils.lcm_estimate(cfg = "ual_hlcm_renter.yaml",
							  choosers = households, 
							  chosen_fname = "unit_id",
							  buildings = residential_units, 
							  join_tbls = unit_aggregations)


@orca.step('ual_hlcm_owner_simulate')
def ual_hlcm_owner_simulate(households, residential_units, unit_aggregations, ual_settings):
	
	# Note that the submarket id (zone_id) needs to be in the table of alternatives,
	# for supply/demand equilibration, and needs to NOT be in the choosers table, to
	# avoid conflicting when the tables are joined
	
	return utils.lcm_simulate(cfg = 'ual_hlcm_owner.yaml', 
							  choosers = households, 
							  buildings = residential_units,
							  join_tbls = unit_aggregations,
							  out_fname = 'unit_id', 
							  supply_fname = 'num_units',
							  vacant_fname = 'vacant_units',
							  enable_supply_correction = 
									ual_settings.get('price_equilibration', None))


@orca.step('ual_hlcm_renter_simulate')
def ual_hlcm_renter_simulate(households, residential_units, unit_aggregations, ual_settings):
	return utils.lcm_simulate(cfg = 'ual_hlcm_renter.yaml', 
							  choosers = households, 
							  buildings = residential_units,
							  join_tbls = unit_aggregations,
							  out_fname = 'unit_id', 
							  supply_fname = 'num_units',
							  vacant_fname = 'vacant_units',
							  enable_supply_correction = 
									ual_settings.get('rent_equilibration', None))



# Add an initialization step (placeholder at least) to specify which units are affordable







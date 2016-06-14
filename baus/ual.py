import orca
from urbansim_defaults import utils
from urbansim.models.relocation import RelocationModel
import numpy as np
import pandas as pd


###############################################################
""" UAL ORCA STEPS FOR DATA MANAGEMENT (MODEL STEPS FOLLOW) """
###############################################################


@orca.step('ual_data_diagnostics')
def ual_data_diagnostics():
	"""
	Lists some tables and columns for diagnostic purposes
	"""
	print
	print "RUNNING ual_data_diagnostics"
	print orca.list_tables()
	print orca.get_table('buildings').local_columns
	print orca.get_table('households').local_columns
	
	hh = orca.get_table('households').to_frame()
	print hh[['tenure','hownrent']].describe()
	try:
		print orca.get_table('residential_units').local_columns
	except:
		pass
	print


@orca.step('ual_initialize_residential_units')
def ual_initialize_residential_units(buildings):
	"""
	This initialization step creates and registers a table of synthetic residential units, 
	based on building info.
	
	Data expections
	---------------
	- 'buildings' table has an index that serves as its id
	- 'buildings' table has column 'residential_units' (int, never missing)
	- 'aggregations' injectable contains list of tables
	
	Results
	-------
	- creates new 'residential_units' table with following columns:
		- 'unit_id' (index)
		- 'unit_residential_price' (float, 0-filled)
		- 'unit_residential_rent' (float, 0-filled)
		- 'building_id' (int, non-missing, corresponds to index of 'buildings' table)
		- 'unit_num' (int, non-missing, unique within building)		
	- adds broadcast linking 'residential_units' to 'buildings'
	- creates new 'unit_aggregations' injectable containing the following tables:
		- 'buildings', plus whatever is in in the 'aggregations' injectable
	"""

	@orca.table('residential_units', cache=True)
	def residential_units(buildings):
		"""
		Sets up a table of synthetic residential units, based on building info
		"""
		df = pd.DataFrame({
			'unit_residential_price': 0,
			'unit_residential_rent': 0,
			'building_id': np.repeat(buildings.index.values,
									 buildings.residential_units.values),
			# counter of the units in a building
			'unit_num': np.concatenate([np.arange(i) for i in \
										buildings.residential_units.values])
		}).sort_values(by=['building_id', 'unit_num']).reset_index(drop=True)
		df.index.name = 'unit_id'
		return df
	
	orca.broadcast('buildings', 'residential_units', cast_index=True, onto_on='building_id')
	
	@orca.injectable('unit_aggregations')
	def unit_aggregations(aggregations):
		return aggregations + [orca.get_table('buildings')]

		
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
	# -> do we need to add broadcasts linking tables together?
	return


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
	# -> Fix this to be to be weighted
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

	# Is it simpler to just do this in the table definition?
	@orca.column('craigslist', 'zone_id', cache=True)
	def zone_id(craigslist, parcels):
		return misc.reindex(parcels.zone_id, craigslist.node_id)
		
	orca.broadcast('nodes', 'craigslist', cast_index=True, onto_on='node_id')
	orca.broadcast('tmnodes', 'craigslist', cast_index=True, onto_on='tmnode_id')
	orca.broadcast('logsums', 'craigslist', cast_index=True, onto_on='zone_id')
	return



############################
""" UAL ORCA MODEL STEPS """
############################


@orca.step('ual_rrh_estimate')
def ual_rrh_estimate(craigslist, aggregations):
	"""
	This model step estimates a residental rental hedonic using craigslist listings.
	
	Data expectations
	-----------------
	- 'craigslist' table and others, as defined in the yaml config
	"""
	return utils.hedonic_estimate("ual_rrh.yaml", craigslist, aggregations)


@orca.step('ual_rsh_simulate')
def ual_rsh_simulate(residential_units, unit_aggregations, settings):
	"""
	This uses the MTC's model specification from rsh.yaml, but generates unit-level
	price predictions rather than building-level.
	
	Data expectations
	-----------------
	- tk
	"""
	utils.hedonic_simulate("rsh.yaml", residential_units, 
							unit_aggregations, "unit_residential_price")
	
	# Following is included to match the MTC model step
	if "rsh_simulate" in settings:
		low = float(settings["rsh_simulate"]["low"])
		high = float(settings["rsh_simulate"]["high"])
		residential_units.update_col("unit_residential_price",
							 residential_units.unit_residential_price.clip(low, high))
		print "Clipped rsh_simulate produces\n", \
			residential_units.unit_residential_price.describe()
	return


@orca.step('ual_rrh_simulate')
def ual_rrh_simulate(residential_units, unit_aggregations, settings):
	"""
	Description tk.
	
	Data expectations
	-----------------
	- tk
	"""
	utils.hedonic_simulate("ual_rrh.yaml", residential_units, 
									unit_aggregations, "unit_residential_rent")

	# Clipping to match the other hedonic (set cap rate as desired)
	if "rsh_simulate" in settings:
		cap_rate = 0.05	 # sale price * cap rate = annual rent
		low = float(settings["rsh_simulate"]["low"]) * cap_rate / 12
		high = float(settings["rsh_simulate"]["high"]) * cap_rate / 12
		residential_units.update_col("unit_residential_rent",
							 residential_units.unit_residential_rent.clip(low, high))
		print "Clipped rrh_simulate produces\n", \
			residential_units.unit_residential_rent.describe()
	return


@orca.step('ual_households_relocation')
def ual_households_relocation(households):
	"""
	This model step randomly assigns households for relocation, using probabilities
	that depend on their tenure status.
	
	Data expectations
	-----------------
	- 'households' table has following columns:
		- 'hownrent' (int in range [1,2], non-missing)
		- 'unit_id' (int, '-1'-filled, corresponds to index of 'residential_units' table
		
	Results
	-------
	- assigns households for relocation by setting their 'unit_id' to -1
	"""
	rates = pd.DataFrame.from_dict({
		'hownrent': [1, 2],
		'probability_of_relocating': [0.15, 0.25]})
		
	print "Total agents: %d" % len(households)
	_print_number_unplaced(households, 'unit_id')

	print "Assigning for relocation..."
	m = RelocationModel(rates, rate_column='probability_of_relocating')
	mover_ids = m.find_movers(households.to_frame(['unit_id','hownrent']))
	households.update_col_from_series('unit_id', pd.Series(-1, index=mover_ids))
	
	_print_number_unplaced(households, 'unit_id')
	return


def _print_number_unplaced(df, fieldname):
	print "Total currently unplaced: %d" % \
		  df[fieldname].value_counts().get(-1, 0)


@orca.step('ual_households_transition')
def households_transition(households, household_controls, year, settings):
	"""
	This model step differs from the MTC version only in setting 'unit_id' rather than
	'building_id' to -1 for new households.
	"""
	s = orca.get_table('households').base_income_quartile.value_counts()
	print "Distribution by income before:\n", (s/s.sum())
	ret = utils.full_transition(households,
								household_controls,
								year,
								settings['households_transition'],
								"unit_id")
	s = orca.get_table('households').base_income_quartile.value_counts()
	print "Distribution by income after:\n", (s/s.sum())
	return ret


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




# Maybe allocate low-income people first for both the rental and owner models

# Add an initialization step to figure out which units are restricted?


# After building new housing, can we allocate a tenure after the fact by comparing the
# rent vs price? Isn't this the same thing the developer model would do? Maybe this would
# be biased toward ownership buildings though



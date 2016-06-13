from urbansim.utils import misc
import os
import sys
import orca
import yaml
import datasources
import variables
from utils import parcel_id_to_geom_id, add_buildings
from urbansim.utils import networks
import pandana.network as pdna
from urbansim_defaults import models
from urbansim_defaults import utils
from urbansim.developer import sqftproforma, developer
from urbansim.developer.developer import Developer as dev
import subsidies
import summaries
import numpy as np
import pandas as pd


""" UAL MODEL STEPS """

from urbansim.models.relocation import RelocationModel

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




""" END UAL MODEL STEPS """


@orca.step('rsh_simulate')
def rsh_simulate(buildings, aggregations, settings):
	utils.hedonic_simulate("rsh.yaml", buildings, aggregations,
						   "residential_price")
	if "rsh_simulate" in settings:
		low = float(settings["rsh_simulate"]["low"])
		high = float(settings["rsh_simulate"]["high"])
		buildings.update_col("residential_price",
							 buildings.residential_price.clip(low, high))
		print "Clipped rsh_simulate produces\n", \
			buildings.residential_price.describe()


@orca.step('hlcm_simulate')
def hlcm_simulate(households, buildings, aggregations, settings, low_income):

	fname = misc.config("hlcm.yaml")

	print "\nAffordable housing HLCM:\n"

	cfg = yaml.load(open(fname))
	cfg["choosers_predict_filters"] = "income <= %d" % low_income
	open(misc.config("hlcm_tmp.yaml"), "w").write(yaml.dump(cfg))

	# low income into affordable units
	utils.lcm_simulate("hlcm_tmp.yaml", households, buildings,
					   aggregations,
					   "building_id", "residential_units",
					   "vacant_affordable_units",
					   settings.get("enable_supply_correction", None))

	os.remove(misc.config("hlcm_tmp.yaml"))

	print "\nMarket rate housing HLCM:\n"

	# then everyone into market rate units
	utils.lcm_simulate("hlcm.yaml", households, buildings,
					   aggregations,
					   "building_id", "residential_units",
					   "vacant_market_rate_units",
					   settings.get("enable_supply_correction", None))


@orca.step('households_transition')
def households_transition(households, household_controls, year, settings):
	s = orca.get_table('households').base_income_quartile.value_counts()
	print "Distribution by income before:\n", (s/s.sum())
	ret = utils.full_transition(households,
								household_controls,
								year,
								settings['households_transition'],
								"building_id")
	s = orca.get_table('households').base_income_quartile.value_counts()
	print "Distribution by income after:\n", (s/s.sum())
	return ret


@orca.step('households_relocation')
def households_relocation(households, settings, years_per_iter):
	rate = settings['rates']['households_relocation']
	rate = min(rate * years_per_iter, 1.0)
	return utils.simple_relocation(households, rate, "building_id")


@orca.step('jobs_relocation')
def jobs_relocation(jobs, settings, years_per_iter):
	rate = settings['rates']['jobs_relocation']
	rate = min(rate * years_per_iter, 1.0)
	return utils.simple_relocation(jobs, rate, "building_id")


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time
@orca.step("scheduled_development_events")
def scheduled_development_events(buildings, development_projects,
								 demolish_events, summary, year, parcels,
								 settings, years_per_iter, parcels_geography,
								 building_sqft_per_job, vmt_fee_categories):

	# first demolish
	demolish = demolish_events.to_frame().\
		query("%d <= year_built < %d" % (year, year + years_per_iter))
	print "Demolishing/building %d buildings" % len(demolish)
	l1 = len(buildings)
	buildings = utils._remove_developed_buildings(
		buildings.to_frame(buildings.local_columns),
		demolish,
		unplace_agents=["households", "jobs"])
	orca.add_table("buildings", buildings)
	buildings = orca.get_table("buildings")
	print "Demolished %d buildings" % (l1 - len(buildings))
	print "	   (this number is smaller when parcel has no existing buildings)"

	# then build
	dps = development_projects.to_frame().\
		query("%d <= year_built < %d" % (year, year + years_per_iter))

	if len(dps) == 0:
		return

	new_buildings = utils.scheduled_development_events(
		buildings, dps,
		remove_developed_buildings=False,
		unplace_agents=['households', 'jobs'])
	new_buildings["form"] = new_buildings.building_type_id.map(
		settings['building_type_map']).str.lower()
	new_buildings["job_spaces"] = new_buildings.building_sqft / \
		new_buildings.building_type_id.fillna(-1).map(building_sqft_per_job)
	new_buildings["job_spaces"] = new_buildings.job_spaces.astype('int')
	new_buildings["geom_id"] = parcel_id_to_geom_id(new_buildings.parcel_id)
	new_buildings["SDEM"] = True
	new_buildings["subsidized"] = False

	new_buildings["zone_id"] = misc.reindex(
		parcels.zone_id, new_buildings.parcel_id)
	new_buildings["vmt_res_cat"] = misc.reindex(
		vmt_fee_categories.res_cat, new_buildings.zone_id)
	del new_buildings["zone_id"]
	new_buildings["pda"] = parcels_geography.pda_id.loc[
		new_buildings.parcel_id].values

	summary.add_parcel_output(new_buildings)


@orca.injectable("supply_and_demand_multiplier_func", autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
	s = demand / supply
	settings = orca.get_injectable('settings')
	print "Number of submarkets where demand exceeds supply:", len(s[s > 1.0])
	# print "Raw relationship of supply and demand\n", s.describe()
	supply_correction = settings["enable_supply_correction"]
	clip_change_high = supply_correction["kwargs"]["clip_change_high"]
	t = s
	t -= 1.0
	t = t / t.max() * (clip_change_high-1)
	t += 1.0
	s.loc[s > 1.0] = t.loc[s > 1.0]
	# print "Shifters for current iteration\n", s.describe()
	return s, (s <= 1.0).all()


# this if the function for mapping a specific building that we build to a
# specific building type
@orca.injectable("form_to_btype_func", autocall=False)
def form_to_btype_func(building):
	settings = orca.get_injectable('settings')
	form = building.form
	dua = building.residential_units / (building.parcel_size / 43560.0)
	# precise mapping of form to building type for residential
	if form is None or form == "residential":
		if dua < 16:
			return 1
		elif dua < 32:
			return 2
		return 3
	return settings["form_to_btype"][form][0]


@orca.injectable("add_extra_columns_func", autocall=False)
def add_extra_columns(df):
	for col in ["residential_price", "non_residential_price"]:
		df[col] = 0

	if "deed_restricted_units" not in df.columns:
		df["deed_restricted_units"] = 0
	else:
		print "Number of deed restricted units built = %d" %\
			df.deed_restricted_units.sum()

	df["redfin_sale_year"] = 2012

	if "residential_units" not in df:
		df["residential_units"] = 0

	if "parcel_size" not in df:
		df["parcel_size"] = \
			orca.get_table("parcels").parcel_size.loc[df.parcel_id]

	if "year" in orca.orca._INJECTABLES and "year_built" not in df:
		df["year_built"] = orca.get_injectable("year")

	if "form_to_btype_func" in orca.orca._INJECTABLES and \
			"building_type_id" not in df:
		form_to_btype_func = orca.get_injectable("form_to_btype_func")
		df["building_type_id"] = df.apply(form_to_btype_func, axis=1)

	return df


@orca.step('alt_feasibility')
def alt_feasibility(parcels, settings,
					parcel_sales_price_sqft_func,
					parcel_is_allowed_func):
	kwargs = settings['feasibility']
	config = sqftproforma.SqFtProFormaConfig()
	config.parking_rates["office"] = 1.5
	config.parking_rates["retail"] = 1.5

	utils.run_feasibility(parcels,
						  parcel_sales_price_sqft_func,
						  parcel_is_allowed_func,
						  config=config,
						  **kwargs)

	f = subsidies.policy_modifications_of_profit(
		orca.get_table('feasibility').to_frame(),
		parcels)

	orca.add_table("feasibility", f)


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year,
						  settings, summary, form_to_btype_func,
						  add_extra_columns_func, parcels_geography,
						  limits_settings, final_year):

	kwargs = settings['residential_developer']

	num_units = dev.compute_units_to_build(
		len(households),
		buildings["residential_units"].sum(),
		kwargs['target_vacancy'])

	targets = []
	typ = "Residential"
	# now apply limits - limits are assumed to be yearly, apply to an
	# entire jurisdiction and be in terms of residential_units or job_spaces
	if typ in limits_settings:

		juris_name = parcels_geography.juris_name.\
			reindex(parcels.index).fillna('Other')

		juris_list = limits_settings[typ].keys()
		for juris, limit in limits_settings[typ].items():

			# the actual target is the limit times the number of years run
			# so far in the simulation (plus this year), minus the amount
			# built in previous years - in other words, you get rollover
			# and development is lumpy

			current_total = parcels.total_residential_units[
				(juris_name == juris) & (parcels.newest_building >= 2010)]\
				.sum()

			target = (year - 2010 + 1) * limit - current_total
			# make sure we don't overshoot the total development of the limit
			# for the horizon year - for instance, in Half Moon Bay we have
			# a very low limit and a single development in a far out year can
			# easily build over the limit for the total simulation
			max_target = (final_year - 2010 + 1) * limit - current_total

			if target <= 0:
					continue

			targets.append((juris_name == juris, target, max_target, juris))
			num_units -= target

		# other cities not in the targets get the remaining target
		targets.append((~juris_name.isin(juris_list), num_units, None, "none"))

	else:
		# otherwise use all parcels with total number of units
		targets.append((parcels.index == parcels.index,
						num_units, None, "none"))

	for parcel_mask, target, final_target, juris in targets:

		print "Running developer for %s with target of %d" % \
			(str(juris), target)

		# this was a fairly heinous bug - have to get the building wrapper
		# again because the buildings df gets modified by the run_developer
		# method below
		buildings = orca.get_table('buildings')

		new_buildings = utils.run_developer(
			"residential",
			households,
			buildings,
			"residential_units",
			parcels.parcel_size[parcel_mask],
			parcels.ave_sqft_per_unit[parcel_mask],
			parcels.total_residential_units[parcel_mask],
			feasibility,
			year=year,
			form_to_btype_callback=form_to_btype_func,
			add_more_columns_callback=add_extra_columns_func,
			num_units_to_build=target,
			**kwargs)

		buildings = orca.get_table('buildings')

		if new_buildings is not None:
			new_buildings["subsidized"] = False

		if final_target is not None:
			# make sure we don't overbuild the target for the whole simulation
			overshoot = new_buildings.net_units.sum() - max_target

			if overshoot > 0:
				index = new_buildings.tail(1).index[0]
				index = int(index)
				buildings.local.loc[index, "residential_units"] -= overshoot

		summary.add_parcel_output(new_buildings)


@orca.step()
def retail_developer(jobs, buildings, parcels, nodes, feasibility,
					 settings, summary, add_extra_columns_func, net):

	dev_settings = settings['non_residential_developer']
	all_units = dev.compute_units_to_build(
		len(jobs),
		buildings.job_spaces.sum(),
		dev_settings['kwargs']['target_vacancy'])

	target = all_units * float(dev_settings['type_splits']["Retail"])
	# target here is in sqft
	target *= settings["building_sqft_per_job"][10]

	feasibility = feasibility.to_frame().loc[:, "retail"]
	feasibility = feasibility.dropna(subset=["max_profit"])

	feasibility["non_residential_sqft"] = \
		feasibility.non_residential_sqft.astype("int")

	feasibility["retail_ratio"] = parcels.retail_ratio
	feasibility = feasibility.reset_index()

	# create features
	f1 = feasibility.retail_ratio / feasibility.retail_ratio.max()
	f2 = feasibility.max_profit / feasibility.max_profit.max()

	# combine features in probability function - it's like combining expense
	# of building the building with the market in the neighborhood
	p = f1 * 1.5 + f2
	p = p.clip(lower=1.0/len(p)/10)

	print "Attempting to build {:,} retail sqft".format(target)

	# order by weighted random sample
	feasibility = feasibility.sample(frac=1.0, weights=p)

	bldgs = buildings.to_frame(buildings.local_columns + ["general_type"])

	devs = []

	for dev_id, d in feasibility.iterrows():

		if target <= 0:
			break

		# any special logic to filter these devs?

		# remove new dev sqft from target
		target -= d.non_residential_sqft

		# add redeveloped sqft to target
		filt = "general_type == 'Retail' and parcel_id == %d" % \
			d["parcel_id"]
		target += bldgs.query(filt).non_residential_sqft.sum()

		devs.append(d)

	if len(devs) == 0:
		return

	# record keeping - add extra columns to match building dataframe
	# add the buidings and demolish old buildings, and add to debug output
	devs = pd.DataFrame(devs, columns=feasibility.columns)

	print "Building {:,} retail sqft in {:,} projects".format(
		devs.non_residential_sqft.sum(), len(devs))
	if target > 0:
		print "	  WARNING: retail target not met"

	devs["form"] = "retail"
	devs = add_extra_columns_func(devs)

	add_buildings(buildings, devs)

	summary.add_parcel_output(devs)


@orca.step()
def office_developer(feasibility, jobs, buildings, parcels, year,
					 settings, summary, form_to_btype_func, scenario,
					 add_extra_columns_func, parcels_geography,
					 limits_settings):

	dev_settings = settings['non_residential_developer']

	# I'm going to try a new way of computing this because the math the other
	# way is simply too hard.  Basically we used to try and apportion sectors
	# into the demand for office, retail, and industrial, but there's just so
	# much dirtyness to the data, for instance 15% of jobs are in residential
	# buildings, and 15% in other buildings, it's just hard to know how much
	# to build, we I think the right thing to do is to compute the number of
	# job spaces that are required overall, and then to apportion that new dev
	# into the three non-res types with a single set of coefficients
	all_units = dev.compute_units_to_build(
		len(jobs),
		buildings.job_spaces.sum(),
		dev_settings['kwargs']['target_vacancy'])

	print "Total units to build = %d" % all_units
	if all_units <= 0:
		return

	for typ in ["Office"]:

		print "\nRunning for type: ", typ

		num_units = all_units * float(dev_settings['type_splits'][typ])

		targets = []
		# now apply limits - limits are assumed to be yearly, apply to an
		# entire jurisdiction and be in terms of residential_units or
		# job_spaces
		if year > 2015 and typ in limits_settings:

			juris_name = parcels_geography.juris_name.\
				reindex(parcels.index).fillna('Other')

			juris_list = limits_settings[typ].keys()
			for juris, limit in limits_settings[typ].items():

				# the actual target is the limit times the number of years run
				# so far in the simulation (plus this year), minus the amount
				# built in previous years - in other words, you get rollover
				# and development is lumpy

				current_total = parcels.total_job_spaces[
					(juris_name == juris) & (parcels.newest_building > 2015)]\
					.sum()

				target = (year - 2015 + 1) * limit - current_total

				if target <= 0:
					print "Already met target for juris = %s" % juris
					print "	   target = %d, current_total = %d" %\
						(target, current_total)
					continue

				targets.append((juris_name == juris, target, juris))
				num_units -= target

			# other cities not in the targets get the remaining target
			targets.append((~juris_name.isin(juris_list), num_units, "none"))

		else:
			# otherwise use all parcels with total number of units
			targets.append((parcels.index == parcels.index, num_units, "none"))

		for parcel_mask, target, juris in targets:

			print "Running developer for %s with target of %d" % \
				(str(juris), target)
			print "Parcels in play:\n", pd.Series(parcel_mask).value_counts()

			# this was a fairly heinous bug - have to get the building wrapper
			# again because the buildings df gets modified by the run_developer
			# method below
			buildings = orca.get_table('buildings')

			new_buildings = utils.run_developer(
				typ.lower(),
				jobs,
				buildings,
				"job_spaces",
				parcels.parcel_size[parcel_mask],
				parcels.ave_sqft_per_unit[parcel_mask],
				parcels.total_job_spaces[parcel_mask],
				feasibility,
				year=year,
				form_to_btype_callback=form_to_btype_func,
				add_more_columns_callback=add_extra_columns_func,
				residential=False,
				num_units_to_build=target,
				**dev_settings['kwargs'])

			if new_buildings is not None:
				new_buildings["subsidized"] = False

			summary.add_parcel_output(new_buildings)


@orca.step()
def developer_reprocess(buildings, year, years_per_iter, jobs,
						parcels, summary, parcel_is_allowed_func):
	# this takes new units that come out of the developer, both subsidized
	# and non-subsidized and reprocesses them as required - please read
	# comments to see what this means in detail

	# 20% of base year buildings which are "residential" have job spaces - I
	# mean, there is a ratio of job spaces to res units in residential
	# buildings of 1 to 5 - this ratio should be kept for future year
	# buildings
	s = buildings.general_type == "Residential"
	res_units = buildings.residential_units[s].sum()
	job_spaces = buildings.job_spaces[s].sum()

	to_add = res_units * .20 - job_spaces
	if to_add > 0:
		print "Adding %d job_spaces" % to_add
		res_units = buildings.residential_units[s]
		# bias selection of places to put job spaces based on res units
		add_indexes = np.random.choice(res_units.index.values, size=to_add,
									   replace=True,
									   p=(res_units/res_units.sum()))
		# collect same indexes
		add_indexes = pd.Series(add_indexes).value_counts()
		# this is sqft per job for residential bldgs
		add_sizes = add_indexes * 400
		print "Job spaces in res before adjustment: ", \
			buildings.job_spaces[s].sum()
		buildings.local.loc[add_sizes.index,
							"non_residential_sqft"] += add_sizes.values
		print "Job spaces in res after adjustment: ",\
			buildings.job_spaces[s].sum()

	# the second step here is to add retail to buildings that are greater than
	# X stories tall - presumably this is a ground floor retail policy
	old_buildings = buildings.to_frame(buildings.local_columns)
	new_buildings = old_buildings.query(
	   '%d == year_built and stories >= 4' % year)

	print "Attempting to add ground floor retail to %d devs" % \
		len(new_buildings)
	retail = parcel_is_allowed_func("retail")
	new_buildings = new_buildings[retail.loc[new_buildings.parcel_id].values]
	print "Disallowing dev on these parcels:"
	print "	   %d devs left after retail disallowed" % len(new_buildings)

	# this is the key point - make these new buildings' nonres sqft equal
	# to one story of the new buildings
	new_buildings.non_residential_sqft = new_buildings.building_sqft / \
		new_buildings.stories * .8

	new_buildings["residential_units"] = 0
	new_buildings["residential_sqft"] = 0
	new_buildings["building_sqft"] = new_buildings.non_residential_sqft
	new_buildings["stories"] = 1
	new_buildings["building_type_id"] = 10

	# this is a fairly arbitrary rule, but we're only adding ground floor
	# retail in areas that are underserved right now - this is defined as
	# the location where the retail ratio (ratio of income to retail sqft)
	# is greater than the median
	ratio = parcels.retail_ratio.loc[new_buildings.parcel_id]
	new_buildings = new_buildings[ratio.values > ratio.median()]

	print "Adding %d sqft of ground floor retail in %d locations" % \
		(new_buildings.non_residential_sqft.sum(), len(new_buildings))

	all_buildings = dev.merge(old_buildings, new_buildings)
	orca.add_table("buildings", all_buildings)

	new_buildings["form"] = "retail"
	# this is sqft per job for retail use - this is all rather
	# ad-hoc so I'm hard-coding
	new_buildings["job_spaces"] = \
		(new_buildings.non_residential_sqft / 445.0).astype('int')
	new_buildings["net_units"] = new_buildings.job_spaces
	summary.add_parcel_output(new_buildings)

	# got to get the frame again because we just added rows
	buildings = orca.get_table('buildings')
	buildings_df = buildings.to_frame(
		['year_built', 'building_sqft', 'general_type'])
	sqft_by_gtype = buildings_df.query('year_built >= %d' % year).\
		groupby('general_type').building_sqft.sum()
	print "New square feet by general type in millions:\n",\
		sqft_by_gtype / 1000000.0


def make_network(name, weight_col, max_distance):
	st = pd.HDFStore(os.path.join(misc.data_dir(), name), "r")
	nodes, edges = st.nodes, st.edges
	net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
					   edges[[weight_col]])
	net.precompute(max_distance)
	return net


def make_network_from_settings(settings):
	return make_network(
		settings["name"],
		settings.get("weight_col", "weight"),
		settings['max_distance']
	)


@orca.injectable('net', cache=True)
def build_networks(settings):
	nets = {}
	pdna.reserve_num_graphs(len(settings["build_networks"]))

	# yeah, starting to hardcode stuff, not great, but can only
	# do nearest queries on the first graph I initialize due to crummy
	# limitation in pandana
	for key in settings["build_networks"].keys():
		nets[key] = make_network_from_settings(
			settings['build_networks'][key]
		)

	return nets


@orca.step('local_pois')
def local_pois(settings):
	# because of the aforementioned limit of one netowrk at a time for the
	# POIS, as well as the large amount of memory used, this is now a
	# preprocessing step
	n = make_network(
		settings['build_networks']['walk']['name'],
		"weight", 3000)

	n.init_pois(
		num_categories=1,
		max_dist=3000,
		max_pois=1)

	cols = {}

	locations = pd.read_csv(os.path.join(misc.data_dir(), 'bart_stations.csv'))
	n.set_pois("tmp", locations.lng, locations.lat)
	cols["bartdist"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

	locname = 'pacheights'
	locs = orca.get_table('landmarks').local.query("name == '%s'" % locname)
	n.set_pois("tmp", locs.lng, locs.lat)
	cols["pacheights"] = n.nearest_pois(3000, "tmp", num_pois=1)[1]

	df = pd.DataFrame(cols)
	df.index.name = "node_id"
	df.to_csv('local_poi_distances.csv')


@orca.step('neighborhood_vars')
def neighborhood_vars(net):
	nodes = networks.from_yaml(net["walk"], "neighborhood_vars.yaml")
	nodes = nodes.replace(-np.inf, np.nan)
	nodes = nodes.replace(np.inf, np.nan)
	nodes = nodes.fillna(0)

	# nodes2 = pd.read_csv('data/local_poi_distances.csv', index_col="node_id")
	# nodes = pd.concat([nodes, nodes2], axis=1)

	print nodes.describe()
	orca.add_table("nodes", nodes)


@orca.step('regional_vars')
def regional_vars(net):
	nodes = networks.from_yaml(net["drive"], "regional_vars.yaml")
	nodes = nodes.fillna(0)

	nodes2 = pd.read_csv('data/regional_poi_distances.csv',
						 index_col="tmnode_id")
	nodes = pd.concat([nodes, nodes2], axis=1)

	print nodes.describe()
	orca.add_table("tmnodes", nodes)


@orca.step('regional_pois')
def regional_pois(settings, landmarks):
	# because of the aforementioned limit of one netowrk at a time for the
	# POIS, as well as the large amount of memory used, this is now a
	# preprocessing step
	n = make_network(
		settings['build_networks']['drive']['name'],
		"CTIMEV", 75)

	n.init_pois(
		num_categories=1,
		max_dist=75,
		max_pois=1)

	cols = {}
	for locname in ["embarcadero", "stanford", "pacheights"]:
		locs = landmarks.local.query("name == '%s'" % locname)
		n.set_pois("tmp", locs.lng, locs.lat)
		cols[locname] = n.nearest_pois(75, "tmp", num_pois=1)[1]

	df = pd.DataFrame(cols)
	print df.describe()
	df.index.name = "tmnode_id"
	df.to_csv('regional_poi_distances.csv')


@orca.step('price_vars')
def price_vars(net):
	nodes2 = networks.from_yaml(net["walk"], "price_vars.yaml")
	nodes2 = nodes2.fillna(0)
	print nodes2.describe()
	nodes = orca.get_table('nodes')
	nodes = nodes.to_frame().join(nodes2)
	orca.add_table("nodes", nodes)


# this is not really simulation - just writing a method to get average
# appreciation per zone over the past X number of years
@orca.step("mls_appreciation")
def mls_appreciation(homesales, year, summary):
	buildings = homesales

	years = buildings.redfin_sale_year
	zone_ids = buildings.zone_id
	price = buildings.redfin_sale_price

	# minimum observations
	min_obs = 10

	def aggregate_year(year):
		mask = years == year
		s = price[mask].groupby(zone_ids[mask]).median()
		size = price[mask].groupby(zone_ids[mask]).size()
		return s, size

	current, current_size = aggregate_year(2013)

	zones = pd.DataFrame(index=zone_ids.unique())

	past, past_size = aggregate_year(year)
	appreciation = (current / past).pow(1.0/(2013-year))
	# zero out the zones with too few observations
	appreciation = appreciation * (current_size > min_obs).astype('int')
	appreciation = appreciation * (past_size > min_obs).astype('int')
	zones["appreciation"] = appreciation

	print zones.describe()

	summary.add_zone_output(zones, "appreciation", year)
	summary.write_zone_output()

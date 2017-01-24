### abag_targets.csv

These are the households and jobs expected per pda and non-pda jurisdiction as agreed upon using ABAG's local knowledge.  We use this to compare against UrbanSim output.

### additional_units.csv

Additional units is a way to hard code the addition of units by city in order to force capacity.  It's a similar model to the "refiner" models common for UrbanSim praxis, but doesn't actually affect household/job totals, just the supply side.

### bart_stations.csv

This is a list of BART stations and their locations so that distance to BART can be used in the modeling.

### baseyear_taz_controls.csv

This contains our expected base year control totals by taz - number of units, vacancy rates, employment by sector (but not households).

### census_id_to_name.csv

For some reason the current parcels_geography file uses census id instead of name.  This is the file that maps those ids to names so we can use it within UrbanSim.

### deed_restricted_zone_totals.csv	

This is the number of deed restricted units per taz.  We don't have a great idea of exactly where those units are, but we have an aggregate idea, so we usually do a random assignment within that taz.

### development_projects.csv	

This is the list of projects that have happened since the base data, or that we think will happen in the future.  It is the development pipeline.  This file tends to have more attributes than we actually use in UrbanSim.

### employment_controls.csv

The number of jobs by sector for 5 year increments over the duration of the Plan.

### employment_relocation_rates.csv

Relocation rates by sector by zone so we can e.g. leave City Hall populated by civic sector jobs. 

### household_building_id_overrides.csv

This is used to move households around to match new city household totals.  When we have a new assignment process and a new assignment this file should go away.  (We needed to update household locations for a small number of households very late in the planning process.)

### household_controls.csv

Number of households by category (quartile) for each year over the duration of the Plan.

### juris_assumptions.csv

These are simulation assumptions that apply per jurisdiction.  Currently, the only attribute used is `minimum_forecast_retail_jobs_per_household` which is used to keep local serving retail appropriate for each jurisdiction.

### juris_controls.csv

This is the revised set of household numbers which we needed to match late in the planning process.  A few small cities correctly alerted us to the fact that our TAZ data had the wrong population in some cases (in a few cases the census was even petitioned to be changed).

### landmarks.csv

The lat-lng locations of a few major landmarks in the region, which are avialable for use in the modeling.

### logsums.csv

This is the set of baseyear logsums from the MTC travel model.  This is available for use in the modeling and is a key compoenent of the integrated modeling effort.

### manual_edits.csv

A csv of manual edits so we don't have to create a new H5 every time we want to edit a single parcel or building.  Simply give the table name, attribute name, and new value, and BAUS will override current data using edits form this file.

### parcel_egs.csv



### pdatargets.csv


### regional_controls.csv

### regional_poi_distances.csv

### rhna_by_juris.csv

### superdistricts.csv

### taz_geography.csv

### taz_growth_rates_gov_ed.csv

### tpp_id_2016.csv

### tpp_id_2016.zip

### vmt_fee_zonecats.csv

### zone_forecast_inputs.csv

### zoning_lookup.csv

A few notes on the zoning_lookup, which hopefully makes this an evolving document.

The zoning lookup is the current, BASELINE zoning for each jurisdiction, and can be assigned to parcels.  The lookup is identified by the unique id ("id" column), has the city name, city id, and the name of the zoning.

The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development, so this means the most constraining constraint has the power.  dua is "Dwelling Units per Acre", far is "Floor Area Ratio" (ratio of square footage in building to square footage on parcel), and height is... height. 

This means there *must* be an equivlance between dua and far if they are not both present.  This is tricky, and requires knowing things like average unit sizes, as well as net to gross factors for the unit square footage.  We make our best guesses on this, but this logic can be configured.  There also must be an equivalance between far and height, which generally is made by taking far and multiplying is by a net to gross building footprint factor (e.g. a calculation to figure out the height of a building with FAR 4.0, when the footprint of the building takes up 80% or so of the parcel, when stories are all 12 feet, and where partial stories count as full stories in terms of height).  As you are thinking at this point, we are bad building architects, but we're making rough approximations here in order to allocate built space in the region and this feels like the appropriate level of detail.

The other columns are building types, and the values are 0 or 1 to indicate whether the type is allowed on this parcel.  For instance, HS is single family, HT is townhome (attached), and HM is multi-family (apartments / condos) and so forth.  R for retail, OF is office, and M is mixed.

max_du_per_parcel is not currently used because we don't necessarily trust our data, but in theory is used to say, e.g., that only a single unit can be built on each parcel.

Note that a blank value in the csv means that there is no constraint for that attribute and the other constraints will be used.  If there is no constraint for any of far, height, and dua, a building will NOT be built. 

*By convention a 0 in dua, far, and height is discouraged.  It is preferred to use the "building type not allowed" columns for this*

### zoning_mods_0.csv
zoning_mods_1.csv
zoning_mods_2.csv
zoning_mods_3.csv
zoning_mods_4.csv
zoning_mods_5.csv
zoning_table_city_lookup.csv

The inputs folder strucutre for Bay Area UrbanSim (BAUS) and a decription of each input. 

# inputs/

## accessibility/
### pandana/
- tmnet.h5: Travel model network information for calculating accessibility within the model using Pandana
- osm_bayarea4326.h5: Street netowrk information for calculating accessibility within the model using Pandana
- landmarks.csv: Locations of a few major landmarks in the region for accessibility calculations.
- regional_poi_distances.csv: The pre-computed distances from each travel model node to each landmark. 
- bart_stations.csv: A list of BART stations and their locations so that distance to BART can calculated.
- logsums.csv: The set of baseyear logsums from the MTC travel model. This is available for use in the modeling and is a key compoenent of the integrated modeling effort.

### travel_model/
- AccessibilityMarkets_YYY.csv: A travel model output file that incorportates travel model run logsums into the forecast, by year.
- mandatoryAccessibilities_YYY.csv: A travel model output file that incorportates travel model run logsums into the forecast, by year.
- nonMandatoryAccessibilities_YYY.csv: A travel model output file that incorportates travel model run logsums into the forecast, by year.

## basis_inputs/ (under construction)
### crosswalks/
- parcel_to_maz22.csv: A lookup table from parcels to Travel Model Two MAZs.
- parcel_to_taz1454sub.csv: A lookup table from parcels to Travel Model One TAZs.
- parcels_geography.csv: A lookup table from parcels to jurisdiction, growth geographies, UGB areas, greenfield areas, and a concatenation of these used to join these geographies zoning_mods.csv, to apply zoning rules within them. 
- census_id_to_name.csv: Maps census id from parcels_geography to name so it can be used.
- maz_geography: A lookup between MAZ, TAZ2, and county.
- maz22_taz1454: A lookup between MAZ and TAZ1.
- superdistricts_geography.csv: A map of superdistrict numbers, names, and their subregion.
- taz_geography.csv: A lookup between TAZ1, supedisctrict, and county.
- tpp_id_2016.csv: Mapping of parcels to tpp_id. **MOVE THIS FILE**
### edits/
- manual_edits.csv: Overrides the current h5 data using the table name, attribute name, and new value, so we don't have to generate a new one each time.
- household_building_id_overrides.csv: Moves households to match new city household totals during the data preprocessing.  
### existing_policy/
- development_caps.yaml: Base year job cap policies in place in jurisdictions (TODO: remove the asserted development caps/k-factors entangled here.)
- inclusionary.yaml: Base year inclusionary zoning policies in place in jurisdictions (TODO: have all model runs inherit these, even if an inclusionary stratey is applied).
### hazards/
- slr_progression.csv: The sea level rise level, for each forecast year.
- slr_inundation.csv: The sea level rise level at which each inundation parcel becomes inundated, for each forecast year. Rows marked with "100" are parcels where sea level rise has been mitigated, either through planned projects or a plan strategy. **SEPARATE STRATEGY?**
### parcels_buildings-agents/
- bayarea_v3.h5: Base year database of households, jobs, buildings, and parcels. The data is pre-processed in pre-processing.py.
- costar.csv: Commercial data from CoStar, including non-residential price to inform the price model.
- development_projects.csv: The list of projects that have happened since the base data, or buildings in the development pipeline.  This file tends to have more attributes than we use in the model.
- deed_restricted_zone_totals.csv: An approximate number of deed restricted units per TAZ to assign randomly within the TAZ.  
- baseyear_taz_controls.csv: Base year control totals by TAZ, to use for checking and refining inputs. The file includes number of units, vacancy rates, and employment by sector (TODO: add households).
- sfbay_craisglist.csv: Craigslist data to inform rental unit information and model tenure.
### zoning/ 
- zoning_parcels.csv: A lookup table from parcels to zoning_id, zoning area information, and a "nodev" flag (currently all set to 0).
- zoning_lookup.csv: The existing zoning for each jurisdiction, assigned to parcels with the "id" field. Fields include the city name, city id, and the name of the zoning. The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development.  

## plan_strategies/
- accessory_units.csv: A file to add accessory dwelling units to jurisdictions by year, simulating policy to allow or reduce barriers to ADU construction in jurisdictions (TODO: Make this a default policy).
- account_strategies.yaml: This files contains the settings for all strategies in a model run that use accounts. The file may include account settings (e.g., how much to spend, where to spend) for: housing development bonds, office development bonds, OBAG funds, and VMT fees (which collect fees and then can spend the subsidies). 
- development_caps_strategy.yaml: A file that specifies a strategy to limit development (generally office development) to a certain number of residential units and/or job spaces.
- inclusionary_strategy.yaml: A file to apply an inclusionary zoning strategy by geography and inclusionary housing requirement percentage.
- preservation.yaml: A file to apply an affordable housing preservation strategy through specifying geography and target number of units for preservation. 
- profit_adjustment_stratgies.yaml: This file contains the settings for all strategies in a model run which modify the profitability of projects thus altering their feasibility. The file may include profit adjustment settings (e.g., the percent change to profit) for: development streamlining, CEQA streamlining, parking requirements reductions, and profitability changes from SB-743.
- renter_protections_relocation_rates_overwrites: The rows in this file overwrite the household relocation rates in the model's settings.
- telecommute_sqft_per_job_adjusters: These are multipliers which adjust the sqft per job setting by superdistrict by year to represent changes from a telework strategy.
- vmt_fee_zonecats.csv: This file pairs with the VMT Fee and SB-743 strategies. It provides VMT levels by TAZ1, which map to the corresponding price adjustments in the strategies.
- zoning_mods.csv: A file which allows you to upzone or downzone. If you enter a value in "dua_up" or "far_up", the model will apply that as the new zoning or maintain the existing zoning if it is higher. If you enter a value in "dua_down" or "far_down", the model will apply that as the zoning or maintain the existing zoning if it is lower. UGBs are also controlled using this file, using zoning changes to enforce them. This file is mapped to parcels using the field "zoningmodcat", which is the concatenated field of growth designations in parcels_geography.csv. **SEPARATE UGB?**

## regional_controls/
- regional_controls.csv: Controls from the regional forecast which give us employed residents and the age distribution by year, used to forecast variables used by the travel model.
- employment_controls.csv: The total number of jobs in the region for the model to allocate, by year. The controls are provided by 6-sector job category.
- household_controls.csv: The total number of households in the region for the model to allocate, by year. The controls are provided by household income quartile.

## zone_forecasts
- tm1_taz1_forecast_inputs.csv: This is closely related to regional_controls.csv. These are zone level inputs used for the process of generating variables for the travel model, while the other file contains regional-level controls. These inputs provide TAZ1454 information, used for Travel Model One summaries. 
- tm2_taz2_forecast_inputs.csv: The same as above, except these inputs provide TAZ2 information, usED for Travel Model Two summaries. 
- tm1_tm2_maz_forecast_inputs.csv: The same as above, except these inputs provide MAZ information, used for btoh Travel Model One and Travel Model Two summaries. 
- taz_growth_rates_gov_ed.csv: Ratios of gov't and education employment per population (sometimes at the TAZ level, sometimes for each county, and sometimes regionally). This csv actually has two header rows - this first row is what the outcome attribute is, and the second is the geography at which the ratio acts (taz / county / regional).
prportional_retail_jobs_forecast.csv: This contains the field "minimum_forecast_retail_jobs_per_household" by jurisdiction, which is used to keep local numbers of retail jobs reasonable through the forecast.



- tm2_emp26_employment_forecast: The forecasted share of jobs in each of the employment sectors used by Travel Model Two. The shares are provided by county and by year. 
- tm1_tm2_regional_demographic_forecast: Similarly to regional_controls.csv, this file provides regional-level information to produce travel model variables, in this case using forecasts of shares by year.


# bayarea_urbansim/

## configs/
### accessibility/
- accessibility_settings.yaml:
- neighborhood_vars.yaml:
- price_vars.yaml:
- regional_vars.yaml:
### adjusters/
- cost_shifters.yaml:
- development_caps_asserted.yaml:
- employment_relocation_rates_overwrites.csv: Relocation rates by sector by zone so we can e.g. leave City Hall populated by civic sector jobs.
- sqft_per_job_adjusters: Adjusts the number of sqft used by each job (resulting in how many jobs can occupy a building) by superdistrict. Can be used to set vacancy rates. 
- zoning_adjusters.yaml:


- juris_controls.csv: This is the revised set of household numbers which we needed to match late in the planning process. A few small cities correctly alerted us to the fact that our TAZ data had the wrong population in some cases (in a few cases the census was even petitioned to be changed).
- additional_units.csv: Hard codes the addition of units by city in order to force capacity.  It's a similar model to the "refiner" models common for UrbanSim praxis, but doesn't actually affect household/job totals, just the supply side.



### settings.yaml
Contains general settings that get passed to the model steps. This includes settings for things like rates (e.g. relocation rates, cap rates), kwargs (e.g. residential developer specifications), and value settings (e.g. building sqft per job, ave sqft per unit clip). These settings are removed from the model logic for clarity and ease in modifying.
### policy.yaml
This contains the settings for the various policy features in Bay Area UrbanSim. More on these policies is found on the [Github Pages for this repository](http://bayareametro.github.io/bayarea_urbansim/pba2040/).
### inputs.yaml
For several data inputs the model uses a flexible file selection. These allow the user to specify how input files enter the model.
### mapping.yaml
This is the location to find all variable mapping and lookups.
### hazards.yaml
These are settings that pertain to the Sea Level Rise and Earthquake model. They enable these features as well as control their specfications and policies.
### The inputs structure for Bay Area UrbanSim (BAUS) and a decription of each input. Model input files are run specific: data inputs and model levers. These are stored in an `inputs` folder to be called by the model. Model configs are longer-term| model estimation constants, assumptions we do not expect to change. These live in the model's repository under the `configs` folder. 
&nbsp;  
# inputs
## accessibility
### pandana
**name**|**use**
-----|-----|
tmnet.h5| Travel model network information for calculating accessibility within the model using Pandana
osm_bayarea4326.h5| Street network information for calculating accessibility within the model using Pandana
landmarks.csv| Locations of a few major landmarks in the region for accessibility calculations.
regional_poi_distances.csv| The pre-computed distances from each travel model node to each landmark. 
bart_stations.csv| A list of BART stations and their locations so that distance to BART can calculated.
logsums.csv| A set of base year logsums from the travel model. 
### travel_model
**name**|**use**
-----|-----|
AccessibilityMarkets_YYY.csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
mandatoryAccessibilities_YYY.csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
nonMandatoryAccessibilities_YYY.csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.  
&nbsp;
## basis_inputs (under construction)
### crosswalks
**name**|**use**
-----|-----|
parcel_to_maz22.csv| A lookup table from parcels to Travel Model Two MAZs.
parcel_to_taz1454sub.csv| A lookup table from parcels to Travel Model One TAZs.
parcels_geography.csv| A lookup table from parcels to jurisdiction, growth geographies, UGB areas, greenfield areas, and a concatenation of these used to join these geographies zoning_mods.csv, to apply zoning rules within them. 
census_id_to_name.csv| Maps census id from parcels_geography to name so it can be used.
maz_geography| A lookup between MAZ, TAZ2, and county.
maz22_taz1454| A lookup between MAZ and TAZ1.
superdistricts_geography.csv| A map of superdistrict numbers, names, and their subregion.
taz_geography.csv| A lookup between TAZ1, supedisctrict, and county.
### edits
**name**|**use**
-----|-----|
data_edits.yaml| Settings for editing the input data in the model code, e.g. clipping values. 
manual_edits.csv| Overrides the current h5 data using the table name, attribute name, and new value, so we don't have to generate a new one each time.
household_building_id_overrides.csv| Moves households to match new city household totals during the data preprocessing.
tpp_id_2016.csv| Updates tpp_ids after changes were made to the ids.  
### existing_policy
**name**|**use**
-----|-----|
development_caps.yaml| Base year job cap policies in place in jurisdictions (TODO: remove the asserted development capsk-factors entangled here.)
inclusionary.yaml| Base year inclusionary zoning policies in place in jurisdictions (TODO: have all model runs inherit these, even if an inclusionary stratey is applied).
### hazards
**name**|**use**
-----|-----|
slr_progression.csv| The sea level rise level, for each forecast year.
slr_inundation.csv| The sea level rise level at which each inundation parcel becomes inundated, for each forecast year. Rows marked with "100" are parcels where sea level rise has been mitigated, either through planned projects or a plan strategy.
### parcels_buildings-agents
**name**|**use**
-----|-----|
bayarea_v3.h5| Base year database of households, jobs, buildings, and parcels. The data is pre-processed in pre-processing.py.
costar.csv| Commercial data from CoStar, including non-residential price to inform the price model.
development_projects.csv| The list of projects that have happened since the base data, or buildings in the development pipeline.  This file tends to have more attributes than we use in the model.
deed_restricted_zone_totals.csv| An approximate number of deed restricted units per TAZ to assign randomly within the TAZ.  
baseyear_taz_controls.csv| Base year control totals by TAZ, to use for checking and refining inputs. The file includes number of units, vacancy rates, and employment by sector (TODO: add households).
sfbay_craisglist.csv| Craigslist data to inform rental unit information and model tenure.
### zoning
**name**|**use**
-----|-----|
zoning_parcels.csv| A lookup table from parcels to zoning_id, zoning area information, and a "nodev" flag (currently all set to 0).
zoning_lookup.csv| The existing zoning for each jurisdiction, assigned to parcels with the "id" field. Fields include the city name, city id, and the name of the zoning. The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development.  
&nbsp;
## plan_strategies
**name**|**use**
-----|-----|
accessory_units.csv| A file to add accessory dwelling units to jurisdictions by year, simulating policy to allow or reduce barriers to ADU construction in jurisdictions (TODO: Make this a default policy).
account_strategies.yaml| This files contains the settings for all strategies in a model run that use accounts. The file may include account settings (e.g., how much to spend, where to spend) for| housing development bonds, office development bonds, OBAG funds, and VMT fees (which collect fees and then can spend the subsidies). 
development_caps_strategy.yaml| A file that specifies a strategy to limit development (generally office development) to a certain number of residential units andor job spaces.
inclusionary_strategy.yaml| A file to apply an inclusionary zoning strategy by geography and inclusionary housing requirement percentage.
preservation.yaml| A file to apply an affordable housing preservation strategy through specifying geography and target number of units for preservation. 
profit_adjustment_stratgies.yaml| This file contains the settings for all strategies in a model run which modify the profitability of projects thus altering their feasibility. The file may include profit adjustment settings (e.g., the percent change to profit) for| development streamlining, CEQA streamlining, parking requirements reductions, and profitability changes from SB-743.
renter_protections_relocation_rates_overwrites| The rows in this file overwrite the household relocation rates in the model's settings.
telecommute_sqft_per_job_adjusters| These are multipliers which adjust the sqft per job setting by superdistrict by year to represent changes from a telework strategy. (TODO: Disentangle the k-factors and the policy application within this file and sqft_per_job_adjusters.csv. In the meantime, use both files as is done in the PBA50 No Project).
vmt_fee_zonecats.csv| This file pairs with the VMT Fee and SB-743 strategies. It provides VMT levels by TAZ1, which map to the corresponding price adjustments in the strategies.
zoning_mods.csv| A file which allows you to upzone or downzone. If you enter a value in "dua_up" or "far_up", the model will apply that as the new zoning or maintain the existing zoning if it is higher. If you enter a value in "dua_down" or "far_down", the model will apply that as the zoning or maintain the existing zoning if it is lower. UGBs are also controlled using this file, using zoning changes to enforce them. This file is mapped to parcels using the field "zoningmodcat", which is the concatenated field of growth designations in parcels_geography.csv.
&nbsp;  
## regional_controls 
**name**|**use**
-----|-----|
employment_controls.csv| The total number of jobs in the region for the model to allocate, by year. The controls are provided by 6-sector job category.
household_controls.csv| The total number of households in the region for the model to allocate, by year. The controls are provided by household income quartile.
&nbsp;
## zone_forecasts
**name**|**use**
-----|-----|
taz_growth_rates_gov_ed.csv| This file has ratios of governement and education employment per population by County and TAZ. The files has two header rows| the first row is what the outcome attribute is and the second is the geography at which the ratio acts (either TAZ, County, or Regional).
prportional_retail_jobs_forecast.csv| This contains the field "minimum_forecast_retail_jobs_per_household" by jurisdiction, which is used to keep local numbers of retail jobs reasonable through the forecast.
tm1_taz1_forecast_inputs.csv| This is closely related to regional_controls.csv. These are zone level inputs used for the process of generating variables for the travel model, while the other file contains regional-level controls. These inputs provide TAZ1454 information, used for Travel Model One summaries. 
tm2_taz2_forecast_inputs.csv| The same as above, except these inputs provide TAZ2 information, usED for Travel Model Two summaries.
tm1_tm2_maz_forecast_inputs.csv| The same as above, except these inputs provide MAZ information, used for btoh Travel Model One and Travel Model Two summaries. 
tm2_emp27_employment_shares| The forecasted share of jobs by 26 sectors, used to apportion that 6 sectors used in the model into more detailed categories Travel Model Two. The shares are provided by county and by year.
tm2_occupation_shares| The forecasted share of jobs by occupation, used for Travel Model Two. The shares are provided by county and by year.
tm1_tm2_regional_controls.csv| Controls from the regional forecast which give us employed residents and the age distribution by year, used to forecast variables used by the travel model.
tm1_tm2_regional_demographic_forecast| Similar to regional_controls.csv, this file provides regional-level information to produce travel model variables, in this case using forecasts of shares by year.
&nbsp;  
# bayarea_urbansim
## configs
### adjusters
**name**|**use**
-----|-----|
cost_shifters.yaml| Multipliers to cost, currently specified by county, used to calibrate the model.
development_caps_asserted.yaml| Caps on development, either residential or office, used to calibrate the model. (TODO: remove any base year existing policy caps entangled here).
employment_relocation_rates_overwrites.csv| These overwrite the relocation rates in employment_relocation_rates.csv to calibrate the model, e.g. leave government sector jobs in San Francisco City Hall's TAZ.
sqft_per_job_adjusters| Multipliers to the number of sqft used by each job, defined in the model's developer settings, which modify the number of jobs that can occupy a building. This is used to calibrate the model, e.g. reflect CBD job densities or adjust vacancy rates by superdistrict. The inputs file telecommute_sqft_per_job_adjusters.csv uses alternative multipliers for for the forecast years in place of these, if the strategy is enabled. (TODO: Disentangle the k-factors and the policy application in these two files. In the meantime, use both files as is done in the PBA50 No Project).
zoning_adjusters.yaml| Adjusters used to modify the model's zoning data.
&nbsp;   
### accessibility
**name**|**use**
-----|-----|
accessibility_settings.yaml| Settings for Pandana, the model's endogenous accessibility calculations.
neighborhood_vars.yaml| Settings for calculating local accessibility variables during the model run.
regional_vars.yaml| Settings for calculating regional accessibility variables during the model run.
price_vars.yaml| Settings for calculating local accessibility variables on price during the model run.
&nbsp;
### developer
**name**|**use**
-----|-----|
developer_settings.yaml| Settings for the model's developer and feasibility models.
residential_vacancy_rates.csv| Residential vacancy rates for the residential developer model, separated from the main developer settings into this file to allow them to vary by year.
&nbsp;
### hedonics
**name**|**use**
-----|-----|
price_settings.yaml| Settings for the model's price simulation and supplydemand equilibration of price.
nrh.yaml| Non-residential hedonic price model specification.
rrh.yaml| Residential rent hedonic price model specification.
rsh.yaml| Residential sales hedonic price model specification.
&nbsp;
### location_choice
**name**|**use**
-----|-----|
elcm.yaml| Employment location choice model specification, segemented by six employment sectors.
hlcm_owner.yaml| Household location choice model specification segmented by income quartiles. The models are estimated for owner households.
hlcm_owner_lowincome.yaml| This uses the same specification and estimated coefficients as hlcm_owner. The only difference is that it is used to only low income households to choose deed-restricted owner units.
hlcm_owner_lowincome_no_unplaced.yaml| This uses the same specification and estimated coefficients as hlcm_owner, but allows owners of all incomes into deed-restricted owner units to cover any gaps in assignment.
hlcm_owner_no_unplaced.yaml| This uses the same specification and estimated coefficients as hlcm_owner, but does another round of placements of owners, this time into non-deed-restricted owner units, to cover any gaps in assignment.
hlcm_renter.yaml| Household location choice model specification segmented by income quartiles. The models are estimated for rental households.
hlcm_renter_lowincome.yaml| This uses the same specification and estimated coefficients as hlcm_renter. The only difference is that it is used to only low income households to choose deed-restricted rental units.
hlcm_renter_lowincome_no_unplaced.yaml| This uses the same specification and estimated coefficients as hlcm_renter, but allows renters of all incomes into deed-restricted rental units to cover any gaps in assignment.
hlcm_renter_no_unplaced.yaml| This uses the same specification and estimated coefficients as hlcm_renter, but does another round of placement of renters, this time into non-deed-restricted rental units, to cover any gaps in assignment.
### location choice cont.
**HLCM Model**|**Estimation Choosers: Filters**|**Estimation Alternatives: Filters**|**Simulation Choosers: Filters**|**Simulation Alternatives: Filters**
-----|-----|-----|-----|-----|
owner|Owners|-|Owners|Owner Units|
owner_lowincome|Owners|-|Low-Income Owners| Affordable Owner Units|
owner_lowincome_no_unplaced|-|-|Owners|Affordable Owner Units|
owner_no_unplaced|Owners|-|Owners| Market-Rate Owner Units|
renter|Renters|-|Renters|Renters Units|
renter_lowincome|Renters|-|Low-Income Renters|Affordable Rental Units|
renter_lowincome_no_unplaced|-|-|Renters|Affordable Rentual Units|
renter_no_unplaced|Renters|-|Renters|Market Rate Rental Units|
&nbsp;
### transition_relocation
**name**|**use**
-----|-----|
employment_relocation_rates.csv| A file with the probability of a job relocating during a time step in the forecast, by TAZ and by employment sector. Pairs with employment_relocation_rates.csv which overwrites the model probabilities with calibration factors.
household_relocation_rates.csv| A file with the probability of a household relocating during a time step in the forecast, by TAZ, income, and tenure. Pairs with renter_protections_relocation_rates_overwrites.csv which overwrites model probabilities with different relocation rates when the renter protections strategy is enabled. 
transition_relocation_settings.yaml| Settings for the transition and relocation models. 
&nbsp;
### mapping.yaml
Mapping used in the model to relate variables to one another.
&nbsp;
### paths.yaml
Variables that store file names for use in the model code.
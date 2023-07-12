### The configuration file structure for Bay Area UrbanSim (BAUS) and a description of each file. Model configurations files are stored in a `configs` folder in the model repository. They specify model settings such as model estimation constants and assumptions.
 
# bayarea_urbansim
## configs
### adjusters
**name**|**description**
-----|-----
cost_shifters.yaml| Multipliers to cost, currently specified by county, used to calibrate the model.
development_caps_asserted.yaml| Caps on development, either residential or office, used to calibrate the model. (TODO: remove any base year existing policy caps entangled here).
employment_relocation_rates_overwrites.csv| These overwrite the relocation rates in employment_relocation_rates.csv to calibrate the model, e.g. leave government sector jobs in San Francisco City Hall's TAZ.
sqft_per_job_adjusters| Multipliers to the number of sqft used by each job, defined in the model's developer settings, which modify the number of jobs that can occupy a building. This is used to calibrate the model, e.g. reflect CBD job densities or adjust vacancy rates by superdistrict. The inputs file telecommute_sqft_per_job_adjusters.csv uses alternative multipliers for for the forecast years in place of these, if the strategy is enabled. (TODO: Disentangle the k-factors and the policy application in these two files. In the meantime, use both files as is done in the PBA50 No Project).
zoning_adjusters.yaml| Adjusters used to modify the model's zoning data.
   
### accessibility
**name**|**description**
-----|-----
accessibility_settings.yaml| Settings for Pandana, the model's endogenous accessibility calculations.
neighborhood_vars.yaml| Settings for calculating local accessibility variables during the model run.
regional_vars.yaml| Settings for calculating regional accessibility variables during the model run.
price_vars.yaml| Settings for calculating local accessibility variables on price during the model run.

### developer
**name**|**description**
-----|-----
developer_settings.yaml| Settings for the model's developer and feasibility models.
residential_vacancy_rates.csv| Residential vacancy rates for the residential developer model, separated from the main developer settings into this file to allow them to vary by year.

### hedonics
**name**|**description**
-----|-----
price_settings.yaml| Settings for the model's price simulation and supplydemand equilibration of price.
nrh.yaml| Non-residential hedonic price model specification.
rrh.yaml| Residential rent hedonic price model specification.
rsh.yaml| Residential sales hedonic price model specification.

### location_choice
**name**|**description**
-----|-----
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
-----|-----|-----|-----|-----
owner|Owners|-|Owners|Owner Units|
owner_lowincome|Owners|-|Low-Income Owners| Affordable Owner Units|
owner_lowincome_no_unplaced|-|-|Owners|Affordable Owner Units|
owner_no_unplaced|Owners|-|Owners| Market-Rate Owner Units|
renter|Renters|-|Renters|Renters Units|
renter_lowincome|Renters|-|Low-Income Renters|Affordable Rental Units|
renter_lowincome_no_unplaced|-|-|Renters|Affordable Rentual Units|
renter_no_unplaced|Renters|-|Renters|Market Rate Rental Units|

### transition_relocation
**name**|**description**
-----|-----
employment_relocation_rates.csv| A file with the probability of a job relocating during a time step in the forecast, by TAZ and by employment sector. Pairs with employment_relocation_rates.csv which overwrites the model probabilities with calibration factors.
household_relocation_rates.csv| A file with the probability of a household relocating during a time step in the forecast, by TAZ, income, and tenure. Pairs with renter_protections_relocation_rates_overwrites.csv which overwrites model probabilities with different relocation rates when the renter protections strategy is enabled. 
transition_relocation_settings.yaml| Settings for the transition and relocation models. 

### mapping.yaml
Mapping used in the model to relate variables to one another.

### paths.yaml
Variables that store file names for use in the model code.
# Plan Bay Area 2050

Plan Bay Area 2050 is the most recent implementation of Bay Area UrbanSim. For all official Plan documents please see the [Plan Bay Area 2050 project website](https://www.planbayarea.org/)

## Scenarios

#### Horizon
#### Draft Blueprint
#### Final Blueprint
#### No Project
#### EIR Alternative 1
#### EIR Alternative 2


## Strategy Coding Technical Notes

#### Deed-Restricted Units notes
* Deed-restricted units can now come from many sources. As of Final Blueprint, those sources are:
    * DR = base year DR + pipeline DR + public lands (added via pipeline) + subsidized (production) + preserved + inclusionary
* Pipeline units cannot be redeveloped, other deed-restricted units can if or when the building is more than 20 years old. 
* In the household location choice models, only Q1 households can enter DR units. However, if the model can't find enough Q1 households for the DR units (due to modeling reasons or over-supply) non-Q1 households are able to enter. 

#### Affordable Housing Fund- Preservation (Final Blueprint)
* Selects buildings to spend preservation funding on, by marking them as deed restricted units. The selection is not tied to price, and housing cost is calculated off model. 
    * The buildings are randomly selected based on the total number of buildings to preserve within a geography which are set [here](https://github.com/BayAreaMetro/bayarea_urbansim/blob/develop/configs/policy.yaml#L2026). 
    * The "deed restricted" column is updated in the residential units table, which is used to filter units in the household location choice models and assign Q1 households to deed restricted units. The "deed restricted units" and "preserved units" columns are updated in the buildings table. [These occur here](https://github.com/BayAreaMetro/bayarea_urbansim/blob/develop/baus/subsidies.py#L126).
* In some cases, a Q1 household will already occupy a newly preserved unit. If that household moves, only another Q1 will be able to move in. In other cases, a non-Q1 household will occupy a newly preserved unit, and a Q1 household is only able to enter if and when that household moves out. 

#### Inclusionary Zoning (Draft Blueprint, Final Blueprint)
* Inclusionary zoning requirement (x% of new housing development have to be affordable) is set in [policy.yalm](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/configs/policy.yaml#L146).
    * The [default setting](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/configs/policy.yaml#L150) represents the existing requirements without plan strategy interventions.
    * Plan strategies are organized by scenario, with a [scenario handler](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/configs/policy.yaml#L18) at the top.
    * Inclusionary rate is set at certain geographic level: 
        * PBA40 and Horizon: inclusionary percentage by [jurisdiction](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/configs/policy.yaml#L233). 
        * Draft Blueprint: inclusionary percentage by [pba50chcat](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/configs/policy.yaml#L1136) (i.e. the combination of several growth geography strategies, GG, TRA, PPA, etc.). 
        * Final Blueprint: inclusionary percentage by [fbpchcat](https://github.com/BayAreaMetro/bayarea_urbansim/pull/236/files#diff-0b50f0cbfb63780ba72d551f45920497R1429).
* Coding notes:
    * [datasources.py](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/datasources.py#L134) reads inclusionary strategy input and maps it to parcels using the corresponding field: PBA40 and Horizon uses ['jurisdiction'](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/datasources.py#L155), Draft Blueprint uses ['pba50chcat'](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/datasources.py#L145), Final Blueprint uses 'fbpchcat'.
    * In [subsidies.py](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/subsidies.py), the [*inclusionary_housing_revenue_reduction*](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/subsidies.py#L89) function calculates [median household AMI](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/subsidies.py#L95), [feasible new affordable housing count and revenue_reduction amount](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/subsidies.py#L144) of each inclusionary geography. In PBA40 and Horizon, these calculations were conducted at the jurisdiction level. PBA50 uses strategy geographies instead - 'pba50chcat' in Draft Blueprint and 'fbpchcat' in Final Blueprint.
    * The inclusionary statements in [summaries.py](https://github.com/BayAreaMetro/bayarea_urbansim/blob/347fea11576a1f448f4056ffb143c4c4e4aadaa4/baus/summaries.py#L195) should be consistent with the inclusionary geography of each plan scenario.

#### Reduce cost for housing development (Draft Blueprint, Final Blueprint)
* One way to (indirectly) subsidize housing is to reduce housing development cost reduction, for example, SB743 CEQA reform, lowering parking requirements, etc. This is defined in [profitability_adjustment_policies](https://github.com/BayAreaMetro/bayarea_urbansim/blob/3ecf457e3cf3661992a3a3c5dba126fe1b33db8a/configs/policy.yaml#L1246). The policies are scenario-based, as noted by "enable_in_scenarios". For each policy, [*profitabiity_adjustment_formula*](https://github.com/BayAreaMetro/bayarea_urbansim/blob/98f3b65ea4f29c1f2766659432e8a9d825eed56c/configs/policy.yaml#L1295) picks the parcels in a certain category (e.g. a certain type of geography) and then decreases the required profitability level needed for the model to build on those parcels, e.g. multiplying by 2.5% or 0.025 means to LOWER the required profit level by 2.5%. When a policy has an alternative version is different scenarios, may use 'alternative_geography_scenarios' and 'alternative_adjustment_formula' to consolidate the scenarios.
* ["Summaries.py"](https://github.com/BayAreaMetro/bayarea_urbansim/blob/98f3b65ea4f29c1f2766659432e8a9d825eed56c/baus/summaries.py#L175) summaries these policies.

#### Affordable Housing Fund Lump-sum Account (Draft Blueprint, Final Blueprint)
* [Lump-sum accounts](https://github.com/BayAreaMetro/bayarea_urbansim/blob/54fe0d50d6f0d133ed3b8000abe0bdf3a955fd81/configs/policy.yaml#L1138) represent direct housing subsidies. 
    * Each county has a `lump-sum account` to hold all the available affordable housing funding for that county. BAUS assumes a constant [annual funding amount (an independent input)](https://github.com/BayAreaMetro/bayarea_urbansim/blob/54fe0d50d6f0d133ed3b8000abe0bdf3a955fd81/configs/policy.yaml#L1235), for each county during the plan period, doesn't consider inflation or fluctuations in funding availability over time. In each simulation iteration, funding available in each county's account equals the [annual amount multiplies by years per iteration (5 years in BAUS)](https://github.com/BayAreaMetro/bayarea_urbansim/blob/54fe0d50d6f0d133ed3b8000abe0bdf3a955fd81/baus/subsidies.py#L62). 
    * Residential development projects that are not feasible under market conditions are potentially qualified for subsidy. Final Blueprint requires the projects to be also located within the Growth Geography. A qualified project draws money from the corresponding account to fill the feasibility gap. Not all qualified projects will be subsidized.
    * Lum-sum accounts are [scenario-based](https://github.com/BayAreaMetro/bayarea_urbansim/blob/54fe0d50d6f0d133ed3b8000abe0bdf3a955fd81/baus/subsidies.py#L39), having scenario-specific fund amount and project qualification criteria.

* Coding notes:
    * Set up the [account](https://github.com/BayAreaMetro/bayarea_urbansim/blob/03c7f27f0edf75edbba3ab663c9947fa7c1ef67a/baus/subsidies.py#L153) in policy.yaml
    * Calculate each account's subsidy amount for each iteration and add it to [coffer](https://github.com/BayAreaMetro/bayarea_urbansim/blob/03c7f27f0edf75edbba3ab663c9947fa7c1ef67a/baus/subsidies.py#L45)
    * Set up the filter in [`run_subsidized_developer()`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/03c7f27f0edf75edbba3ab663c9947fa7c1ef67a/baus/subsidies.py#L859)
    * Update the config in ['summaries.py'](https://github.com/BayAreaMetro/bayarea_urbansim/blob/54fe0d50d6f0d133ed3b8000abe0bdf3a955fd81/baus/summaries.py#L271)
    * Check [`subsidized_residential_developer_lump_sum_accts()`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/03c7f27f0edf75edbba3ab663c9947fa7c1ef67a/baus/subsidies.py#L1128) 
and make sure it is included in the model list in `baus.py`

#### VMT Fees / Transportation Impact Fees (Draft Blueprint)
* Apply fees on new commercial or residential development that reflects transportation impacts associated with such development, focusing primarily on new commercial spaces or residential units anticipated to have high employment-related or residence-related vehicle miles traveled (VMT). The fees could be set at county, jurisdiction, or TAZ level, usually on a $/sqft basis for commercial development and $/unit basis for residential development. Draft Blueprint applies VMT on new office development based on the county and associated VMT per worker associated with the TAZ to incentivize development inside low-VMT job centers.
* This [diagram](https://mtcdrive.app.box.com/file/678131785447) illustrates the modeling steps in BAUS:
    * BAUS has three types of VMT fees - "com_for_res" (apply fees on commercial development to subsidize residential development), "res_for_res" (apply fees on residential development to subsidize residential development), and "com_for_com" (apply fees on commercial development to subsidize commercial development). 
    * Each parcel is assigned a "vmt_res_cat" value and a "vmt_nonres_cat" value based on its categorized VMT-level. This is then mapped to the [fee table](https://mtcdrive.box.com/s/bh3hqzab817lu2m87gxr4o5l6dt39lep) to decide the fee amount for new residential and commercial development on the parcel.
    * During each model iteration period (currently five years), [VMT fees collected from new development](https://github.com/BayAreaMetro/bayarea_urbansim/blob/680b9c3451013f7014becaf1de88b58053ff75b9/baus/subsidies.py#L282) go into one of the [two accounts](https://github.com/BayAreaMetro/bayarea_urbansim/blob/680b9c3451013f7014becaf1de88b58053ff75b9/baus/subsidies.py#L41) - "vmt_res_acct" and "vmt_com_acct" - for each geography (regional or sub-regional). Policies that aim to support/incentivize certain types of housing development or commercial activities can draw funding from respective account. In [Draft Blueprint](https://mtcdrive.box.com/s/bh3hqzab817lu2m87gxr4o5l6dt39lep), VMT fees revenue is not applied into any job/housing incentive, but is accumulated to help to understand how much revenue is raised to support other economy strategies.

#### Jobs-housing Balance Fee (Draft Blueprint)
* Apply a regional jobs-housing linkage fee to generate funding for affordable housing when new office development occurs in job-rich places, thereby incentivizing more jobs to locate in housing-rich places. The $/sqft fee assigned to each jurisdiction is a composite fee based on the jobs-housing ratio and jobs-housing fit for both cities and counties.
* Modeling jobs-housing fee in BAUS:
    * Jobs-housing fee is tracked in BAUS under the ["jobs_housing_com_for_res" account at the county level](https://github.com/BayAreaMetro/bayarea_urbansim/blob/622ddc296983e6d6110872913065fec0b2419193/baus/subsidies.py#L55), which is similar to the "com_for_res" account of the VMT strategy.
    * In each model interation period, jobs-housing fees [applies to new office development in each county](https://github.com/BayAreaMetro/bayarea_urbansim/blob/622ddc296983e6d6110872913065fec0b2419193/baus/subsidies.py#L453) based on the [$/sqft level of jurisdiction](https://github.com/BayAreaMetro/bayarea_urbansim/blob/622ddc296983e6d6110872913065fec0b2419193/configs/policy.yaml#L1854) where the development occurs. The fees collected goes to each county's account. 
    * The account then acts similarly to the county-level lump-sum account to [subsidize affordable housing](https://github.com/BayAreaMetro/bayarea_urbansim/blob/622ddc296983e6d6110872913065fec0b2419193/baus/subsidies.py#L917) in that county. 

#### Office Lump Sum Account (Final Blueprint)
* Similar to the residential funding lump sum account, office lump sum account provides funding to subsidize office development in targeted areas. 
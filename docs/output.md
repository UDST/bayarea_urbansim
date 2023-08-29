### BAUS Outputs 
The outputs of BAUS and a description of each file. Model output files are written to an `outputs` folder during a BAUS model run.

### outputs/
#### core summaries
**name**|**description**
-----|-----
parcel_summary_[year].csv | Development, households, and jobs on each parcel in a given year.
parcel_growth_summary.csv | Change in development, households, and jobs on each parcel between the model's base year and forecast year.
building_summary_[year].csv | Inventory of buildings in a given year, linked to the parcel they sit on.
new_buildings_summary.csv | Inventory of all buildings built during the simulation.
interim_zone_output_[year].csv | Interim model data at the TAZ level.
feasibility.csv | Parcel-level data on the development feasibilities of various development types given the zoning, development costs, and expected return. Only parcels where at least one development type is feasible is included. | 
 
#### geographic summaries
**name**|**description**
-----|-----
jurisdiction_summary_[year].csv | Jurisdiction-level summary of development, households, and jobs in a given year.
jurisdiction_summary_growth.csv | Jurisdiction-level change in development, households, and jobs between the model's base year and forecast year.
superdistrict_summary_[year].csv | Superdistrict-level summary of development, households, and jobs in a given year.
superdistrict_summary_growth.csv | Superdistrict-level change in development, households, and jobs between the model's base year and forecast year.
county_summary_[year].csv | County-level summary of development, households, and jobs in a given year.
county_summary_growth.csv | County-level change in development, households, and jobs between the model's base year and forecast year.
subregion_summary_[year].csv | Subregion-level change in development, households, and jobs in a given year.
subregion_summary_growth.csv | Subregion-level change in development, households, and jobs between the model's base year and forecast year.
region_summary_[year].csv | Regional summary of development, households, and jobs in a given year.
region_summary_growth.csv | Regional change in development, households, and jobs between the model's base year and forecast year.
 
#### travel model summaries
**name**|**description**
-----|-----
taz1_summary_[year].csv | TAZ1/TAZ1454-level summaries of development, households, jobs, demographics, and density attributes used for travel modeling.
taz1_summary_growth.csv | TAZ1/TAZ1454-level change in development, households, jobs, demographics, and density attributes used for travel modeling.
maz_marginals_[year].csv | MAZ-level summaries of households and demographics used to create the synthesized population for travel modeling.
maz_summary_[year].csv | MAZ-level summaries of development, households, jobs, and density attributes used for travel modeling.
maz_summary_growth.csv | MAZ-level change in development, households, jobs, and density attributes used for travel modeling.
taz2_marginals_[year].csv | TAZ2-level summaries of households and demographics used to create the synthesized population for travel modeling.
county_marginals_[year].csv | County-level summaries of demographics and jobs used to create the synthesized population for travel modeling.
region_marginals_[year].csv | Region-level summaries of demographics used to create the synthesized population for travel modeling.
 
#### affordable housing summaries
**name**|**description**
-----|-----
juris_dr_summary_[year].csv | Jurisdiction-level summary of deed-restricted units by type in a given year.
juris_dr_growth.csv | Jurisdiction-level change in deed-restricted units by type between the model's base year and forecast year.
superdistrict_dr_summary_[year].csv | Superdistrict-level summary of deed-restricted units by type in a given year.
superdistrict_dr_growth.csv | Superdistrict-level change in deed-restricted units by type between the model's base year and forecast year.
county_dr_summary_[growth].csv | County-level summary of deed-restricted units by type in a given year.
county_dr_growth | County-level change in deed-restricted units by type between the model's base year and forecast year.
region_dr_summary_[year].csv | Region-level summary of deed-restricted units by type in a given year.
region_dr_growth.csv | Region-level change in deed-restricted units by type between the model's base year and forecast year.
 
#### hazards summaries
**name**|**description**
-----|-----
slr_summary_[year].csv | Sea level rise impacted parcels, buildings, households, and jobs in a given year.
eq_codes_summary_[year].csv | Summary of earthquake codes assigned to buildings, in the earthquake year.
eq_fragilities_summary_[year].csv |  SUmmary of fragilities assigned to buildings, in the earthquake year.
slr_summary_[year].csv | Earthquake impacted parcels, buildings, households, and jobs in the earthquake year.
eq_demolish_buildings_[year].csv | Inventory of buildings impacted by earthquake, by TAZ for the resilience team.
eq_demolish_buildings_[year].csv | Inventory of buildings retrofit for earthquake, by TAZ for the resilience team.
eq_buildings_list_[year].csv | Inventory of buildings in key earthquake years, by TAZ for the resilience team.

#### metrics
**name**|**description**
-----|-----
growth_geog_summary_[year].csv | Households and jobs in growth geographies and combinations of growth geographies, by year.
growth_geog_growth_summary_[year].csv | Change in households and jobs in growth geographies and combinations of growth geographies between the model's base year and forecast year.
dr_units_metrics.csv | Change in deed-restricted units by HRA and COC.
household_income_metrics_[year].csv | Low income households by growth geography, by year.
equity_metrics.csv | Change in low income households in Displacement tracts and COC tracts.
jobs_housing_metrics.csv | Jobs-Housing ratios by county, by year.
jobs_metrics.csv | Change in PPA and manufacturing jobs.
slr_metrics.csv | Sea level rise affected and protected total households, low-income households and COC households.
earthquake_metrics.csv | Total housing units retrofit and total retrofit cost, for all units and for COC units. Earthquake affected and protected total households, low-income households, and COC households.
greenfield_metric.csv | Change in annual greenfield development acres.








### `interim_zone_output.csv` zone-level diagnostic attributes:

| **Attribute** | **Description** | **Data Type** | **Source** | **Sub-model/step** |
|-----------------|-----------|--------------|--------------|--------------|

| unit_residential_price | median residential price of all residential units in a TAZ | float | BAUS "residential_units" table | [rsh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L745) || unit_residential_rent | median residential monthly rent of all residential units in a TAZ | float | BAUS "residential_units" table | [rrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L764) |
| unit_residential_price_>_rent | 1 or 0 representing if the TAZ-level per-unit residential price is higher than annualized rent divided by [cap_rate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/configs/developer/developer_settings.yaml#L2). | int | BAUS "residential_units" table and "developer_settings" | [summary.py](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/summaries.py#L265) |
| residential_price | median per sq.ft. residential price of all residential buildings in a TAZ (based on general building type); currently used only for estimation, not for simulation | float | "buildings" table  | [residential_price()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L248) |
| zoned_du | maximum number of dwelling units allowed on all parcels in a TAZ | int | BAUS "parcels" table | [zoned_du()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L957) |
| zoned_du_underbuild | additional dwelling units allowed on all parcels in a TAZ given existing development conditions, 0 if the additional units are not at least half of existing development | int | BAUS "parcels" table | [zoned_du_underbuild()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L1037) 
| zoned_du_underbuild_ratio | ratio of additional allowable residential units to maximum allowable residential units | int | BAUS "zones" table | [zoned_du_build_ratio()]() |
| building_count | total number of residential buildings in a TAZ, based on general building type | int | BAUS "buildings" table | XXX |
| residential_units | total number of residential units in a TAZ | int | BAUS "buildings" table | XXX |
| ave_unit_sqft | 0.6 quantile of building-level average residential unit size of all builidings in a TAZ | int | BAUS "building" table | [ave_unit_sqft()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L943) |
| residential_vacancy | percentage of residential units in a TAZ that are not occupied by a household | float | BAUS "households" table, BAUS "buildings" table | BAUS [summary.py]() |
| non_residential_sqft | total sq.ft. of all non-residential buildings in a TAZ | int | BAUS "buildings" table | XXX |
| job_spaces | total number of jobs that can be accommodated by non-residential space in a TAZ | int | BAUS "building" table; sq.ft. per job setting | XXX |
| non_residential_vacancy | percentage of job_spaces in a TAZ that are not occupied by a job | float | BAUS "jobs" table, BAUS "buildings" table | XXX |
| average_income | median income of all households in a TAZ | int | BAUS "households" table; BAUS "households_preproc" table | [summary.py](); MTC/ABAG household models? |

### `parcel_output.csv` parcel-level attributes:

| **Attribute** | **Description** | **Data Type** | **Source** | **Sub-model/step** |
|-----------------|-----------|--------------|--------------|--------------|
| development_id | XXX | XXX | XXX | XXX |
| SDEM | XXX | XXX | XXX | XXX |
| building_type | XXX | XXX | XXX | XXX |
| residential | XXX | XXX | XXX | XXX |
| building_sqft | XXX | XXX | XXX | XXX |
| non_residential_sqft | XXX | XXX | XXX | XXX |
| job_spaces | XXX | XXX | XXX | XXX |
| residential_sqft | XXX | XXX | XXX | XXX |
| residential_units | XXX | XXX | XXX | XXX |
| total_residential_units | XXX | XXX | XXX | XXX |
| total_sqft | XXX | XXX | XXX | XXX |
| source | XXX | XXX | XXX | XXX |
| x | XXX | XXX | XXX | XXX |
| y | XXX | XXX | XXX | XXX |
| year_built | XXX | XXX | XXX | XXX |

### `fesibility` attributes
`feasibility` contains two sets of development variables grouped by six development types (coded as `form`): `retail`, `industrial`, `office`, `residential`, `mixedresidential`, `mixedoffice`. For every development type, one set of variables are passed through from the parcels table as input for the feasibility evaluation; the other set of variables are the result of the feasibility evaluation.

| **Attribute** | **Description** | **Data Type** | **Source** |
|-----------------|-----------|--------------|--------------|
| parcel_id | Parcel index | int |  |
| oldest_building | ? | ? | pass_through from input parcel frame |
| total_sqft | ? | ? | pass_through from input parcel frame |
| total_residential_units | ? | ? | pass_through from input parcel frame |
| max_far |  | int | pass_through from input parcel frame |
| max_dua |  | int | pass_through from input parcel frame |
| land_cost |  | int | pass_through from input parcel frame |
| residential |  |  | pass_through from input parcel frame |
| min_max_fars |  | int | pass_through from input parcel frame |
| max_height |  | int | pass_through from input parcel frame |
| building_purchase_price |  | int | pass_through from input parcel frame |
| building_purchase_price_sqft |  |  | pass_through from input parcel frame |
| residential_sales_price_sqft | https://app.asana.com/0/0/1201856230375726/f |  | pass_through from input parcel frame |
| pda_pba40 |  |  | pass_through from input parcel frame |
| pda_pba50  |  |  | pass_through from input parcel frame |
| trich_id |  | str | pass_through from input parcel frame |
| cat_id |  |  | pass_through from input parcel frame |
| tra_id |  |  | pass_through from input parcel frame |
| ppa_id |  |  | pass_through from input parcel frame |
| sesit_id |  |  | pass_through from input parcel frame |
| coc_id  |  |  | pass_through from input parcel frame |
| juris |  |  | pass_through from input parcel frame |
| county |  |  | pass_through from input parcel frame |
| superdistrict |  |  | pass_through from input parcel frame |
| geom_id |  |  | pass_through from input parcel frame |
| vmt_res_cat |  |  | pass_through from input parcel frame |
| vmt_nonres_cat |  |  | pass_through from input parcel frame |
| parking_config | parking type, e.g. deck, surface | string | created by run_feasibility() |
| building_sqft | The number of square feet for the building to build. Keep in mind this includes parking and common space. Will need a helpful function to convert from gross square feet to actual usable square feet in residential units. | float | created by run_feasibility() |
| building_cost | The cost of constructing the building as given by the ave_cost_per_sqft from the cost model (for this FAR) and the number of square feet. | float | created by run_feasibility() |
| parking_ratio | ? | ? | ? |
| stories | ? | ? | ? |
| total_cost | The cost of constructing the building plus the cost of acquisition of the current parcel/building. | float | created by run_feasibility() |
| building_revenue | The NPV of the revenue for the building to be built, which is the number of square feet times the yearly rent divided by the cap rate (with a few adjustment factors including building efficiency). | float | created by run_feasibility() |
| max_profit_far | The FAR of the maximum profit building (constrained by the max_far and max_height from the input dataframe). | float | created by run_feasibility() |
| max_profit | The profit for the maximum profit building (constrained by the max_far and max_height from the input dataframe). | float | created by run_feasibility() |
| residential_sqft |  | float | created by run_feasibility() |
| non_residential_sqft |  | float | created by run_feasibility() |
| fees |  |  | created by run_feasibility() |
| policy_based_revenue_reduction |  | int | created by run_feasibility() |
| deed_restricted_units |  | int | created by run_feasibility() |
| inclusionary_units |  | int | created by run_feasibility() |
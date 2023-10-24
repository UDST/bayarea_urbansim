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
feasibility.csv | Parcel-level data on the development feasibilities of various development types given the zoning, development costs, and expected return. Contains two sets of development variables grouped by six development types (coded as `form`): `retail`, `industrial`, `office`, `residential`, `mixedresidential`, `mixedoffice`. For every development type, one set of variables are passed through from the parcels table as input for the feasibility evaluation; the other set of variables are the result of the feasibility evaluation.Only parcels where at least one development type is feasible are included.

#### `interim_zone_output.csv`

**attribute** | **description** | **source**
-----|-----|-----
non_residential_sqft | total sq.ft. of all non-residential buildings in a TAZ | non-residential developer model
job_spaces | total number of jobs that can be accommodated by non-residential space in a TAZ | buildings table; sq.ft. per job settings
residential_units | total number of residential units in a TAZ | residential developer model
deed_restricted_units | total number of deed resitricuted units in a TAZ | data inputs, development pipeline, developer model, subsidized developer model, preservation model
preserved_units | total number of units preserved in a TAZ | preservation model
inclusionary_units | total number of inclusionary units built in market rate buildings in a TAZ | developer model
subsidized_units | total number of subsidized units built with bonds, fees, etc. | subsidized developer model
zoned_du | maximum number of dwelling units allowed on all parcels in a TAZ | [zoned_du()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L957)
zoned_du_underbuild | additional dwelling units allowed on all parcels in a TAZ given existing development conditions, 0 if the additional units are not at least half of existing development | [zoned_du_underbuild()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L1037) 
zoned_du_underbuild_ratio | ratio of additional allowable residential units to maximum allowable residential units |[zoned_du_build_ratio()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/variables.py#L1053)
unit_residential_price | median residential price of all residential units in a TAZ | [rsh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L745)
unit_residential_rent | median residential monthly rent of all residential units in a TAZ | [rrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L764)
non_residential_rent | median residential monthly rent of all residential units in a TAZ | [rrh_simulate()](https://github.com/BayAreaMetro/bayarea_urbansim/blob/900cfd8674be3569ae42cc0afb532ee12581188f/baus/ual.py#L708)
residential_vacancy | percentage of residential units in a TAZ that are not occupied by a household | summary calculations
non_residential_vacancy | percentage of job_spaces in a TAZ that are not occupied by a job | summary calculations

### `new_buildings_summary.csv`

**attribute** | **description** | **source**
-----|-----|-----
building_type | |
source | |
deed_restricted_units | |
subsidized_units | |
inclusionary_units | |
preserved_units | |
residential_units | |
total_residential_units | |
total_nonresidential_sqft | |
job_spaces | |
x | |
y | |
year_built | |

 
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
See data dictionary in the travel model repository [here](https://github.com/BayAreaMetro/modeling-website/wiki/TazData)
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
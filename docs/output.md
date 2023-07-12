### The outputs of Bay Area UrbanSim (BAUS) and a description of each file. Model output files are written to an `outputs` folder during a BAUS model run.
&nbsp;  
### core summaries
**name**|**description**
-----|-----
parcel_summary_[year].csv | Development, households, and jobs on each parcel in a given year.
parcel_growth_summary.csv | Change in development, households, and jobs on each parcel between the model's base year and forecast year.
building_summary_[year].csv | Inventory of buildings in a given year, linked to the parcel they sit on.
diagnostic_output.csv | Interim model data.
&nbsp; 
### geographic summaries
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
&nbsp; 
### travel model summaries
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
&nbsp; 
### affordable housing summaries
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
&nbsp; 
### hazards summaries
**name**|**description**
-----|-----
slr_summary_[year].csv | Sea level rise impacted parcels, buildings, households, and jobs in a given year.
eq_codes_summary_[year].csv | Summary of earthquake codes assigned to buildings, in the earthquake year.
eq_fragilities_summary_[year].csv |  SUmmary of fragilities assigned to buildings, in the earthquake year.
slr_summary_[year].csv | Earthquake impacted parcels, buildings, households, and jobs in the earthquake year.
eq_demolish_buildings_[year].csv | Inventory of buildings impacted by earthquake, by TAZ for the resilience team.
eq_demolish_buildings_[year].csv | Inventory of buildings retrofit for earthquake, by TAZ for the resilience team.
eq_buildings_list_[year].csv | Inventory of buildings in key earthquake years, by TAZ for the resilience team.
&nbsp;
### metrics
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
&nbsp;
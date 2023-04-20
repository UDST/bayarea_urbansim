# Model Overview

## Approach

A forecast with BAUS begins with a basemap. This is a detailed geodatabase containing all of the region’s buildings, households, employees, policies, and transport network for a recent year. This roughly corresponds to tooday’s conditions but is a few years in the past due to data collection lag and formalities related to the modeling framework. The buildings are largely an accurate collection of every structure gathered from assessor’s data, commercial read estate databases, and other sources. The households and employees are represented at the micro level but their characteristics are built through synthesis (i.e., we only have samples of this informations so we build a full representation that is consistent with these samples). Policies such as zoning and growth limits are collected for each jurisdiction and are binding unless they are explicitly changed for a forecast.

BAUS is used to forecast the future by advancing through repeated steps that chart out a potential pathway for future urban growth. In BAUS each step represents a five year period. Most steps repeat the same set of sub-steps. In UrbanSim, each of these sub-steps is called a “model”. The file used to run BAUS (baus.py) sets the number steps and the order in which the models are are run. The major types of model steps are:

* (Hazards): This optional model simulates the impact of earthquakes and sea level rise by destroying buildings and displacing their inhabitants.
* Calculate accessibility: Logsum accessibility measures are brought in from the Travel Model and pedestrian accessibility is calculated within UrbanSim
* Calculate housing prices and rents: Hedonic regression models are applied to current (to the model time step) conditions to reestimate current prices and rents
* HH/employee relcation and transition: Some households and employeees are selected to move; additiional households and employees are added or subtracted to reflect the exogenous control totals
* Add buildings from the Development Projects list: Buildings are forced into the model (as opposed to being explicitly modeled); these represent institutional plans (e.g., universities and hospitals), large approved projects (e.g., Treasure Island), and development that has occurred between the basemap year and the current year (i.e., constructed 2016-2020).
* Build new market-rate housing units and commercial space: The for-profit real estate development process is simulated and new buildings are built where they are most feasible; this also build deed-restricted unit that are produced through the market-rate process (e.g., inclusionary zoning).
* Build new deed-restricted affordable units: A similar not-for-profit real estate development process produces affordable housing units based on money available within BAUS Accounts (e.g., bond measures).
HH/employee location choices: Households and employees are assigned to new locations based on logistic regresssion models that capture the preferences of particular segments (e.g., lower income households, retail jobs).
* Produce summary tables: Numerous zonal summaries of the detailed geodatabaase are produced for analysis of urban change and for use in the Travel Model; this step includes additional post-processing to get the data ready for the Travel Model’s population synthesizer.

## Sub-Models

Typically, a set of BAUS inputs or assumptions are modified to produce a scenario. Policies such as zoning or fees can be modified. Control totals can be adjusted. Assumptions about preferences or financical situations can be adjusted. The package of changes is then simulated to forecast its impact on the future urban landscape and these outcomes are often enterered into the travel model to predict future year travel patterns and greenhouse gas emmissions.

## Application Types

BAUS is used for for three typical types of application:

* Forecasting: UrbanSim produces a complete map for each five year interval into the future. This information is required by statute and useful for planning processes.
* Policy Studies: BAUS can evaluate the impact of policy changes on, for example, the amount of housing produced, the amount of deed-restricted housing producded, and the locations of future residential or commercial development.
* Transport Project Evaluation: BAUS provides information on future land use patterns used to evaluate the benefits and costs of large transport infrastructure investments and allows an assessment of how such projects may influence future development.

## Model System

BAUS is the middle model in an interactive suite of three model systems maintained by MTC:

Regional economic and demographic information is supplied to BAUS from the REMI CGE model and related demographic processing scripts. BAUS outputs on housing production are used to adjust regional housing prices (and thus other variables) in REMI.
Accessibillity information is supplied to BAUS from the Travel Model and influences real estate prices and household and employee location choices. BAUS outputs on the location of various types of households and employees are used to establish origins and destinations in the Travel Model.




### An overview of baus.py
 
baus.py is a command line interface (cli) used to run Bay Area UrbanSim in various modes.  These modes currently include:

* estimation, which runs a series of models to save parameter estimates for all statistical models
* simulation, which runs all models to create a simulated regional growth forecast
* fetch_data, which downloads large data files from Amazon S3 as inputs for BAUS
* preprocessing, which performas long-running data cleaning steps and writes newly cleaned data back to the binary h5 file for use in the other steps
* baseyearsim which runs a "base year simulation" which summarizes the data before the simulation runs (during simulation, summaries are written after each year, so the first year's summaries are *after* the base year is finished - a base year simulation writes the summaries before any models have run)

### Urban Analytics Lab (UAL) Improvements

#### Data schemas

* Builds out the representation of individual housing units to include a semi-persistent tenure status, which is assigned based on characteristics of initial unit occupants
* Joins additional race/ethnicity PUMS variables to synthetic households [NB: currently missing from the reconciled model, but will be re-added]
* Adds a representation of market rents alongside market sale prices

#### Model steps

* Residential hedonics predict market rents and sale prices separately, with rents estimated from Craigslist listings
* Household move-out choice is conditional on tenure status
* Household location choice is modeled separately for renters and owners, and includes race/ethnicity measures as explanatory variables
* Developer models are updated to produce both rental and ownership housing stock

Notebooks, work history, code samples, etc are kept in a separate [bayarea_urbansim_work](https://github.com/ual/bayarea_urbansim_work) repository. 

#### Current status (August 2016)

* All of the UAL alterations have been refactored as modular orca steps
* This code is contained in `baus/ual.py`, `configs/ual_settings.yaml` and individual `yaml` files as needed for regression models that have been re-estimated
* There are *no* changes to `urbansim`, `urbansim_defaults`, or MTC's orca initialization and model steps
* MTC and UAL model steps can be mixed and matched by passing different lists to orca; see `run.py` for examples
* The UAL model steps document and test for required data characteristics, using the [orca_test](https://github.com/udst/orca_test) library

### Outputs from Simulation (written to the runs directory)

ALL OUTPUT IN THIS DIRECTORY IS NOT OFFICIAL OUTPUT. PLEASE CONTACT MTC FOR OFFICIAL OUTPUTS OF THE LAST PLAN BAY AREA.

`[num]` = a positive integer used to identify each successive run.  This number usually starts at 1 and increments each time baus.py is called.

Many files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run[num]\_topsheet\_[year].csv | An overall summary of various housing and employment outcomes summarized by very coarse geographies.
run[num]_parcel_output.csv 		| A csv of all new built space in the region, not including ADUs added to existing buildings.  This has a few thousand rows and dozens of columns which contain various inputs and outputs, as well as debugging information which helps explain why each development was picked by UrbanSim.
run[num]\_parcel_data\_[year].csv 			|A CSV with parcel level output for *all* parcels with lat, lng and includes change in total_residential_units and change in total_job_spaces, as well as zoned capacity measures.
run[num]\_building_data\_[year].csv 			|The same as above but for buildings.
run[num]\_taz\_summaries\_[year].csv 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)
run[num]\_pda_summaries\_[year].csv, run[num]\_juris_summaries\_[year].csv, run[num]\_superdistrict_summaries\_[year].csv | Similar outputs to the taz summaries but for each of these geographies.  Used for understanding the UrbanSim forecast at an aggregate level.
run[num]\_acctlog_[account name]\_[year].csv    |A series of CSVs of each account's funding amount and buildings developed under this acount (if the funding is used to subsidize development) in each iteration.
run[runnum]_dropped_buildings.csv     | A summary of buildings which were redeveloped during the simulated forecast.
run[runnum]_simulation_output.json | Used by the web output viewer.



### Directory structure

* baus/ contains all the Python code which runs the BAUS model.
* data/ contains BAUS inputs which are small enough to store and render in GitHub (large files are stored on Amazon S3) - this also contains lots of scenario inputs in the form of csv files.  See the readme in the data directory for detailed docs on each file.
* configs/ contains the model configuration files used by UrbanSim.  This also contains settings.yaml which provides simulation inputs and settings in a non-tabular form. 
* scripts/ these are one-off scripts which are used to perform various input munging and output analysis tasks.  See the docs in that directory for more information.




# A list of features available in BAUS

## Data management

* Add manual_edits.csv to edit individual attributes for each building/parcel in the building and parcel tables

* An orca step to correct the baseyear vacancies on the job side which happens pre-simulation

* Do job assignment by starting with baseyear controls and assigning on the fly (should do the same for households)

* Randomly assign deed restricted units within a zone because that's the most specific data would could get

## Run management

* Ability to run different model sets, output to Slack and web maps (in baus.py)

## Standard (or extensions to) UrbanSim features

* Houshold and job control totals by year

* Standard UrbanSim models - hedonic, location choice, transition, relocation models

* Basic supply and demand interactions to raise prices where demand exceeds supply

* Separate low and high income hlcms for the deed restricted units

* 1300 lines of computed columns to support the functionality described above

### Accessibility variables

* Accessibility variables including special "distance to location" variables

* Both local (local street network from OSM) and regional (travel model network) networks

* Network aggregations for prices as well

## Human input and overrides for specific models

* Do parcel-by-parcel rejections based on human knowledge to override the model

* Quite a bit of support by year and by scenario for custom development projects, as well as add vs demolish settings

* Proportional job model on "static parcels".  Static parcels are parcels whose jobs do not enter the relocation model and are auto-marked nodev.  These are e.g. city hall and Berkeley which would never get newly created by the model

* Relocation rates by taz

## Developer model

* Provide baseline zoning using parcel to zoning lookups and then attributes for all the zones in the region (num zones << num parcels)

* Do conditional upzoning and downzoning, add and remove building types, all by policy zone

* Limits - assign all parcels to a "bucket" - in this case to a city, but it could be a pda-city intersection.  Limits the amount of res and non-res development that comes from each bucket.  Make sure to do so for all non-res types.

* A special retail model which takes into account where demand for retail is high (income x households) but supply is low

* We still need a better way to determine the uses that get built between office / retail / industrial / whatever else

* A reprocessing of the developer results which adds job spaces to residential buildings at an appropriate rate, a second part of this adds ground floor retail by policy for tall buildings in order to create retail where people live

## Tweaks to get more reasonable results

* A setting which allows a user to disable building a more dense buiding that currently exists in a taz (used when we don't trust zoning much) - we minimize the max_dua or far on each parcel with the max built_dua or far near to that parcel

* For lack of better land price data, we use land prices proportional to the prevailing price per sqft

* Add (large) factors to land which is industrial to account for expensive land preparation

* Price shifters and cost shifters

* Rules to not consider certain parcels for development, like parcels which contain historical buildings, or have single family homes on small lots

## Accounts system and subsidies for affordable housing

* "Lump sump accounts" which are regional accounts that get a certain subsidy per year and are used to subsidize development in parcels that pass a certain filter and can create affordable housing or not

* Inclusionary housing rates which decrease revenues by a certain amount based on the AMI in an area and the inclusionary rate in an area - inclusionary housing rates get set by city

* Other policies which modify revenue as a percent of profit

* Impact fees which impose a cost per unit or per sqft

* Subsidized development on both the residential and office sides (not on retail?)

* Ad hoc "land value tax" revenue adjustment

## Output summaries, analysis and visualization

* Create a topsheet which does very basic one line summaries of a number of different outcomes - write as a text file to Slack so it can be read and reviewed from mobile

* Geographic summaries of households, jobs, and buildings for each 5 year increment, for superdistrict, pda, and juris geographies

* Write out disaggregate parcel and building data for visualization with ESRI

* A utility to compare output summary tables and put them in separate tabs of a workbook and color-code large differences

* Back into some demographic variables needed by the travel model that aren't produced by our UrbanSim model, including age categories, employed residents (by household location), population (we do households) some legacy land use variables from the previous model

* The travel model output which summarized some basic variables by zone

* Write out account information (subsididies and fees)

* Do urbanfootprint accounting - whether development is inside or outside the "UGB"

* Some extra diagnostic accounting including simple capacity calculations, simulated vacancy rates (for debugging, not as a sim output), sqft by building type

* Compare household and job counts to abag targets at the pda and juris levels
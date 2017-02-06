# A list of features available in BAUS

## Data management

* Add manual_edits.csv to edit individual attributes for each building/parcel in the building and parcel tables

* An orca step to correct the baseyear vacancies on the job side which happens pre-simulation

* Do job assignment by starting with baseyear controls and assigning on the fly (should do the same for households)

* Randomly assign deed restricted units within a zone because that's the most specific data would could get

## Run management

* Ability to run different model sets, output to Slack and web maps (in run.py)

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

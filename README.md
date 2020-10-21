DRAFT Bay Area UrbanSim (BAUS) Implementation
=======

[![Build Status](https://travis-ci.org/UDST/bayarea_urbansim.svg?branch=master)](https://travis-ci.org/UDST/bayarea_urbansim)

This is the DRAFT UrbanSim implementation for the Bay Area. Policy documentation for the Bay Area model is available [here](http://data.mtc.ca.gov/bayarea_urbansim/) and documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/).

### Installation

Bay Area UrbanSim is written in Python and runs in a command line environment. It's compatible with Mac, Windows, and Linux, and with Python 2.7 and 3.5+. Python 3 is recommended.

1. Install the [Anaconda Python](https://www.anaconda.com/products/individual#Downloads) distribution (not strictly required, but makes things easier and more reliable)
2. Clone this repository
3. Download base data from this [Box folder](https://mtcdrive.app.box.com/folder/103451630229?s=di77ya24xmmsg3rkrotaewegzsd61hu6) and move the files to `bayarea_urbansim/data/` (ask an MTC contact for access)
4. Clone the MTC [urban_data_internal repository](https://github.com/BayAreaMetro/urban_data_internal) to the same location as this repository (ask an MTC contact for access)
5. Create a Python environment with the current dependencies: `conda env create -f baus-env-2020.yml`
6. Activate the environment: `conda activate baus-env-2020`
7. Pre-process the base data: `python baus.py --mode preprocessing` (only needed once)
8. Run the model: `python baus.py`

More info about the command line arguments: `python baus.py --help`

Optional visualization tool and Slack messenger: 
* Configure Amazon Web Services (AWS) to get s3 permission (you will need an appropriately configured AWS credentials file from your MTC contact) 
* Install AWS SDK for Python -- boto3 using `pip install boto3`
* Install Slacker to use Slack API using `pip install slacker` (you will need an appropriate slack token to access the slack bot from your MTC contact)
* Set environment variable `URBANSIM_SLACK = TRUE`

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

DRAFT Bay Area Urbansim Implementation
=======

This is the DRAFT UrbanSim implementation for the Bay Area. Documenation for the Bay Area model is available at [here](http://data.mtc.ca.gov/bayarea_urbansim/) and documentation for there UrbanSim framework is [here](https://udst.github.io/urbansim/)

### Install Overview

* Install Python for your OS ([Anaconda](https://www.continuum.io/downloads) highly suggested)
* Clone this repository
* Install dependencies using `pip install -r requirements.txt`
* Get data using `python run.py -c --mode fetch-data` (you will need an appropriately configured AWS credentials file which you must get from your MTC contact)
* Preprocess data using `python run.py -c --mode preprocessing`
* Run a simulation using `python run.py -c` (default mode is simulation)

### An overview of `run.py`
 
Run.py is a command line interface (cli) used to run Bay Area UrbanSim in various modes.  These modes currently include:

* estimation, which runs a series of models to estimate all models
* simulation, which runs all models to simulate a regional growth forecast
* fetch_data, which downloads large data files from Amazon S3
* preprocessing, which performas long-running data cleaning steps and writes newly cleaned data back to the h5 for use in the other steps
* baseyearsim which runs a "base year simulation" which summarizes the data before the simulation runs (during simulation, summaries are written after each year, so the first year's summaries are *after* the base year is finished - a base year simulation writes the summaries before any models have run)

### Outputs from Simulation (written to the `runs` directory)

ALL OUTPUT IN THIS DIRECTORY IS NOT OFFICIAL OUTPUT. PLEASE CONTACT MTC FOR OFFICIAL OUTPUTS OF THE LAST PLAN BAY AREA.

`[num]` = a identifier used to identify each successive run.  It's an integer starts at 1 and increments each time run.py is called.

Many files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run[num]_topsheet_2040 | An overall summary of various housing and employment outcomes summarized by very coarse geographies.
run[num]_parcel_output.csv 		| A csv of all new 
run[num]_parcel_data_[year].csv 			|A CSV with parcel level output for *all* parcels with lat, lng and includes change in total_residential_units and change in total_job_spaces, as well as zoned capacity measures
run#_taz_summaries 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)

baus/

data/

configs/    

scripts/



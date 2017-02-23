DRAFT Bay Area UrbanSim (BAUS) Implementation
=======

This is the DRAFT UrbanSim implementation for the Bay Area. Policy documentation for the Bay Area model is available [here](http://data.mtc.ca.gov/bayarea_urbansim/) and documentation for the UrbanSim framework is available [here](https://udst.github.io/urbansim/).

### Install Overview

* Install Python for your OS ([Anaconda](https://www.continuum.io/downloads) highly suggested)
* Clone this repository
* Install dependencies using `pip install -r requirements.txt`
* Get data using `python run.py -c --mode fetch-data` (you will need an appropriately configured AWS credentials file which you must get from your MTC contact)
* Preprocess data using `python run.py -c --mode preprocessing`
* Run a simulation using `python run.py -c` (default mode is simulation)

### An overview of run.py
 
Run.py is a command line interface (cli) used to run Bay Area UrbanSim in various modes.  These modes currently include:

* estimation, which runs a series of models to save parameter estimates for all statistical models
* simulation, which runs all models to create a simulated regional growth forecast
* fetch_data, which downloads large data files from Amazon S3 as inputs for BAUS
* preprocessing, which performas long-running data cleaning steps and writes newly cleaned data back to the binary h5 file for use in the other steps
* baseyearsim which runs a "base year simulation" which summarizes the data before the simulation runs (during simulation, summaries are written after each year, so the first year's summaries are *after* the base year is finished - a base year simulation writes the summaries before any models have run)

### Outputs from Simulation (written to the runs directory)

ALL OUTPUT IN THIS DIRECTORY IS NOT OFFICIAL OUTPUT. PLEASE CONTACT MTC FOR OFFICIAL OUTPUTS OF THE LAST PLAN BAY AREA.

`[num]` = a positive integer used to identify each successive run.  This number usually starts at 1 and increments each time run.py is called.

Many files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run[num]\_topsheet\_[year].csv | An overall summary of various housing and employment outcomes summarized by very coarse geographies.
run[num]_parcel_output.csv 		| A csv of all new built space in the region.  This has a few thousand rows and dozens of columns which contain various inputs and outputs, as well as debugging information which helps explain why each development was picked by UrbanSim.
run[num]\_parcel_data\_[year].csv 			|A CSV with parcel level output for *all* parcels with lat, lng and includes change in total_residential_units and change in total_job_spaces, as well as zoned capacity measures.
run[num]\_building_data\_[year].csv 			|The same as above but for buildings.
run[num]\_taz\_summarie\s_[year].csv 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)
run[num]\_pda_summaries\_[year].csv, run[num]\_juris_summaries\_[year].csv, run[num]\_superdistrict_summaries\_[year].csv | Similar outputs to the taz summaries but for each of these geographies.  Used for understanding the UrbanSim forecast at an aggregate level.
run[runnum]_dropped_buildings.csv     | A summary of buildings which were redeveloped during the simulated forecast.
run[runnum]_simulation_output.json | Used by the web output viewer.


### Directory structure

* baus/ contains all the Python code which runs the BAUS model.
* data/ contains BAUS inputs which are small enough to store and render in GitHub (large files are stored on Amazon S3) - this also contains lots of scenario inputs in the form of csv files.  See the readme in the data directory for detailed docs on each file.
* configs/ contains the model configuration files used by UrbanSim.  This also contains settings.yaml which provides simulation inputs and settings in a non-tabular form. 
* scripts/ these are one-off scripts which are used to perform various input munging and output analysis tasks.  See the docs in that directory for more information.

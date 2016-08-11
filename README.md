DRAFT Bay Area Urbansim Implementation
=======

This is the DRAFT UrbanSim implementation for the Bay Area. Documenation for the Bay Area model is available at http://metropolitantransportationcommission.github.io/baus_docs/ and documentation for the generic UrbanSim model is at https://udst.github.io/urbansim/index.html

UAL research fork
-----------------

This copy of the Bay Area UrbanSim repository is a research fork maintained by U.C. Berkeley's Urban Analytics Lab (http://ual.berkeley.edu).

The `master` branch tracks the main UDST fork, and the `ual-development` branch contains our modifications to the model. Our current projects focus on extending Bay Area UrbanSim to better capture processes of residential displacement and affordable housing provision.

Modifications include: 

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

#### Installation

The following setup procedure seems reliable for OS X and Linux. See [ual_baus_install.sh](https://github.com/ual/bayarea_urbansim/blob/ual-development/ual_baus_install_template.sh) for a programmatic version. 

* Install Anaconda
* Git-clone the repositories for `orca`, `urbansim`, `pandana`, `orca_test`, `urbansim_defaults`, and `bayarea_urbansim`
* Run `python setup.py develop` for all of them (except `bayarea_urbansim`)
* Switch to the `ual-development` branch of `bayarea_urbansim`
* Copy the appropriate data files into the `data` directory (or add symbolic links)
* Try running `python run.py`

### Subsequent README content is from the master branch

Install Overview
----------------

* https://mtcdrive.account.box.com/login
* get anaconda (version as indicated in reqs below)
* bash Anaconda2-4.0.0-Linux-x86_64.sh
* yes to prepend install location to .bashrc
* open new terminal
* sudo apt-get update
* sudo apt-get -y install git g++ python-dev unzip
* git clone https://github.com/MetropolitanTransportationCommission/bayarea_urbansim.git
* pip install -r requirements.txt (comment out pandana)
* pip install pandana
* get data
* change RUNNUM so in 5000s etc
* python run.py -s 4 & OR python all.py &

Data
----

We track the data for this project in the Makefile in this repository. The makefile will generally be the most up to date list of which data is needed, where it goes in the directory, etc.

To fetch data with [AWS CLI](https://aws.amazon.com/cli/) and Make, you can:
`make data`.

Below we provide a list to links of the data in the Makefile for convenience, but in general the makefile is what is being used to run simulations. If you find that something below is out of date w/r/t the makefile, please feel free to update it and submit a pull request.

####Data necessary for run.py to run

These data should be in the data/ folder:

https://s3.amazonaws.com/bayarea_urbansim/data/2015_06_01_osm_bayarea4326.h5  
https://s3.amazonaws.com/bayarea_urbansim/data/2015_08_03_tmnet.h5  
https://s3.amazonaws.com/bayarea_urbansim/data/2015_12_21_zoning_parcels.csv  
https://s3.amazonaws.com/bayarea_urbansim/data/02_01_2016_parcels_geography.csv  
https://s3.amazonaws.com/bayarea_urbansim/data/2015_08_29_costar.csv  
https://s3.amazonaws.com/bayarea_urbansim/data/2015_09_01_bayarea_v3.h5  

Because the hdf5 file used here contains one table with  proprietary data, you will need to enter credentials to download it. You can request them from Tom Buckley(tbuckl@mtc.ca.gov). Or if you already have access to Box, you can download the hdf5 file from there. 

####Data Description  


How To 
------
####Set Up Simulation and Estimation  
Install dependencies using standard [pip](https://pip.pypa.io/en/latest/user_guide.html#requirements-files) requirements install:
`pip install -r requirements.txt`
You may also need to install pandana
`pip install pandana`

####Set up using a Virtual Machine
For convenience, there is a [Vagrantfile](https://www.vagrantup.com/) and a `scripts/vagrant/bootstrap.sh` file. This is the recommended way to set up and run `Simulation.py` on Windows. 

####Enter Amazon Web Services credentials to fetch data.

See [Installing](http://docs.aws.amazon.com/cli/latest/userguide/installing.html) and [configuring] (http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) 

Each of the following just runs a different set of models for a different set of years.

####Run a Simulation  
In the repository directory type `python run.py`  

####Estimate Regressions used in the Simulation
In the repository directory edit `run.py` and set `MODE` to "estimation" and type `python run.py`  

####Run a Base Year Simulation
In the repository directory edit `run.py` and set `MODE` to "baseyearsim" and type `python run.py`.  A base year simulation is used to run a few models and make sure everything matches the first year of the control totals but not to add any new buildings.  This is then used in comparison of the year 2040 to the base year for all future simulations (until the control totals change) and this mode is rerun.

####Review Outputs from Simulation

#####Runs Directory

ALL OUTPUT IN THIS DIRECTORY IS CONSIDERED DRAFT. PLEASE CONTACT MTC FOR OFFICIAL FINAL OUTPUTS.

`#` = a number that is updated in the RUNNUM file in the bayarea_urbansim directory each time you run Simulation.py.

Many files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run#_topsheet_2040 | An overall summary of various housing, employment, etc by regional planning area types
run#_parcel_output.csv 		|csv of parcels that are built for review in Explorer
run#_parcel_data_diff.csv 			|A CSV with parcel level output for *all* parcels with lat, lng and includes change in total_residential_units and change in total_job_spaces, as well as zoned capacity measures
run#_simulation_output.json |summary by TAZ for review in Explorer (unix only)
run#_taz_summaries 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)
run#_urban_footprint_summary | A CSV with A Summary of how close the scenario is to meeting [Performance Target 4](http://planbayarea.org/the-plan/plan-details/goals-and-targets.html)


Browse results [here](http://urbanforecast.com/runs/)   

######Other Directories
Below is an explanation of the directories in this repository not described above.

configs/    

The YAML files in this directory allow you to configure UrbanSim by changing the keys and values of arguments taken by urbansim functions. See the [UrbanSim Defaults](https://udst.github.io/urbansim_defaults/) docs for more details.

Note that even the values taken by data can be and are configured with these config files (e.g. values in `settings.yaml`).

data_regeneration/

The scripts in here can be used to re-create the data in the `data/` folder from source (various local, state, and federal sources). Use these to re-create the data here when source data change fundamentally.

scripts/
This is a good place to put scripts that can exist independently of the analysis environment here.  

####Parcel Geometries

The parcel geometries are the basis of many operations in the simulation. For example, as one can see in [this pull request](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/pull/121), in order to add schedule real estate development projects to the list of projects that are included in the simulation, one must use an existing `geom_id`, which is a field on the parcels table added [here](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/master/data_regeneration/match_aggregate.py#L775-L784).

Parcel geometries are available at the following link:

https://s3.amazonaws.com/bayarea_urbansim/data/09_01_2015_parcel_shareable.zip

#####Geom ID

Please be aware that many ArcGIS users have found that ArcGIS automatically converts and then rounds the `geom_id` column, effectively making it unusable. Therefore we recommend using QGIS, which does not exhibit this behavior with delimited files by default. 

Also, in Microsoft Excel, you will need to make sure that the data type of the `geom_id` column is set to `number` and that the number of decimal points is set to 0. Otherwise when you save the CSV again the `geom_id`s will be unusable.

What is the `geom_id` field and why does it exist? 

In short, this is a legacy identifier. The `geom_id` field was introduced as a stable identifier for parcels across shapefiles, database tables, CSV's, and other data types. It is an integer because at some point there was a need to support integer only identifiers. It is not based on an Assessor's Parcel Numbers because there was a perception that those were inadequate. And it is based on the geometry of the parcel because many users have found that geometries are the most important feature of parcels.

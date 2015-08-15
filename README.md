Bay Area Urbansim Implementation
=======

This is the full UrbanSim implementation for the Bay Area.

###Data

Put the following in the data directory:

* https://s3.amazonaws.com/bayarea_urbansim/data/2015_06_01_osm_bayarea4326.h5  
* https://s3.amazonaws.com/bayarea_urbansim/data/2015_06_01_zoning_parcels.csv  
* https://mtcdrive.box.com/2015-06-01-bayarea-v3-h5  

####Data Description  
[The MTC Analytics Wiki](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/InputFiles?validation_key=0301bd909f2a02c80cb5e315fec942d8) contains a draft table with descriptions for the data inputs. 

How To 
------
####Set Up Simulation and Estimation  
Install dependencies using standard [pip](https://pip.pypa.io/en/latest/user_guide.html#requirements-files) requirements install:
`pip install -r requirements.txt` 

####Run a Simulation  
In the repository directory type `python Simulation.py`  

####Estimate Regressions used in the Simulation
In the repository directory type `python Estimation.py`  

####Review Outputs from Simulation

#####Runs Directory

First, some vocabulary.

`#` = a number that is updated in the RUNNUM file in the bayarea_urbansim directory each time you run Simulation.py.

Several files are output to the `runs/` directory. They are described below.

filename |description
----------------------------|-----------
run#_parcel_output.csv 		|csv of parcels that are built for review in Explorer
run#_subsidy_summary.csv 	|currently empty
run#_simulation_output.json |summary by TAZ for review in Explorer (unix only)
run#_taz_summaries 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel) (Windows only)

#####Open A Browser with a Map of whats built in a Simulation
After running a simulation, you should have a run_parcel_output.csv and run#_simulation_output.json in the runs/ directory

To put these results on a map, run, `python scripts/explorer.py #`

This script will start a server and open a web-browser pointed at it.

Optional Tools
--------------
####Make

Because the hdf5 file used here contains proprietary data, you will need to enter credentials to download it. You can request them from Tom Buckley(tbuckl@mtc.ca.gov). Or if you already have access to Box, you can download the hdf5 file at the link above. 

For fetching data, alternatively, with [AWS CLI](https://aws.amazon.com/cli/) and Make, you can 
`make data`.

####Parcel geometries

The parcel geometry for these data are here:

https://mtcdrive.box.com/shared/static/4s4l4g2q88ddpe1altcklpgtqhqaawcp.zip

#####Using Make to get source data:

To get the full dump of the database that the h5 file came from type:

`make database_backup`

To get the shapefile for parcels associated with the h5 file type:

`make parcel_shapefile`

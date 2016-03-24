Bay Area Urbansim Implementation
=======

This is the full UrbanSim implementation for the Bay Area.

UAL development branch
----------------------

Extends Bay Area UrbanSim to better capture processes of residential displacement and affordable housing provision. Modifications include: 

#### Data structures

* Builds out the representation of individual housing units to include a semi-persistent tenure status, which is assigned based on characteristics of initial unit occupants
* Joins additional race/ethnicity PUMS variables to synthetic households
* Adds a representation of market rents alongside market sale prices

#### Model steps

* Residential hedonics predict market rents and sale prices separately, with rents estimated from Craigslist listings
* Household move-out choice depends on tenure status
* Household location choice is modeled separately for renters and owners, and includes race/ethnicity measures as explanatory variables
* Developer models are updated to produce both rental and ownership housing stock

Other work is in progress; see wiki. 

Subsequent README content is from the master branch.

Data
----

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

####Open A Browser with a Map of whats built in a Simulation
Uncomment the line `#travel_model_output` in Simulation.py  

Type `mkdir runs` to make a folder where the outputs will be written.  
Type `python Simulation.py`

When that completes, run, for example `python scripts/explorer.py 2`. NOTE: The `2` passed to this script is a requirement. It will correspond to the number in the filename written in the `runs` directory, which is just a count of how many times you've run the Simulation, which is written to the file RUNNUM in the repository's directory. This script will start a server and open a web-browser pointed at it.  

Optional Tools
--------------
####Make

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

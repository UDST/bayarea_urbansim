Bay Area Urbansim Implementation
=======

This is the full UrbanSim implementation for the Bay Area.

###Data

We track the data for this project in the Makefile in this repository. 

To fetch data with [AWS CLI](https://aws.amazon.com/cli/) and Make, you can:
`make data`.

Because the hdf5 file used here contains one table with  proprietary data, you will need to enter credentials to download it. You can request them from Tom Buckley(tbuckl@mtc.ca.gov). Or if you already have access to Box, you can download the hdf5 file from there. 

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
run#_taz_summaries 			|A CSV for [input to the MTC travel model](http://analytics.mtc.ca.gov/foswiki/UrbanSimTwo/OutputToTravelModel)

#####Results on S3:

Results can be pushed to S3 with the S3 flag in `Simulation.py`.   

Browse results [here](http://bayarea-urbansim-results.s3-us-west-1.amazonaws.com/index.html)   

Optional Tools
--------------


####Parcel geometries

The parcel geometry for these data are on box::badata/out/summaries

#####Using Make to get source data:

To get the full dump of the database that the h5 file came from type:

`make database_backup`

######Other Directories
Below is an explanation of the directories in this repository not described above.

configs/    

The YAML files in this directory allow you to configure UrbanSim by changing the keys and values of arguments taken by urbansim functions. See the [UrbanSim Defaults](https://udst.github.io/urbansim_defaults/) docs for more details.

Note that even the values taken by data can be and are configured with these config files (e.g. columns in `settings.yaml`).

data_regeneration/

The scripts in here can be used to re-create the data in the `data/` folder from source (various local, state, and federal sources). Use these to re-create the data here when source data change fundamentally.

notebooks/
Contains iPython notebooks--interactive examples of how to use simulation, estimation, and others.

scripts/
This is a good place to put scripts that can exist independently of the analysis environment here.  

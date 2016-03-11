Bay Area Urbansim Implementation
=======

This is the full UrbanSim implementation for the Bay Area. Documenation for the Bay Area model is available at http://metropolitantransportationcommission.github.io/baus_docs/ and documentation for the generic UrbanSim model is at https://udst.github.io/urbansim/index.html

###Data

We track the data for this project in the Makefile in this repository. The makefile will generally be the most up to date list of which data is needed, where it goes in the directory, etc.

To fetch data with [AWS CLI](https://aws.amazon.com/cli/) and Make, you can:
`make data`.

Below we provide a list to links of the data in the Makefile for convenience, but in general the makefile is what is being used to run simulations. If you find that something below is out of date w/r/t the makefile, please feel free to update it and submit a pull request.

####Data necessary for run.py to run

These data should be in data/:

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

Note that even the values taken by data can be and are configured with these config files (e.g. values in `settings.yaml`).

data_regeneration/

The scripts in here can be used to re-create the data in the `data/` folder from source (various local, state, and federal sources). Use these to re-create the data here when source data change fundamentally.

scripts/
This is a good place to put scripts that can exist independently of the analysis environment here.  

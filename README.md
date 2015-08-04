bayarea
=======

This is the full UrbanSim implementation for the Bay Area.

BEWARE: THE CURRENT MASTER REQUIRES THE bayarea_v3.h5 FILE.

data
=======
Simulation.py has specific data requirements, which are outlined in the Makefile of this repository, under the target `data:`.

####Downloading necessary simulation data in a zipfile

These data can also be downloaded as a zipfile [here](https://mtcdrive.box.com/2015-06-01-bayarea-urbansim). 

The contents of this zipfile should be placed in the data folder of this repository. 

####Using Make to get the necessary simulation data:

If you are on linux/osx, and have [AWS CLI](https://aws.amazon.com/cli/) installed, you can fetch the necessary data by typing:
`make data`, in the directory of this repository. 

#####Using Make to get source data:

If you are debugging errors in the data source you may need to inspect source data. 

To get the full dump of the database that the h5 file came from type:

`make database_backup`

To get the shapefile for parcels associated with the h5 file type:

`make parcel_shapefile`

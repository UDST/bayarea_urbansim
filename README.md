bayarea
=======

This is the full UrbanSim implementation for the Bay Area.

data
=======
Simulation.py has specific data requirements, which are outlined in the Makefile of this repository, under the target `data:`.

####Downloading necessary data:

Put the following data in the data directory:

https://s3.amazonaws.com/bayarea_urbansim/data/2015_06_01_osm_bayarea4326.h5  
https://s3.amazonaws.com/bayarea_urbansim/data/2015_06_01_zoning_parcels.csv  
https://mtcdrive.box.com/2015-06-01-bayarea-v3-h5  

####Using Make to get the data:

If you are on linux/osx, and have [AWS CLI](https://aws.amazon.com/cli/) installed with the proper keys, you can fetch the necessary data by typing:
`make data`, in the directory of this repository. 

####Parcel geometries for these data

The parcel geometry for these data are here:

https://mtcdrive.box.com/shared/static/4s4l4g2q88ddpe1altcklpgtqhqaawcp.zip

#####Using Make to get source data:

To get the full dump of the database that the h5 file came from type:

`make database_backup`

To get the shapefile for parcels associated with the h5 file type:

`make parcel_shapefile`



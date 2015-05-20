bayarea_urbansim
=======

This is the full UrbanSim implementation for the Bay Area.

BEWARE: THE CURRENT MASTER REQUIRES THE bayarea_v3.h5 FILE.

###How To
The ipython notebooks in /notebooks are the easiest way to start. 

The *estimation* notebook allows the estimation of price models and location choices, the *simulation* notebook runs the full UrbanSim simulation, and the *exploration* notebook allows the interactive exploration of the included data using web mapping tools (primarily enabling the interaction between Leaflet and Pandas).

###Data Regeneration

####BAY AREA REGENERATION INSTALLATION GUIDE (Windows)

####OVERVIEW

The regional regeneration tools are written in Python and SQL. They depend on soft- ware developed by Synthicity, spandex and urbansim, and third-party open source software, including Python, pandas, PostgreSQL, and PostGIS. Refer to table 1 for the purpose of each dependency.
Although not required, a visualization tool, like UrbanCanvas or QGIS, is recom- mended.
Assumptions. This guide makes several assumptions.
(1) The operating system is Windows 64-bit. The regeneration tools will run on other platforms, including Linux and OS X, but only Windows installation is documented in this guide.
(2) The target system has sufficient memory. 16GB+ is recommended.
(3) Git is installed.
(4) No other Python builds are installed to the system PATH, as they may interfere
with the desired Anaconda Python distribution.
(5) The user has familiarity with Git, Python, and PostgreSQL.

####INSTALLATION
￼￼￼￼￼￼￼￼￼PostgreSQL and PostGIS. Installation of PostgreSQL and PostGIS is described in the PostGIS documentation. Be sure to install the 64-bit builds of PostgreSQL and PostGIS.

Or you can use something like: https://github.com/buckleytom/postgis-gdal-vagrant

PostgreSQL can be configured for better performance, especially on high-end hard- ware and with large, spatial queries. The third-party web application PgTune may suggest parameters that take advantage of available hardware, like more memory. Specify “Data warehouses” for the DB Type if using PgTune.

Additionally, if the database is stored on a solid-state drive, the random_page_cost parameter can be set to a lower value closer to seq_page_cost, for example 1.1.

Restart PostgreSQL by running services.msc, right-clicking the service, and click- ing restart.
Create a new database called mtc. This can be done from within pgAdmin III af- ter connecting to the server. The database name will be later used in the spandex configuration file.

Python. Anaconda Python is developed and distributed by Continuum Analytics. Be sure to install the 64-bit build of the Anaconda Python distribution.

Then, update Anaconda.

     conda update conda
     conda update anaconda
     
urbansim. urbansim installation is described in its documentation.

     conda config --add channels synthicity
     conda install urbansim

Some dependencies can be installed using conda.
conda install gdal pandas six sqlalchemy GeoAlchemy2 can be installed using pip.  

     conda install sqlalchemy pip
     pip install GeoAlchemy2

Currently, Psycopg2 is not packaged by the Anaconda distribution and must be manually installed. One workaround is to run the win-psycopg installer maintained by Jason Erickson.

http://www.lfd.uci.edu/~gohlke/pythonlibs/#psycopg

Download, then:

`pip install psycopg2-2.5.5-cp27-none-win_amd64.whl`

Likewise, the optional dependency PyGraphViz, for plotting simulation framework relationships, is also not packaged by the Anaconda distribution. One workaround is to run the installer maintained by Christoph Gohlke, which requires installation of GraphViz.

Once spandex dependencies are satisfied, spandex itself can be installed from within the cloned Git repository using pip. The -e option installs spandex in editable mode so that it will track updates to the cloned repository.

     git clone https://github.com/synthicity/spandex
     cd spandex
     pip install -e .

Spandex should be configured with a text file. Refer below for an example file. This file should be placed in ~/.spandex/user.cfg, where ~ represents the home direc- tory, e.g., C:\Users\username. The data directory can be specified with forward slashes or double backslashes.

spandex configuration file, based on config/example.cfg Regeneration. The regeneration tools are distributed with the regional UrbanSim:
```
[database]
# Database configuration is passed to psycopg2.connect, which also supports
# libpq connection parameters:
# http://initd.org/psycopg/docs/module.html#psycopg2.connect
database = mtc
user = postgres
password = FIXME
host = localhost
port = 5432
[data]
# Directory containing shapefiles to import. Can be an absolute path or
# relative to the current working directory.
directory = c:/FIXME/badata
# Spatial Reference System Identifier (SRID) to reproject to.
# http://spatialreference.org/
# http://prj2epsg.org/
#
# NAD83(HARN) / California zone 3.
srid = 2768
```

Current development takes place in the spandex branch of the repository and will be
periodically merged to master.

     git clone https://github.com/synthicity/bayarea_urbansim
     cd bayarea_urbansim
     git checkout spandex
     


####RUNNING REGENERATION
The run.py script under the data_regeneration directory in the bayarea_urbansim repository will process all regeneration steps. Other scripts in that directory can be executed individually for debugging purposes.
`python data_regeneration/load.py`  

Currently, run.py executes separate scripts in each regeneration step.  

(1) Preprocessing: import shapefiles by county, fix invalid geometries, and repro- ject.
(a) load.py
(2) Processing: create regional parcels table from county-specific attributes and
geometries.
(a) counties/*.py
(b) join_counties.sql
(3) Postprocessing: tag parcels by TAZ. (a) spatialops.py

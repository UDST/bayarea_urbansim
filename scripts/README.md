# Script Docs

Wow we've done a poor job of keeping this up to date.
^ Still True! :) 

###compare_output.py

Use this script to compare the outcomes of 4 scenario runs. This script should run automatically at the the end of a set of Simulation runs for scenarios. In case it doesn't, you can also execute it manually. 

####Short Usage

To compare the 2040 output files for all the geographies for a set of 4 runs, execute the following at the command line in the directory of the bayarea_urbansim repository:

`python scripts/compare_output.py # # # #`

Where each # is a run number, for example:

`python scripts/compare_output.py 717 718 719 777`

####Usage Explained

Looking at 'compare_output.py', we can see that the script is short. In fact, at the very minimum, you could accomplish something similar by starting a Python interpreter (while in the bayarea_urbansim repository directory) and then typing:

```
from scripts.output_csv_utils import compare_outcome_for 

compare_outcome_for('tothh', set_geography='superdistrict', runs=[717,718,719,777])
```

Note that the 1compare_outcome_for` function takes 3 arguments:
* Variable ('tothh')
* set_geography ('superdistrict') 
* runs ([717,718,719,777])

Note that you must have 4 run numbers in the array. Also, you must have a CSV file for the year 2040 outputs that correspends to each run number AND to the `set_geography` variable. So in this case, we would need to have the following files available:

```
runs/run717_superdistrict_summaries_2040.csv
runs/run718_superdistrict_summaries_2040.csv
runs/run719_superdistrict_summaries_2040.csv
runs/run777_superdistrict_summaries_2040.csv
```

If you don't have all geographies present in the runs folder, you must comment out those geographies

The `compare_outcome_for` function is further defined in `output_csv_utils.py`

####Background & Notes

The compare_output.py script was written in order to facilitate scenario planning by comparing the outcomes of different scenarios specifications passed to each Simulation. A simulation of a given policy scenario is achieved by executing the script `Simulation.py` with pre-defined flags for scenarios. 

For example, if in one scenario a particular policy was put in place, then it would be reflected in different assumptions about how the simulation will run. Mechanically, some of differences in the scenarios can be found [here](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/master/Simulation.py#L98-L129)

An example of how scenarios are presented at Commission meetings can be found [here](https://mtc.legistar.com/LegislationDetail.aspx?ID=2555453&GUID=4B5BB96B-FB97-497D-B075-BED97FC5CB77&Options=ID|Text|&Search=%22land+use%22). 

The best list of current working policies in the scenarios used in this repository is [here](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/wiki/Model-Runs).

## UrbanSim Explorer makes the urbansim_explorer output map for a given run number

####Usage

Example setup:
```
git clone https://github.com/UDST/urbansim_explorer
cd urbansim_explorer/
python setup.py develop
cd bayarea_urbansim/
mkdir -p /var/www/html
```

example usage:
```
cd bayarea_urbansim
python scripts/explorer.py 717 
```

general explanation:
`scripts/explorer.py SCENARIO_NUM` 

SCENARIO_NUM is the number of a set of `taz_summaries` and `parcel_outputs` in the /runs directory e.g. 717 would refer to csv's like:

```
runs/run717_taz_summaries_2040.csv
runs/run717_parcel_output.csv
```

####fix_zoning_missing_id.py

This script was written to find parcels that have a zoning name assigned to them but which don't have an entry in the `data/zoning_lookup.csv`. Example instances of where this might happen are if a city adds a newly named zoning code or where we might not have assigned zoning at all, which seems to have happened often with parks, the working assumption being that a park would never be re-developed. This is probably an assumption worth testing. 

#####Usage

To use it, run:

`python scripts/fix_zoning_missing_id.py`

The script will print out a summary of parcels with null zoning values by city. It will also look up strings for each city in the zoning_lookup.csv `name` column and then assign the appropriate id if its finds one. Note that it will write the results back out to a `zoning_parcels.csv` file preceded by todays date. 

## export.py and export2.py are used to get parcel info into TogetherMap

Here are the steps I take to get the data into Mongo (TogetherMap will read automatically from Mongo if you get the data in right).

* First remove the old data from mongo.
  * Login in to the MTC EC2 machine
  * type `mongo togethermap` to start the mongo client in the togethermap db
  * type `db.places.remove({collectionId: "ZC7yyAyA8jkDFnRtf"})` - note that this hash is whatever the collection hash is in togethermap.  We could have multiple parcel sets loaded at the same time with different hashes or whatever, but to remove all the places for a collection the command looks like this.  OK, now the TM collection is empty.  You can go to the app and confirm this - http://urbanforecast.com:1984/collection/ZC7yyAyA8jkDFnRtf.  (ctrl-D to exit mongo shell)
* Next we need to get the attributes that we'll put on the parcels.
  * This is easy - just `cd bayarea_urbansim` and then `python scripts/export.py` - this script should be pretty easy to follow as it just uses orca to get some computed columns on the parcels.  This outputs a file called parcels.csv automatically which has all the attributes tied to ids.
* Next we need to join the attributes to shapes and load them into mongo.
  * Assuming you're still in bayarea_urbansim, just `python scripts/export2.py` - this runs through the parcel shapefile using the Python Fiona library and merges the attributes from the above together with the shapes and turns the result into geojson.  The geojson can get written to disk or loaded directly into Mongo (which is the default).

## The scripts below aren't used nearly as much anymore.

compare_to_targets compares parcel level residential unit totals to the pda targets

and make_pda_maps uses folium to make maps of the comparison between modeled results and targets

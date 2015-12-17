# Script Docs

Wow we've done a poor job of keeping this up to date.

## UrbanSim Explorer makes the urbansim_explorer output map for a given run number

## export.py and export2.py are used to get parcel info into TogetherMap

Here are the steps I take to get the data into Mongo (TogetherMap will read automatically from Mongo if you get the data in right).

* First remove the old data from mongo.
  * Login in to the MTC EC2 machine
  * type `mongo togethermap` to start the mongo client in the togethermap db
  * type `db.places.remove({collectionId: "ZC7yyAyA8jkDFnRtf"})` - note that this hash is whatever the collection hash is in togethermap.  We could have multiple parcel sets loaded at the same time with different hashes or whatever, but to remove all the places for a collection the command looks like this.  OK, now the TM collection is empty.  You can go to the app and confirm this - http://urbanforecast.com:1984/collection/ZC7yyAyA8jkDFnRtf.  (ctrl-D to exit mongo shell)
* Next we need to get the attributes that we'll put on the parcels.
  * This is easy - just `cd bayarea_urbansim` and then `python scripts/export.py` - this script should be pretty easy to follow as it just uses orca to get some computed columns on the parcels.  This outputs a file called parcels.csv automatically which has all the attributes tied to ids.
* Next we need to join the attributes to shapes and load them into mongo.
  * Assuming you're still in bayarea_urbansim, just `python scripts/export2.py` - this runs through the parcel shapefile using the Python Fiona library and the attributes from the above together with the shapes into geojson.  The geojson can get written to disk or loaded directly into Mongo (which is the default).

## These aren't used nearly as much anymore.

compare_to_targets compares parcel level residential unit totals to the pda targets

and make_pda_maps uses folium to make maps of the comparison between modeled results and targets

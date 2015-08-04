#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/
database_get = aws s3 cp s3://landuse/spandex/outputs/

#the data necessary for simulation.py to run:
data: data/2015_06_01_bayarea_v3.h5 \
data/2015_06_01_osm_bayarea4326.h5 \
data/2015_06_01_zoning_parcels.csv

#the parcel shapefile associated with the data above:
parcel_shapefile: data/2015_06_01_parcels.dbf \
data/2015_06_01_parcels.prj \
data/2015_06_01_parcels.shp \
data/2015_06_01_parcels.shx

#the database that the h5 file above was exported from:
database_backup: 2015/06/23/full.dump \
2015/06/23/db-schema.sql \
2015/06/23/globals.sql

#summaries of the taz data for the run of data_regeneration/run.py associated with the data above
taz_summaries: data/2015_06_01_taz_summary.csv

data/2015_06_01_avenodeprice.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_bayarea_v3.h5:
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_buildings.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_nodes.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_nodes_prices.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_osm_bayarea4326.h5: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels.dbf: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels.prj: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels.shp: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels.shx: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels_geography.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_parcels_to_zoning.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_pdatargets.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_taz_summary.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_zoning_parcels.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

#the database was updated on 6/23, with some changes to the zoning_parcels table--it is based on the 6/1/2015 parcel database
2015/06/23/full.dump:
	mkdir -p $(dir $@)
	$(database_get)$@ \
	$@.download
	mv $@.download $@

2015/06/23/db-schema.sql:
	mkdir -p $(dir $@)
	$(database_get)$@ \
	$@.download
	mv $@.download $@

2015/06/23/globals.sql:
	mkdir -p $(dir $@)
	$(database_get)$@ \
	$@.download
	mv $@.download $@
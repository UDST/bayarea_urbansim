#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/
database_get = aws s3 cp s3://landuse/spandex/outputs/

#the data necessary for simulation.py to run:
data: data/2015_09_01_bayarea_v3.h5 \
data/2015_06_01_osm_bayarea4326.h5 \
data/2015_09_22_zoning_parcels.csv \
data/2015_08_19_parcels_geography.csv \
data/2015_08_29_costar.csv \
data/2015_08_03_tmnet.h5

#the database that the h5 file above was exported from:
database_backup: 2015/06/23/full.dump \
2015/06/23/db-schema.sql \
2015/06/23/globals.sql

data/2015_06_01_bayarea_v3.h5:
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_osm_bayarea4326.h5: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_08_13_zoning_parcels.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_08_19_parcels_geography.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_08_29_costar.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_08_03_tmnet.h5:
	$(get)$@ \
	$@.download
	mv $@.download $@

#####################
####EXTRAS/SOURCE####
#####################
parcels_06_12_2015.shp: parcels_06_12_2015.zip
	unzip -o $<
	touch $@

#the database was updated on 6/23, with some changes to the zoning_parcels table--it is based on the 6/1/2015 parcel database
data/parcels_06_12_2015.zip: 
	$(get)$@ \
	$@.download
	mv $@.download $@

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

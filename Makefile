#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/
database_get = aws s3 cp s3://landuse/spandex/outputs/

#the data necessary for simulation.py to run:
data: data/2015_09_01_bayarea_v3.h5 \
data/2015_06_01_osm_bayarea4326.h5 \
data/2015_08_03_tmnet.h5 \
data/2015_12_21_zoning_parcels.csv \
data/02_01_2016_parcels_geography.csv  \
data/2015_08_29_costar.csv

#the database that the h5 file above was exported from:
database_backup:
	mkdir -p database_backup
	aws s3 cp --recursive s3://landuse/spandex/outputs/2015/10/14/18/ database_backup/

cartography: data/simple_parcels.shp

data/2015_09_01_bayarea_v3.h5:
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_06_01_osm_bayarea4326.h5: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/2015_12_21_zoning_parcels.csv:
	$(get)$@ \
	$@.download
	mv $@.download $@

data/02_01_2016_parcels_geography.csv:
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

#a parcels shapefile is available on the land use server in d:/badata/out/summaries/ or box/badata/out/regeneration
data/simple_parcels.zip: 
	$(get)$@ \
	$@.download
	mv $@.download $@

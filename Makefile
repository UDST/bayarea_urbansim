#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/

data: data/2015_06_01_avenodeprice.csv \
data/2015_06_01_bayarea_v3.h5 \
data/2015_06_01_buildings.csv \
data/2015_06_01_nodes.csv \
data/2015_06_01_nodes_prices.csv \
data/2015_06_01_osm_bayarea4326.h5 \
data/2015_06_01_parcels.dbf \
data/2015_06_01_parcels.prj \
data/2015_06_01_parcels.shp \
data/2015_06_01_parcels.shx \
data/2015_06_01_parcels_geography.csv \
data/2015_06_01_parcels_to_zoning.csv \
data/2015_06_01_taz_summary.csv \
data/2015_06_01_zoning_parcels.csv

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

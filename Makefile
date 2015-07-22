#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/data

all:

data/avenodeprice.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/bayarea_v3.h5:
	$(get)$@ \
	$@.download
	mv $@.download $@

data/buildings.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/nodes.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/nodes_prices.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/osm_bayarea4326.h5: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels.dbf: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels.prj: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels.shp: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels.shx: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels_geography.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/parcels_to_zoning.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/pdatargets.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/taz_summary.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

data/zoning_parcels.csv: 
	$(get)$@ \
	$@.download
	mv $@.download $@

#$@ is the file name of the target of the rule. 
#example get from s3:aws s3 cp s3://landuse/zoning/match_fields_tables_zoning_2012_source.csv match_fields_tables_zoning_2012_source.csv
get = aws s3 cp s3://bayarea_urbansim/data

all:

data/avenodeprice.csv:


data/bayarea_v3.h5:
data/buildings.csv:



data/nodes.csv:
data/nodes_prices.csv:
data/osm_bayarea4326.h5:
data/parcels.dbf:
data/parcels.prj:
data/parcels.shp:
data/parcels.shx:
data/parcels_geography.csv:
data/parcels_to_zoning.csv:
data/pdatargets.csv:
data/taz_summary.csv:
data/zoning_parcels.csv:
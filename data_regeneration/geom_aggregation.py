from spandex import TableLoader
from spandex.spatialtoolz import geom_unfilled
from spandex.io import exec_sql

loader = TableLoader()

################
#### Approach 1:  Merge geometries (and aggregate attributes) based on a common identifier
################
print 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) based on a common identifier'

exec_sql("""
-- SCL
drop table if exists condos_scl;

SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
condo_identifier,
ST_CollectionExtract(ST_Multi(ST_Union(geom)), 3) AS geom,
max(gid) as gid,
'merged'  as imputation_flag,
max(development_type_id) as development_type_id
into condos_scl
FROM parcels
where county_id = '085' AND land_use_type_id = 'RCON' AND length(condo_identifier)>3
GROUP BY condo_identifier;

delete from parcels where county_id = '085' AND land_use_type_id = 'RCON' AND length(condo_identifier)>3 ;

insert into parcels select * from condos_scl;

-- CNC
drop table if exists condos_cnc;

SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
condo_identifier,
ST_CollectionExtract(ST_Multi(ST_Union(geom)), 3) AS geom,
max(gid) as gid,
'merged'  as imputation_flag,
max(development_type_id) as development_type_id
into condos_cnc
FROM parcels
where county_id = '013' AND land_use_type_id = '29' AND length(condo_identifier)>3
GROUP BY condo_identifier;

delete from parcels where county_id = '013' AND land_use_type_id = '29' AND length(condo_identifier)>3 ; 

insert into parcels select * from condos_cnc;

-- SON

drop table if exists condos_son;

SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
condo_identifier,
ST_CollectionExtract(ST_Multi(ST_Union(geom)), 3) AS geom,
max(gid) as gid,
'merged'  as imputation_flag,
max(development_type_id) as development_type_id
into condos_son
FROM parcels
where county_id = '097' AND length(condo_identifier)>3 and res_type = 'multi'
GROUP BY condo_identifier;

delete from parcels where county_id = '097' AND length(condo_identifier)>3 and res_type = 'multi';

insert into parcels select * from condos_son;
""")


################
#### Approach 2:  Merge geometries (and aggregate attributes) based on within-interior-ring status
################
print 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) based on within-interior-ring status'

exec_sql("""
drop table if exists unfilled;
drop table if exists unfilled_exterior;
drop table if exists aggregation_candidates;
drop table if exists parcels_small;
""")

loader.database.refresh()
t = loader.tables

##Identify parcels with interior rings.  This is an indication of encircling common space that is typical in condo projects.
df = geom_unfilled(t.public.parcels, 'unfilled')

exec_sql("""
ALTER TABLE unfilled
    ALTER COLUMN geom TYPE geometry(MultiPolygon) USING ST_Multi(geom);
SELECT UpdateGeometrySRID('unfilled', 'geom', 2768);
""")

#Calculate area and delete exterior polygons below certain threshold size 
exec_sql("""
ALTER TABLE parcels ADD COLUMN calc_area numeric;
UPDATE parcels SET calc_area = ST_Area(geom);
select * into parcels_small from parcels where (calc_area < 550000) and res_type='multi';
ALTER TABLE parcels_small ADD PRIMARY KEY (gid);
CREATE INDEX small_parcel_gidx on parcels_small using gist (geom);
""")

exec_sql("""
ALTER TABLE unfilled ADD COLUMN calc_area numeric;
UPDATE unfilled SET calc_area = ST_Area(geom);
delete from unfilled where calc_area > 550000;
""")

exec_sql("""
SELECT gid, ST_Collect(ST_MakePolygon(geom)) As geom
into unfilled_exterior
FROM (
    SELECT gid, ST_ExteriorRing((ST_Dump(geom)).geom) As geom
    FROM unfilled
    ) s
GROUP BY gid;
ALTER TABLE unfilled_exterior ADD PRIMARY KEY (gid);
CREATE INDEX exterior_gidx on unfilled_exterior using gist (geom);
""")

exec_sql("""
with a as(
SELECT a.*, b.gid as parent_gid FROM parcels_small a, unfilled_exterior b WHERE ST_Contains(b.geom, a.geom)
)
select distinct * into aggregation_candidates from a;
""")

exec_sql("""

delete from parcels where gid in (select distinct gid from aggregation_candidates);

with a as(
SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
max(condo_identifier) as condo_identifier,
ST_CollectionExtract(ST_Multi(ST_Union(geom)), 3) AS geom,
gid,
max(development_type_id) as development_type_id,
max(parent_gid) as parent_gid
FROM aggregation_candidates
GROUP BY gid
), b as(
SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
max(condo_identifier) as condo_identifier,
ST_CollectionExtract(ST_Multi(ST_Union(geom)), 3) AS geom,
max(gid) as gid,
'merged'  as imputation_flag,
max(development_type_id) as development_type_id
FROM a
GROUP BY parent_gid
)
insert into parcels
select * from b;
""")


################
#### Approach 3:  Merge geometries (and aggregate attributes) if duplicate stacked parcel geometry
################
print 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) if duplicate stacked parcel geometry'

print 'Collapsing and aggregating stacked parcels'
exec_sql("""
drop table if exists stacked;
drop table if exists stacked_merged;

SELECT * into stacked FROM parcels
where geom in (select geom from parcels
group by geom having count(*) > 1);

SELECT 
max(county_id) as county_id,
max(apn) as apn,
max(parcel_id_local) as parcel_id_local,
max(land_use_type_id) as land_use_type_id,
max(res_type) as res_type,
sum(land_value) as land_value,
sum(improvement_value) as improvement_value,
max(year_assessed) as year_assessed,
max(year_built) as year_built,
sum(building_sqft) as building_sqft,
sum(non_residential_sqft) as non_residential_sqft,
sum(residential_units) as residential_units,
max(sqft_per_unit) as sqft_per_unit,
max(stories) as stories,
max(tax_exempt) as tax_exempt,
max(condo_identifier) as condo_identifier,
geom,
max(gid) as gid,
'merged'  as imputation_flag,
max(development_type_id) as development_type_id
into stacked_merged
FROM stacked
GROUP BY geom;

delete from parcels where gid in (select distinct gid from stacked);

insert into parcels
select * from stacked_merged;
""")
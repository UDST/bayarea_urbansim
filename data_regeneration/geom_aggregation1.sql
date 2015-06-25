/*################
#### Approach 1:  Merge geometries (and aggregate attributes) based on a common identifier
################
*/
\ECHO 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) based on a common identifier'

create table parcel_backup as select * from parcels;
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
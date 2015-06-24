from spandex import TableLoader
from spandex.spatialtoolz import geom_unfilled
from spandex.io import exec_sql

loader = TableLoader()

################
#### Approach 2:  Merge geometries (and aggregate attributes) based on within-interior-ring status
################
print 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) based on within-interior-ring status'


drop table if exists unfilled;
drop table if exists unfilled_exterior;
drop table if exists aggregation_candidates;
drop table if exists parcels_small;


loader.database.refresh()
t = loader.tables

##Identify parcels with interior rings.  This is an indication of encircling common space that is typical in condo projects.
df = geom_unfilled(t.public.parcels, 'unfilled')

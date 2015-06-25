from spandex import TableLoader
from spandex.spatialtoolz import geom_unfilled
from spandex.io import exec_sql

loader = TableLoader()

################
#### Approach 2:  Merge geometries (and aggregate attributes) based on within-interior-ring status
################
print 'PARCEL AGGREGATION:  Merge geometries (and aggregate attributes) based on within-interior-ring status'

loader.database.refresh()
t = loader.tables

##Identify parcels with interior rings.  This is an indication of encircling common space that is typical in condo projects.
df = geom_unfilled(t.public.parcels, 'unfilled')

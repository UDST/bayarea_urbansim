#!/usr/bin/env python

import os
import subprocess
import sys

from spandex import TableLoader


python = sys.executable
root_path = os.path.dirname(__file__)


def run(filename):
    """"Run Python file relative to script without blocking."""
    path = os.path.join(root_path, filename)
    return subprocess.Popen([python, path])


def check_run(filename):
    """Run Python file relative to script, block, assert exit code is zero."""
    path = os.path.join(root_path, filename)
    return subprocess.check_call([python, path])


print("PREPROCESSING: Loading shapefiles by county.")

# Load shapefile data inputs, fix invalid geometries, and reproject.
check_run('load.py')


print("PROCESSING: Loading parcel attributes by county.")

# Run county attribute processing scripts.
county_names = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt', 'sol', 'son']
for name in county_names:
    filename = os.path.join('counties', name + '.py')
    check_run(filename)


print("PROCESSING: Combining to create regional parcels table.")

# Join the county attributes and geometries and union the counties together.
loader = TableLoader()
sql_path = os.path.join(root_path, 'join_counties.sql')
with open(sql_path) as sql:
    with loader.database.cursor() as cur:
        cur.execute(sql.read())
        
        
print("PROCESSING: Aggregating condos and stacked parcels.")
        
# Aggregate parcels for multi-geometry representation of buildings
check_run('geom_aggregation.py')


print("PROCESSING: Imputing attributed based on POINT data sources.")
        
# Applying information based on Redfin, Costar, employment points etc.
check_run('point_based_imputation.py')


print("POSTPROCESSING: Applying spatial operations: tag with location identifiers.")

# Apply spatial operations to append location identifiers.
check_run('spatialops.py')


print("PROCESSING: Synthesizing/scaling to match aggregate zonal totals.")

# Matching aggregate zonal totals
check_run('match_aggregate.py')


print("PROCESSING: Adding/populating price fields to the building table.")

# Imputing base-year residential and non-residential prices
check_run('price_imputation.py')


print("PROCESSING: Allocating households and jobs to buildings.")

# Allocating households and jobs to buildings
check_run('demand_agent_allocation.py')


print("PROCESSING: Attaching zone_id's to parcels.")

# Reading the zoning inputs and joining/appending to parcels based on geom_id
check_run('attach_zoning_id.py')


print("SUMMARIZING: Generating data summaries.")

# Output summary CSV files by county and TAZ.
check_run('summaries.py')


# print("SUMMARIZING: Exporting to HDF5 and UrbanCanvas db.")

# Output core tables to HDF5 for UrbanSim.
check_run('export_to_h5.py')

# # Output buildings, parcels, and zoning tables to UrbanCanvas db.
check_run('export_to_uc.py')

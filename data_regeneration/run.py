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
# aka load_county_parcel_data_to_staging
check_run('load.py')


print("PROCESSING: Loading parcel attributes by county.")

# Run county attribute processing scripts.
# aka homogenize_and_clean_county_parcel_attributes
# a lot of these scripts drop duplicate APN parcels
county_names = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt', 'sol', 'son']
for name in county_names:
    filename = os.path.join('counties', name + '.py')
    check_run(filename)


print("PROCESSING: Combining to create regional parcels table.")

# Join the county attributes and geometries and union the counties together.
# aka merge_county_parcel_tables_into_one_spatial_table
loader = TableLoader()
sql_path = os.path.join(root_path, 'join_counties.sql')
with open(sql_path) as sql:
    with loader.database.cursor() as cur:
        cur.execute(sql.read())
        
        
print("PROCESSING: Aggregating condos and stacked parcels.")
        
# Aggregate parcels for multi-geometry representation of buildings
# aka spatially_combine_those_parcels
# this attempts to combine parcel geometries in ways
# that might relate to the way a developer would think about them
# filling in common spaces around buildings, combining condos together
check_run('geom_aggregation.py')


print("PROCESSING: Imputing attributed based on POINT data sources.")
        
# Applying information based on Redfin, Costar, employment points etc.
# aka assign_historic_sales_data_to_parcels_by_spatial_proximity
# assigns redfin and costar data to parcels
# this also does various checks for the reasonableness of parcel attributes
# (for example, year built within a reasonable year, price reasonable, etc)
# and then it also seems to make some decisions about when to assign
# the redfin sale price to a parcel
check_run('point_based_imputation.py')


print("POSTPROCESSING: Applying spatial operations: tag with location identifiers.")

# Apply spatial operations to append location identifiers.
# aka tag parcels with administrative names (e.g. TAZ number)
check_run('spatialops.py')


print("PROCESSING: Synthesizing/scaling to match aggregate zonal totals.")

# Matching aggregate zonal totals
# aka move_jobs_and_houses_to_match_aggregate_taz_2010_totals_csv
check_run('match_aggregate.py')


print("PROCESSING: Adding/populating price fields to the building table.")

# Imputing base-year residential and non-residential prices
# aka predict_prices_of_buildings_and_parcels_with_regressions
check_run('price_imputation.py')


print("PROCESSING: Allocating households and jobs to buildings.")

# Allocating households and jobs to buildings
# aka put_people_jobs_on_parcels
check_run('demand_agent_allocation.py')


print("SUMMARIZING: Generating data summaries.")

# Output summary CSV files by county and TAZ.
# aka summarize_by_taz_and_county
check_run('summaries.py')

# Output core tables to HDF5 for UrbanSim.
# aka export_database_to_h5
check_run('export_to_h5.py')

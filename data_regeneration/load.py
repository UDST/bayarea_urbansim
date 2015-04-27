import logging

import pandas as pd
from spandex import TableLoader
from spandex.io import df_to_db, exec_sql, logger
from spandex.spatialtoolz import conform_srids


logger.setLevel(logging.INFO)


shapefiles = {
    #'staging.controls_blocks':
    #'hh/control_sm/block10_gba.shp',

    #'staging.controls_blockgroups':
    #'hh/control_sm/blockgroup10_gba.shp',

    #'staging.nat_farms':
    #'nat/farm/williamson_act.shp',

    #'staging.nat_slopes_gt6':
    #'nat/slope/gt6pctslope_1km.shp',

    #'staging.nat_slopes_gt12':
    #'nat/slope/gt12pctslope_1km',

    #'staging.nat_water':
    #'nat/water/bayarea_allwater.shp',

    #'staging.nat_water_wetlands':
    #'nat/wetlands/wetlands.shp',

    'staging.parcels_ala':
    'built/parcel/2010/ala/parcelsAlaCo2010/asr_parcel.shp',

    'staging.parcels_cnc_poly':
    'built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoly_0410.shp',

    'staging.parcels_cnc_pt':
    'built/parcel/2010/cnc/raw10/CAD_AO_ParcelPoints_int0410.shp',

    'staging.parcels_nap':
    'built/parcel/2010/nap/Napa_Parcels.shp',

    'staging.parcels_nap_tract':
    'built/parcel/2010/nap/Napa_Census_tract.shp',

    'staging.parcels_mar':
    'built/parcel/2005/parcels2005_mar.shp',

    'staging.parcels_scl':
    'built/parcel/2010/scl/parcels2010_scl.shp',

    'staging.parcels_sfr':
    'built/parcel/2010/sfr/parcels2010_sfr.shp',

    'staging.parcels_smt':
    'built/parcel/2010/smt/shapefiles/ACTIVE_PARCELS_APN.shp',

    'staging.parcels_sol':
    'built/parcel/2010/sol/Parcels.shp',

    'staging.parcels_sol_zoning':
    'built/parcel/2010/sol/zoning.shp',

    'staging.parcels_son':
    'built/parcel/2010/son/PAR_PARCELS.shp',

    # Geometry type is MultiPolygonZM.
    #'staging.parcels_son_exlu':
    #'built/parcel/2010/son/parcels2010_son/Final2010exlu.shp',

    'staging.taz':
    'juris/reg/zones/taz1454.shp',
    
    'staging.establishment_points':
    'emp/micro/est10_gt1/est10_esri_gt1.shp',
}


# Install PostGIS and create staging schema.
loader = TableLoader()
with loader.database.cursor() as cur:
    cur.execute("""
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE SCHEMA IF NOT EXISTS staging;
    """)
loader.database.refresh()

# Load shapefiles specified above to the project database.
loader.load_shp_map(shapefiles)

# Fix invalid geometries and reproject.
staging = loader.tables.staging
conform_srids(loader.srid, schema=staging, fix=True)

# Load county land use code mapping.
csv = loader.get_path('built/parcel/2010/rtp13_processing_notes/lucodes.csv')
df = pd.read_csv(csv, dtype=str)
df.dropna(how='any', inplace=True,
          subset=['county_id', 'land_use_type_id', 'development_type_id'])
df.index.name = 'index'
df_to_db(df, 'lucodes', schema=staging)

# Add county land use code mapping unique constraint.
exec_sql("""
ALTER TABLE staging.lucodes ADD CONSTRAINT lucodes_unique
UNIQUE (county_id, land_use_type_id);
""")
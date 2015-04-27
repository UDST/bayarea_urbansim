import os
import pandas as pd
from spandex import TableLoader, TableFrame
from spandex.utils import load_config
from spandex.io import exec_sql
import pandas.io.sql as sql

# Build parcels TableFrame.
loader = TableLoader()
table = loader.database.tables.public.parcels
tf = TableFrame(table, index_col='gid')

# Load TAZ residential unit control totals.
taz_controls_csv = loader.get_path('hh/taz2010_imputation.csv')
targetunits = pd.read_csv(taz_controls_csv, index_col='taz1454')['targetunits']

# Get CSV output file directory.
output_dir = loader.get_path('out/regeneration/summaries')

# Generate summary CSV by county and TAZ.
for grouper in ['county_id', 'taz']:
    df = tf[[grouper, 'non_residential_sqft', 'residential_units']]
    df.dropna(subset=[grouper], inplace=True)

    if grouper == 'taz':
        df[grouper] = df[grouper].astype(int)

    df['count'] = 1
    summary = df.groupby(grouper).sum()

    if grouper == 'taz':
        summary['residential_units_target'] = targetunits
        taz_df = summary

    output_filename = os.path.join(output_dir,
                                   'summary_{}.csv'.format(grouper))
    summary.to_csv(output_filename)

parcel_output_dir = loader.get_path('out/regeneration/summaries/parcels')

config = load_config()
db_config = dict(config.items('database'))

exec_sql("ALTER TABLE parcel RENAME COLUMN taz_id to zone_id;")
exec_sql("ALTER TABLE parcel RENAME COLUMN parcel_acres to acres;")

##  Export parcel shapefile to output directory
os.system('pgsql2shp -f "%s" -h %s -u %s -P %s %s parcel' % (parcel_output_dir, db_config['host'], db_config['user'], db_config['password'], db_config['database']))

##  Export buildings as csv
building_output_path = loader.get_path('out/regeneration/summaries/buildings.csv')

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)
    
buildings = db_to_df('select * from building').set_index('building_id')
buildings.to_csv(building_output_path)
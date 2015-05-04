### UrbanCanvas

import os
from spandex import TableLoader
from spandex.io import exec_sql #spandex localhost exec_sql func
from spandex.utils import load_config
import psycopg2

loader = TableLoader()

## spandex localhost db config
db_config = dict(load_config().items('database'))  

## UrbanCanvas db config
urbancanvas_db_config = {'database': 'mtc',
                         'host': '67.225.185.54',
                         'password': 'Urb27MTC',
                         'port': '5432',
                         'user': 'mtc'}

## if 'loading' schema not on localhost db, create.  This schema is for tables to load to UrbanCanvas
exec_sql("CREATE SCHEMA IF NOT EXISTS loading;")  

#UrbanCanvas exec_sql func, for executing sql on UrbanCanvas database
def exec_sql2(query):
    print query
    conn_string = "host=67.225.185.54 dbname='mtc' user='mtc' password='Urb27MTC' port=5432"
    import psycopg2
    conn=psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

"""
Exports specified tables from localhost database to the UrbanCanvas
database. Assumes the spandex data directory has an 'out' folder.
A 'loading' schema must exist on both localhost and UrbanCanvas.

Parameters
----------
table_name : str
    Name of table to export to UrbanCanvas.
table_schema : str
    Name of schema on localhost that the table resides in.
localhost_db_config : dict
    Dictionary of localhost database configuration settings.
    Should have the following keys:  'database', 'host', 'user',
    'pass', 'port'.
urbancanvas_db_config : dict
    Dictionary of UrbanCanvas database configuration settings.
    Should have the following keys:  'database', 'host', 'user',
    'pass', 'port'.
exec_sql_localhost_fn : function
    Function that executes sql on localhost db based on sql 
    string argument.
exec_sql_urbancanvas_fn : function
    Function that executes sql on UrbanCanvas db based on sql 
    string argument.
src_table_name : str, Optional
    Name of source table to export to UrbanCanvas db if name of
    table is different on localhost than on UrbanCanvas.

Returns
-------
None

"""
def localhost_to_urbancanvas_db(table_name, table_schema, localhost_db_config, urbancanvas_db_config, exec_sql_localhost_fn, exec_sql_urbancanvas_fn, src_table_name = None):
    exec_sql_localhost_fn("drop table if exists loading.%s;" % table_name)

    if src_table_name is not None:
        exec_sql_localhost_fn("SELECT * INTO loading.%s FROM %s.%s;" % (table_name, table_schema, src_table_name))
    else:
        exec_sql_localhost_fn("SELECT * INTO loading.%s FROM %s.%s;" % (table_name, table_schema, table_name))

    postgres_backup = loader.get_path('out/%s.backup' % table_name)
    os.system('pg_dump --host %s --port %s --username "%s" --format custom --verbose --file "%s" --table "loading.%s" "%s"' % 
              (db_config['host'], db_config['port'], db_config['user'], postgres_backup, table_name, db_config['database'])) 

    exec_sql_urbancanvas_fn("drop table if exists loading.%s;" % table_name)

    os.system('pg_restore --host %s --port %s --username "%s" --dbname "%s" --role "%s" --no-password  --verbose "%s"' % 
              (urbancanvas_db_config['host'], urbancanvas_db_config['port'], urbancanvas_db_config['user'], urbancanvas_db_config['database'], urbancanvas_db_config['user'], postgres_backup))


## Export tables from localhost database to UrbanCanvas database
localhost_to_urbancanvas_db('zoning', 'public', db_config, urbancanvas_db_config, exec_sql, exec_sql2)
localhost_to_urbancanvas_db('building', 'public', db_config, urbancanvas_db_config, exec_sql, exec_sql2)
localhost_to_urbancanvas_db('parcel', 'public', db_config, urbancanvas_db_config, exec_sql, exec_sql2)


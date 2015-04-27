from spandex import TableLoader
from spandex.spatialtoolz import tag
from spandex.io import exec_sql
import pandas.io.sql as sql

###Database connection
loader = TableLoader()
t = loader.tables

###Tagging parcels with taz id based on point in poly
tag(t.public.parcels, 'taz', t.staging.taz, 'taz_key')


###Deal with special cases where parcels do not fall within TAZ boundaries
def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)
    
exec_sql("""
CREATE OR REPLACE FUNCTION expandoverlap_metric(a geometry, b geometry, maxe double precision, maxslice double precision) RETURNS integer AS $BODY$ BEGIN FOR i IN 0..maxslice LOOP IF st_expand(a,maxe*i/maxslice) && b THEN RETURN i; END IF; END LOOP; RETURN 99999999; END; $BODY$ LANGUAGE plpgsql IMMUTABLE COST 100; ALTER FUNCTION expandoverlap_metric(geometry, geometry, double precision, double precision) OWNER TO postgres
""")

if db_to_df("select exists (select 1 from pg_type where typname = 'pgis_nn');").values[0][0] == False:
    exec_sql("CREATE TYPE pgis_nn AS (nn_gid integer, nn_dist numeric(16,5))")
    
exec_sql("""
CREATE OR REPLACE FUNCTION _pgis_fn_nn(geom1 geometry, distguess double precision, numnn integer, maxslices integer, lookupset character varying, swhere character varying, sgid2field character varying, sgeom2field character varying) RETURNS SETOF pgis_nn AS $BODY$ DECLARE strsql text; rec pgis_nn; ncollected integer; it integer; BEGIN ncollected := 0; it := 0; WHILE ncollected < numnn AND it <= maxslices LOOP strsql := 'SELECT currentit.' || sgid2field || ', st_distance(ref.geom, currentit.' || sgeom2field || ') as dist FROM ' || lookupset || '  as currentit, (SELECT geometry(''' || CAST(geom1 As text) || ''') As geom) As ref WHERE ' || swhere || ' AND st_distance(ref.geom, currentit.' || sgeom2field || ') <= ' || CAST(distguess As varchar(200)) || ' AND st_expand(ref.geom, ' || CAST(distguess*it/maxslices As varchar(100)) ||  ') && currentit.' || sgeom2field || ' AND expandoverlap_metric(ref.geom, currentit.' || sgeom2field || ', ' || CAST(distguess As varchar(200)) || ', ' || CAST(maxslices As varchar(200)) || ') = ' || CAST(it As varchar(100)) || ' ORDER BY st_distance(ref.geom, currentit.' || sgeom2field || ') LIMIT ' ||  CAST((numnn - ncollected) As varchar(200)); FOR rec in EXECUTE (strsql) LOOP IF ncollected < numnn THEN ncollected := ncollected + 1; RETURN NEXT rec; ELSE EXIT; END IF; END LOOP; it := it + 1; END LOOP; END $BODY$ LANGUAGE plpgsql STABLE COST 100 ROWS 1000; ALTER FUNCTION _pgis_fn_nn(geometry, double precision, integer, integer, character varying, character varying, character varying, character varying) OWNER TO postgres
""")

exec_sql("""
CREATE OR REPLACE FUNCTION pgis_fn_nn(geom1 geometry, distguess double precision, numnn integer, maxslices integer, lookupset character varying, swhere character varying, sgid2field character varying, sgeom2field character varying) RETURNS SETOF pgis_nn AS $BODY$ SELECT * FROM _pgis_fn_nn($1,$2, $3, $4, $5, $6, $7, $8); $BODY$ LANGUAGE sql STABLE COST 100 ROWS 1000; ALTER FUNCTION pgis_fn_nn(geometry, double precision, integer, integer, character varying, character varying, character varying, character varying) OWNER TO postgres
""")

exec_sql('drop table if exists parcels_outside_taz;')

exec_sql('SELECT * into parcels_outside_taz FROM parcels where taz is null;')


exec_sql("""
update parcels_outside_taz set taz = nn.nn_gid from 
(select parcels_outside_taz.*, 
(pgis_fn_nn(parcels_outside_taz.geom, 10000, 1, 1, 'staging.taz', 'true', 'taz_key', 'geom')).* 
from parcels_outside_taz)
as nn where parcels_outside_taz.gid = nn.gid;
""")

exec_sql("update parcels set taz = a.taz from parcels_outside_taz a where parcels.gid = a.gid;")
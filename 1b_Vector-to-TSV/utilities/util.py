import os
import subprocess
import psycopg2


def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings


def export(table_name, tile, creds):

    shp_dir = os.path.join(tile.out_dir, 'shp')

    if not os.path.exists(shp_dir):
        os.mkdir(shp_dir)

    shp_filename = tile.tile_id + '.shp'
    # pgsql2shp -u gis -P gis -h localhost gis wdpa_protected_areas_00n_000e
    to_shp_cmd = ['pgsql2shp', '-u', creds['user'], '-P', creds['password'], '-h', 'localhost',
                  creds['dbname'], table_name.lower(), '-f', shp_filename]
    print to_shp_cmd

    # for some reason can't specify a full output path, just a filename
    # to choose dir, set it to CWD
    subprocess.check_call(to_shp_cmd, cwd=shp_dir)

    out_shp = os.path.join(shp_dir, table_name.lower() + '.shp')
    out_geojson = os.path.join(tile.out_dir, tile.tile_id + '.geojson')

    # this will ultimately be a TSV, using geojson for now to QC
    to_geojson_cmd = ['ogr2ogr', '-f', 'GeoJSON', out_geojson, out_shp]
    print to_geojson_cmd

    subprocess.check_call(to_geojson_cmd)


def boundary_field_to_sql(field_name):

    if 'boundary_field' in field_name:
        field_name = '1 AS ' + field_name

    return field_name


def clip(table_name, tile, creds):

    clip_dir = os.path.join(tile.out_dir, 'clip')

    if not os.path.exists(clip_dir):
        os.mkdir(clip_dir)

    clip_tablename = table_name + '_clip'

    conn_str = 'PG:user={} password={} dbname={} host={}'.format(creds['user'], creds['password'], creds['dbname'], creds['host'])

    bbox_list = [str(x) for x in tile.bbox]

    col_str = ', '.join([boundary_field_to_sql(field) for field in tile.col_list])

    #TODO fix this so that each tile knows if it needs to save any values (plantation name/type or wpdapid etc)

    # ogr2ogr -f PostgreSQL PG:"user=charlie password=charlie dbname=charlie" ~/Desktop/data/wdpa_protected_areas.shp -clipsrc 145 -39 146 -38 -nln wdpa_clip
    cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', clip_tablename, '-nlt', 'PROMOTE_TO_MULTI',
           '-sql', "SELECT '{0}', {1} FROM {0}".format(tile.dataset_name, col_str), '-lco', 'geometry_name=geom', '-overwrite',
            '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-clipsrc'] + bbox_list

    print cmd

    subprocess.check_call(cmd)

    return clip_tablename


def table_has_rows(cursor, table_name):

    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows


def postgis_intersect(tile):

    # conn = psycopg2.connect("host=localhost dbname=gis user=gis password=gis")
    creds = {'host': 'localhost', 'password': 'gis', 'dbname': 'gis', 'user': 'gis'}

    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # table_name = '{}_{}'.format(tile.dataset_name, tile.tile_id)
    table_name = '{}_{}'.format(tile.dataset_name, tile.tile_id).replace('-', 'x')

    # run ogr2ogr first to clip the tile
    clip_table = clip(table_name, tile, creds)

    if table_has_rows(cursor, clip_table):

        admin2_columns = ['ISO', 'ID_1', 'ID_2']
        groupby_columns = ", ".join(admin2_columns + tile.col_list)
        print groupby_columns

        sql = ("CREATE TABLE {table_name} AS "
               "SELECT {fields}, (ST_Dump(ST_Union(ST_MakeValid(ST_Intersection(ST_MakeValid(c.geom), b.geom))))).geom as geom "
               "FROM {clip_table} c, adm2_final b "
               "WHERE ST_Intersects(c.geom, b.geom) AND ST_GeometryType(c.geom) IN ('ST_Polygon', 'ST_MultiPolygon') "
               "GROUP BY {fields};".format(table_name=table_name, clip_table=clip_table, fields=groupby_columns))

        print sql

        cursor.execute(sql)
        conn.commit()

        if table_has_rows(cursor, table_name):
            export(table_name, tile, creds)

        cursor.execute('DROP TABLE {}'.format(table_name))

    cursor.execute('DROP TABLE {}'.format(clip_table))
    conn.commit()

    conn.close()



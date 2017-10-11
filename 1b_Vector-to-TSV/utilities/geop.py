import os
import subprocess
import psycopg2

import util


def clip(table_name, tile, creds):

    clip_dir = os.path.join(tile.out_dir, 'clip')

    if not os.path.exists(clip_dir):
        os.mkdir(clip_dir)

    clip_tablename = table_name + '_clip'

    conn_str = 'PG:user={user} password={password} dbname={dbname} host={host}'.format(**creds)

    bbox_list = [str(x) for x in tile.bbox]

    col_str = ', '.join([util.boundary_field_to_sql(field) for field in tile.col_list])

    # ogr2ogr -f PostgreSQL PG:"user=charlie password=charlie dbname=charlie" ~/Desktop/data/wdpa_protected_areas.shp
    # -clipsrc 145 -39 146 -38 -nln wdpa_clip
    cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', clip_tablename, '-nlt', 'PROMOTE_TO_MULTI',
           '-sql', "SELECT '{0}', {1} FROM {0}".format(tile.dataset_name, col_str), '-lco', 'geometry_name=geom',
           '-overwrite', '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-clipsrc'] + bbox_list

    print cmd

    subprocess.check_call(cmd)

    return clip_tablename


def postgis_intersect(q):

    # source: https://pymotw.com/2/Queue/
    while True:
        tile = q.get()

        creds = util.get_creds()

        conn = psycopg2.connect(**creds)
        cursor = conn.cursor()

        # table_name = '{}_{}'.format(tile.dataset_name, tile.tile_id)
        table_name = '{}_{}'.format(tile.dataset_name, tile.tile_id).replace('-', 'x')

        # run ogr2ogr first to clip the tile
        clip_table = clip(table_name, tile, creds)

        if util.table_has_rows(cursor, clip_table):

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

            if util.table_has_rows(cursor, table_name):
                util.export(table_name, tile, creds)

            cursor.execute('DROP TABLE {}'.format(table_name))

        cursor.execute('DROP TABLE {}'.format(clip_table))
        conn.commit()

        conn.close()

        q.task_done()

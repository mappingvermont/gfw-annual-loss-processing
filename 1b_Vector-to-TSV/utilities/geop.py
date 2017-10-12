import os
import subprocess
import psycopg2
import logging

import util
from tile import Tile


def clip(q):

    while True:
        tile = q.get()
        tile.postgis_table = '_'.join([tile.dataset_name, tile.tile_id,  'clip'])

        creds = util.get_creds()
        conn_str = 'PG:user={user} password={password} dbname={dbname} host={host}'.format(**creds)

        bbox_list = [str(x) for x in tile.bbox]

        col_str = ', '.join([util.boundary_field_to_sql(field) for field in tile.col_list])

        ogr2ogr_layer_name = os.path.splitext(os.path.basename(tile.dataset))[0]

        cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', tile.postgis_table, '-nlt', 'PROMOTE_TO_MULTI',
               '-sql', "SELECT '{0}', {1} FROM {0}".format(ogr2ogr_layer_name, col_str), '-lco', 'geometry_name=geom',
               '-overwrite', '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-clipsrc'] + bbox_list

        print cmd

        subprocess.check_call(cmd)

        q.task_done()


def intersect_layers(q):

    # source: https://pymotw.com/2/Queue/
    while True:
        output_layer, tile1, tile2 = q.get()

        creds = util.get_creds()

        conn = psycopg2.connect(**creds)
        cursor = conn.cursor()

        table_name = '{}_{}_{}'.format(tile1.postgis_table, tile2.postgis_table, tile1.tile_id)


        if util.table_has_rows(cursor, tile1.postgis_table):

            admin2_columns = ['ISO', 'ID_1', 'ID_2']
            groupby_columns = ", ".join(admin2_columns + tile1.col_list + tile2.col_list)
            print groupby_columns

            sql = ("CREATE TABLE {table_name} AS "
                   "SELECT {fields}, (ST_Dump(ST_Union(ST_Buffer(ST_MakeValid(ST_Intersection(ST_MakeValid("
                   "c.geom), b.geom)), 0.0000001)))).geom as geom "
                   "FROM {table1} c, {table2} b "
                   "WHERE ST_Intersects(c.geom, b.geom) AND "
                   "ST_GeometryType(c.geom) IN ('ST_Polygon', 'ST_MultiPolygon') "
                   "GROUP BY {fields};".format(table_name=table_name, table1=tile1.postgis_table,
                                               table2=tile2.postgis_table, fields=groupby_columns))

            print sql

            valid_intersect = True

            try:
                cursor.execute(sql)
                logging.info('Intersect for {} successful'.format(table_name))
            except Exception, e:
                valid_intersect = False
                logging.error('Error {} in sql statement {}'.format(sql, e))

            if valid_intersect:
                conn.commit()

                if util.table_has_rows(cursor, table_name):
                    output_tile = Tile(None, None, None, None, table_name)
                    output_layer.tile_list.append(output_tile)

        cursor.execute('DROP TABLE {}'.format(tile1.postgis_table))
        conn.commit()

        conn.close()

        q.task_done()

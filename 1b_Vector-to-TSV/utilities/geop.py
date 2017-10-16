import os
import subprocess
import psycopg2
import logging
import pandas as pd

import util, tile, layer


def clip(q):

    while True:
        tile = q.get()
        tile.postgis_table = '_'.join([tile.dataset_name, tile.tile_id,  'clip'])

        conn_str = util.build_ogr_pg_conn()
        bbox_list = [str(x) for x in tile.bbox]

        col_str = ', '.join([util.boundary_field_to_sql(field) for field in tile.col_list])

        ogr2ogr_layer_name = os.path.splitext(os.path.basename(tile.dataset))[0]
        sql = "SELECT '{0}', {1} FROM {0}".format(ogr2ogr_layer_name, col_str)

        cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', tile.postgis_table,
               '-nlt', 'PROMOTE_TO_MULTI','-sql', sql, '-lco', 'geometry_name=geom', '-overwrite',
               '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-dim', '2', '-clipsrc'] + bbox_list

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
                    output_tile = tile.Tile(None, None, tile1.tile_id, tile1.bbox, table_name)
                    output_layer.tile_list.append(output_tile)

        cursor.execute('DROP TABLE {}'.format(tile1.postgis_table))
        conn.commit()

        conn.close()

        q.task_done()


def export(layer_dir, output_name, tile, output_format):

    conn_str = util.build_ogr_pg_conn()

    cmd = ['ogr2ogr', '-f']
    sql = 'SELECT * FROM {}'.format(tile.postgis_table)

    if output_format in ['geojson', 'shp']:
        output_lkp = {'shp': 'ESRI Shapefile','geojson': 'GeoJSON'}
        output_str = output_lkp[output_format]

        cmd += [output_str]

        output_path = os.path.join(layer_dir, '{}__{}.{}'.format(output_name, tile.tile_id, output_format))
        tile.final_output = output_path

        cmd += [output_path, conn_str, '-sql', sql]
        print cmd

        subprocess.check_call(cmd)

    else:
        export_tsv(layer_dir, output_name, tile)

    if tile.postgis_table:
        util.drop_table(tile.postgis_table)


def export_tsv(layer_dir, output_name, tile):

    cmd = ['ogr2ogr', '-f', 'CSV', '-lco', 'GEOMETRY=AS_WKT']

    # for some reason ogr2ogr wants to create a directory and THEN the CSV
    output_path = os.path.join(layer_dir, '{}'.format(tile.tile_id))
    cmd += [output_path]

    # if we're exporting already clipped data from PostGIS . . .
    if tile.postgis_table:

        conn_str = util.build_ogr_pg_conn()

        sql = 'SELECT * FROM {}'.format(tile.postgis_table)
        cmd += [conn_str, '-sql', sql, '-lco', 'GEOMETRY_NAME=geom']

        csv_output = os.path.join(output_path, 'sql_statement.csv')

    # or if we're clipping an existing TSV
    else:
        cmd += [tile.dataset, '-clipsrc'] + [str(x) for x in tile.bbox]
        csv_output = os.path.join(output_path, 'data.csv')

    print cmd
    subprocess.check_call(cmd)

    df = pd.read_csv(csv_output)

    tsv_output = os.path.join(layer_dir, '{}__{}.tsv'.format(output_name, tile.tile_id))
    df.to_csv(tsv_output, sep='\t', header=None, index=False)

    tile.final_output = tsv_output


def intersect_gadm(source_layer, gadm_layer):

    input_list = []

    output_layer = layer.Layer(None, [])

    for t in source_layer.tile_list:
        input_list.append((output_layer, t, gadm_layer.tile_list[0]))

    util.exec_multiprocess(intersect_layers, input_list)

    return output_layer

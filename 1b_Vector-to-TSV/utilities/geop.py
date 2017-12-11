import os
import subprocess
import psycopg2
import logging
import pandas as pd

import util, tile, layer


def clip(q):

    while True:
        tile = q.get()

        conn_str = util.build_ogr_pg_conn()
        col_str = util.boundary_field_dict_to_sql_str(tile.col_list)

        if not tile.postgis_table:
            dataset_name = os.path.splitext(os.path.basename(tile.dataset))[0]
            tile.postgis_table = '_'.join([dataset_name, tile.tile_id,  'clip'])

        # any TSV name will have the data as it's layer, otherwise use the actual shape
        if os.path.splitext(tile.dataset)[1] == '.shp':
            ogr_layer_name = os.path.splitext(os.path.basename(tile.dataset))[0]
        else:
            ogr_layer_name = 'data'

        sql = "SELECT {}, GEOMETRY FROM {}".format(col_str, ogr_layer_name)

        cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', tile.postgis_table,
               '-nlt', 'PROMOTE_TO_MULTI', '-dialect', 'sqlite', '-sql', sql, '-lco', 'geometry_name=geom',
               '-overwrite', '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-dim', '2']

        if tile.bbox:
            bbox_list = [str(x) for x in tile.bbox]
            cmd += ['-clipsrc'] + bbox_list

        logging.info(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
        for line in iter(p.stdout.readline, b''):
            if 'error' in line:
                logging.error('Error {} in sql statement {}'.format(sql, e))

        q.task_done()

def intersect(q):

    # source: https://pymotw.com/2/Queue/
    while True:
        output_layer, tile1, tile2 = q.get()

        creds = util.get_creds()

        conn = psycopg2.connect(**creds)
        cursor = conn.cursor()

        table_name = '{}_{}_{}'.format(tile1.postgis_table, tile2.postgis_table, tile1.tile_id)

        if util.table_has_rows(cursor, tile1.postgis_table):

            # find which table has ISO/ID_1/ID_2 columns
            # will return ['a.ISO', 'a.ID_1', 'a._ID_2'] or b., depending on if first or second tile has admin columns
            admin2_columns = util.find_admin_columns(cursor, tile1, tile2)

            # tile1 and tile2 could have same column names (boundary_field1, boundary_field2)
            # need to alias them to ensure that they're referenced properly
            tile1_cols = tile1.alias_select_columns('a')
            tile2_cols = tile2.alias_select_columns('b')

            tile_cols_aliased = []

            # rename boundary fields
            for i, fieldname in enumerate(tile1_cols + tile2_cols):
                tile_cols_aliased.append(fieldname + ' AS boundary_field{}'.format(str(i)))

            select_cols = ", ".join(tile_cols_aliased + admin2_columns)
            groupby_columns = ", ".join(tile1_cols + tile2_cols + admin2_columns)

            sql = ("CREATE TABLE {table_name} AS "
                   "SELECT {s}, (ST_Dump(ST_Union(ST_Buffer(ST_MakeValid(ST_Intersection(ST_MakeValid("
                   "a.geom), b.geom)), 0.0000001)))).geom as geom "
                   "FROM {table1} a, {table2} b "
                   "WHERE ST_Intersects(a.geom, b.geom) AND "
                   "ST_GeometryType(a.geom) IN ('ST_Polygon', 'ST_MultiPolygon') "
                   "GROUP BY {g};".format(s=select_cols, table_name=table_name, table1=tile1.postgis_table,
                                               table2=tile2.postgis_table, g=groupby_columns))

            logging.info(sql)

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

        conn.commit()
        conn.close()

        q.task_done()


def export(q):

    while True:
        layer_dir, output_name, tile, output_format = q.get()

        conn_str = util.build_ogr_pg_conn()

        if output_format in ['geojson', 'shp']:

            sql = "SELECT '{}' as table__name, * FROM {}".format(output_name, tile.postgis_table)

            output_lkp = {'shp': 'ESRI Shapefile','geojson': 'GeoJSON'}
            output_str = output_lkp[output_format]

            cmd = ['ogr2ogr', '-f', output_str]

            output_path = os.path.join(layer_dir, '{}__{}.{}'.format(output_name, tile.tile_id, output_format))
            tile.final_output = output_path

            cmd += [output_path, conn_str, '-sql', sql]
            logging.info(cmd)

            subprocess.check_call(cmd)

        else:
            export_tsv(layer_dir, output_name, tile)

        if tile.postgis_table:
            util.drop_table(tile.postgis_table)

        q.task_done()


def export_tsv(layer_dir, output_name, tile):

    # for some reason ogr2ogr wants to create a directory and THEN the CSV
    output_path = os.path.join(layer_dir, '{}'.format(tile.tile_id))

    cmd = ['ogr2ogr', '-f', 'CSV', '-lco', 'GEOMETRY=AS_WKT', output_path]

    duplicate_geom_field = False

    # if we're exporting already clipped data from PostGIS . . .
    if tile.postgis_table:

        conn_str = util.build_ogr_pg_conn()

        sql = "SELECT '{}' as table__name, * FROM {}".format(output_name, tile.postgis_table)
        cmd += [conn_str, '-sql', sql, '-lco', 'GEOMETRY_NAME=geom']

        csv_output = os.path.join(output_path, 'sql_statement.csv')

    # or if we're clipping an existing TSV
    else:
        cmd += [tile.dataset, '-clipsrc'] + [str(x) for x in tile.bbox]
        csv_output = os.path.join(output_path, 'data.csv')

        # ogr2ogr duplicates this for some reason
        duplicate_geom_field = True

    logging.info(cmd)
    subprocess.check_call(cmd)

    df = pd.read_csv(csv_output)

    if duplicate_geom_field:
        del df['field_1']

    tsv_output = os.path.join(layer_dir, '{}__{}.tsv'.format(output_name, tile.tile_id))
    df.to_csv(tsv_output, sep='\t', header=None, index=False)

    tile.final_output = tsv_output


def intersect_gadm(source_layer, gadm_layer):

    input_list = []

    output_layer = layer.Layer(None, [])

    for t in source_layer.tile_list:
        input_list.append((output_layer, t, gadm_layer.tile_list[0]))

    util.exec_multiprocess(intersect, input_list)

    return output_layer


def intersect_layers(layer_a, layer_b):

    input_list = []

    output_layer = layer.Layer(None, [])

    for a, b in zip(layer_a.tile_list, layer_b.tile_list):
        input_list.append((output_layer, a, b))

    util.exec_multiprocess(intersect, input_list)

    return output_layer

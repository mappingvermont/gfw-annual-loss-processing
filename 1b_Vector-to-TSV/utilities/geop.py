import os
import subprocess
import psycopg2
import logging

import util, tile, layer, postgis_util as pg_util


def tabulate_area(q):
    while True:
        tile = q.get()

        creds = pg_util.get_creds()
        conn = psycopg2.connect(**creds)
        cursor = conn.cursor()

        col_list = ['polyname', 'boundary_field1', 'boundary_field2',
                    'boundary_field3', 'boundary_field4', 'iso', 'id_1', 'id_2']
        col_str = ', '.join(col_list)

        sql = ("INSERT INTO aoi_area "
               "SELECT {c}, sum(ST_Area(geography(geom))) / 10000 AS area_ha "
               "FROM {t} "
               "GROUP BY {c} ".format(t=tile.postgis_table, c=col_str))

        cursor.execute(sql)
        conn.commit()

        conn.close()
        q.task_done()


def clip(q):
    while True:
        tile = q.get()

        conn_str = pg_util.build_ogr_pg_conn()
        col_str = pg_util.boundary_field_dict_to_sql_str(tile.col_list)

        if not tile.postgis_table:
            dataset_name = os.path.splitext(os.path.basename(tile.dataset))[0]
            tile.postgis_table = '_'.join([dataset_name, tile.tile_id, 'clip']).lower()

        if pg_util.check_table_exists(tile.postgis_table):
            pass

        else:

            # any TSV name will have the data as it's layer, otherwise use the actual shape
            file_ext = os.path.splitext(tile.dataset)[1]

            # why VRT here? so we can read the TSVs that are already created
            # which requires a crazy vector VRT file
            if file_ext in ['.shp', '.tsv', '.vrt']:
                if file_ext == '.shp':
                    ogr_layer_name = os.path.splitext(os.path.basename(tile.dataset))[0]
                else:
                    ogr_layer_name = 'data'

                sql = "SELECT {}, GEOMETRY FROM {}".format(col_str, ogr_layer_name)

                cmd = ['ogr2ogr', '-f', 'PostgreSQL', conn_str, tile.dataset, '-nln', tile.postgis_table,
                       '-nlt', 'GEOMETRY', '-dialect', 'sqlite', '-sql', sql, '-lco', 'geometry_name=geom',
                       '-overwrite', '-s_srs', 'EPSG:4326', '-t_srs', 'EPSG:4326', '-dim', '2']

                if tile.bbox:
                    bbox_list = [str(x) for x in tile.bbox]
                    cmd += ['-clipsrc'] + bbox_list

            elif file_ext == '.tif':
                cmd = ['gdal_polygonize.py', tile.dataset, '-f', 'PostgreSQL',
                       conn_str, tile.postgis_table, 'boundary_field1']

            else:
                raise ValueError('Unknown file extension {}, expecting shp, tif or tsv'.format(file_ext))

            logging.info(cmd)

            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in iter(p.stdout.readline, b''):
                if 'error' in line.lower():
                    logging.error('Error in loading dataset, {}'.format(cmd))

            creds = pg_util.get_creds()
            conn = psycopg2.connect(**creds)
            cursor = conn.cursor()
            
            # a few other things required to get our raster data to match vector
            if file_ext == '.tif':
                sql_list = ["ALTER TABLE {} RENAME wkb_geometry to geom",
                            "ALTER TABLE {} ADD COLUMN boundary_field2 integer",
                            "UPDATE {} SET boundary_field2 = 1"]

                for sql in sql_list:
                    cursor.execute(sql.format(tile.postgis_table))

            pg_util.fix_geom(tile.postgis_table, cursor)
            
            conn.commit()
            conn.close()

        q.task_done()


def intersect(q):
    # source: https://pymotw.com/2/Queue/
    while True:
        output_layer, tile1, tile2 = q.get()

        creds = pg_util.get_creds()

        conn = psycopg2.connect(**creds)
        cursor = conn.cursor()

        table_name = '{}_{}_{}'.format(tile1.postgis_table, tile2.postgis_table, tile1.tile_id)

        if pg_util.table_has_rows(cursor, tile1.postgis_table):

            # find which table has ISO/ID_1/ID_2 columns
            # will return ['a.ISO', 'a.ID_1', 'a._ID_2'] or b., depending on if first or second tile has admin columns
            admin2_columns = pg_util.find_admin_columns(cursor, tile1, tile2)

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
                   "a.geom), ST_MakeValid(b.geom))), 0.0000001)))).geom as geom "
                   "FROM {table1} a, {table2} b "
                   "WHERE ST_Intersects(a.geom, b.geom) "
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
            
                pg_util.fix_geom(table_name, cursor)

                if pg_util.table_has_rows(cursor, table_name):
                    output_tile = tile.Tile(None, None, tile1.tile_id, tile1.bbox, table_name)
                    output_layer.tile_list.append(output_tile)
            
        conn.commit()
        conn.close()

        q.task_done()


def intersect_gadm(source_layer, gadm_layer):
    input_list = []

    output_layer = layer.Layer(None, [])

    for t in source_layer.tile_list:
        input_list.append((output_layer, t, gadm_layer.tile_list[0]))

    util.exec_multiprocess(intersect, input_list)

    return output_layer


def vectorize(q):
    while True:
        output_dir, tile = q.get()

        dataset_name = os.path.splitext(os.path.basename(tile.dataset))[0]
        tiled_fname = '_'.join([dataset_name, tile.tile_id, 'clip']) + '.tif'
        clipped_ras = os.path.join(output_dir, tiled_fname)

        # stringify bbox and then reorder for gdal_translate
        bbox_list = [str(x) for x in tile.bbox]
        ordered = [0, 3, 2, 1]
        bbox_list = [bbox_list[i] for i in ordered]

        clip_cmd = ['gdal_translate', tile.dataset, clipped_ras,
                    '-co', 'COMPRESS=LZW', '-projwin'] + bbox_list

        logging.info(clip_cmd)
        subprocess.check_call(clip_cmd)

        # set this clipped tile as our input dataset
        # we can use this with the clip function above, using gdal_polygonize
        # instead of the standard ogr2ogr approach
        tile.dataset = clipped_ras
        tile.bbox = None

        q.task_done()


def raster_to_vector(layer_dir, tile_list):
    input_list = []

    for t in tile_list:
        input_list.append((layer_dir, t))

    util.exec_multiprocess(vectorize, input_list)

    return


def intersect_layers(layer_a, layer_b):
    input_list = []

    output_layer = layer.Layer(None, [])

    # need to sort tiles to make sure both lists line up
    layer_a.tile_list.sort(key=lambda x: x.tile_id)
    layer_b.tile_list.sort(key=lambda x: x.tile_id)

    for a, b in zip(layer_a.tile_list, layer_b.tile_list):
        input_list.append((output_layer, a, b))

    util.exec_multiprocess(intersect, input_list)

    return output_layer

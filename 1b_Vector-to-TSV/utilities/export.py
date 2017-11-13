import os
import logging
import subprocess

import util


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

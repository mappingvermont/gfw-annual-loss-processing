import os
import logging
import subprocess

import pandas as pd

import postgis_util as pg_util


def export(q):
    while True:

        layer_dir, output_name, tile = q.get()

        conn_str = pg_util.build_ogr_pg_conn()

        # for some reason ogr2ogr wants to create a directory and THEN the CSV
        output_path = os.path.join(layer_dir, '{}'.format(tile.tile_id))

        conn_str = pg_util.build_ogr_pg_conn()
        sql = "SELECT '{}' as table__name, * FROM {}".format(output_name, tile.postgis_table)

        cmd = ['ogr2ogr', '-f', 'CSV', '-lco', 'GEOMETRY=AS_WKT', output_path,
               conn_str, '-sql', sql, '-lco', 'GEOMETRY_NAME=geom']

        csv_output = os.path.join(output_path, 'sql_statement.csv')

        logging.info(cmd)
        subprocess.check_call(cmd)

        df = pd.read_csv(csv_output)

        tsv_output = os.path.join(layer_dir, '{}__{}.tsv'.format(output_name, tile.tile_id))
        df.to_csv(tsv_output, sep='\t', header=None, index=False)

        tile.final_output = tsv_output

        if tile.postgis_table:
            pg_util.drop_table(tile.postgis_table)

        q.task_done()


import os
import psycopg2
import subprocess
import logging

import util
from layer import Layer
from tile import Tile


def load():

    creds = util.get_creds()

    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    boundary_fields = [{'boundary_field1': 'boundary_field1'}, {'boundary_field2': 'boundary_field2'}]

    if check_table_exists(cursor, 'adm2_final'):
        logging.info('GADM 28 data already in PostGIS')

    else:
        gadm28_shp = download_gadm28()

        table_name = insert_into_postgis(creds, gadm28_shp, boundary_fields)

        fix_geometry(cursor, table_name)

        conn.commit()

    conn.close()

    l = Layer('adm2_final', [])
    l.tile_list = [Tile(l.input_dataset, boundary_fields, None, None, l.input_dataset)]

    return l


def insert_into_postgis(creds, src_shp, dummy_fields):

    conn_str = 'postgresql://{user}:{password}@{host}'.format(**creds)

    cmd = ['shp2pgsql', '-I', '-s', '4326', src_shp, '|', 'psql', conn_str]

    # has to be string for some reason-- likely to do with the | required
    subprocess.check_call(' '.join(cmd), shell=True)

    table_name = os.path.splitext(os.path.basename(src_shp))[0]

    # add dummy column names
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # add these to match schema of other intersects
    for field_name in dummy_fields:
        cursor.execute('ALTER TABLE {} ADD COLUMN {} varchar(30)'.format(table_name, field_name))
        cursor.execute("UPDATE {} SET {} = '1'".format(table_name, field_name))

    conn.commit()
    conn.close()

    return table_name


def fix_geometry(cursor, table_name):

    sql = "UPDATE {} SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) <> '1'".format(table_name)
    logging.info(sql)

    cursor.execute(sql)


def download_gadm28():

    zip_name = 'gadm28_adm2_final.zip'

    logging.info('loading gadm28 table into postGIS')
    s3_src = r's3://gfw2-data/alerts-tsv/gis_source/' + zip_name

    out_dir = r'/tmp/'
    out_file = os.path.join(out_dir, zip_name)

    download_cmd = ['aws', 's3', 'cp', s3_src, out_file]
    subprocess.check_call(download_cmd)

    unzip_cmd = ['unzip', out_file]
    subprocess.check_call(unzip_cmd, cwd=out_dir)

    return os.path.join(out_dir + 'adm2_final.shp')


def check_table_exists(cursor, table_name):

    # source: https://stackoverflow.com/questions/1874113/
    sql = "select exists(select * from information_schema.tables where table_name=%s)"
    cursor.execute(sql, (table_name,))

    return cursor.fetchone()[0]


import os
import psycopg2
import subprocess
import logging

import util
from layer import Layer
from tile import Tile


def load(zip_source):

    creds = util.get_creds()

    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    boundary_fields = [{'boundary_field1': 'boundary_field1'}, {'boundary_field2': 'boundary_field2'}]

    table_name = os.path.splitext(os.path.basename(zip_source))[0]
    if util.check_table_exists(cursor, table_name):
        logging.info('{} data already in PostGIS'.format(zip_source))
    
    else:

        gadm28_shp = download_gadm28(zip_source)

        insert_into_postgis(creds, gadm28_shp, boundary_fields, table_name)

        fix_geometry(cursor, table_name)

        conn.commit()

    conn.close()

    l = Layer(table_name, [])
    l.tile_list = [Tile(l.input_dataset, boundary_fields, None, None, l.input_dataset)]

    return l


def insert_into_postgis(creds, src_shp, dummy_fields, table_name):

    conn_str = 'postgresql://{user}:{password}@{host}'.format(**creds)

    cmd = ['shp2pgsql', '-I', '-s', '4326', src_shp, '|', 'psql', conn_str]

    # has to be string for some reason-- likely to do with the | required
    subprocess.check_call(' '.join(cmd), shell=True)

    # add dummy column names
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # add these to match schema of other intersects
    for field_dict in dummy_fields:
        field_name = field_dict.keys()[0]
        cursor.execute('ALTER TABLE {} ADD COLUMN {} varchar(30)'.format(table_name, field_name))
        cursor.execute("UPDATE {} SET {} = '1'".format(table_name, field_name))

    conn.commit()
    conn.close()


def fix_geometry(cursor, table_name):

    sql = "UPDATE {} SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) <> '1'".format(table_name)
    logging.info(sql)

    cursor.execute(sql)


def download_gadm28(s3_source):

    logging.info('loading {} into postGIS'.format(s3_source))

    if s3_source: # s3 _source is true if user is not loading regular gadm28. Name is confusing so I don't have to re-define s3_source
        zip_name = s3_source.split("/")[-1:][0] # this would be like s3://gfw-files/source.zip -> source.zip
        unzipped_path = zip_name.replace('zip', 'shp') # this will work as long as shapefile is same name as zip file

    else:
        zip_name = 'gadm28_adm2_final.zip'
        s3_src = r's3://gfw2-data/alerts-tsv/gis_source/' + zip_name
        unzipped_path = 'adm2_final.shp'

    out_dir = r'/tmp/'
    out_file = os.path.join(out_dir, zip_name)

    download_cmd = ['aws', 's3', 'cp', s3_src, out_file]
    print download_cmd
    subprocess.check_call(download_cmd)

    unzip_cmd = ['unzip', out_file]
    print unzip_cmd
    subprocess.check_call(unzip_cmd, cwd=out_dir)
    print os.path.join(out_dir + unzipped_path)

    return os.path.join(out_dir + unzipped_path)



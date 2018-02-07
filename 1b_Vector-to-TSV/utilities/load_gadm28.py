import os
import subprocess
import logging

import postgis_util as pg_util
from layer import Layer
from tile import Tile


def load(zip_source):

    boundary_fields = [{'boundary_field1': 'boundary_field1'}, {'boundary_field2': 'boundary_field2'}]

    table_name = os.path.splitext(os.path.basename(zip_source))[0]
    if pg_util.check_table_exists(table_name):
        logging.info('{} data already in PostGIS'.format(zip_source))
    
    else:
        gadm28_shp = download_gadm28(zip_source)

        pg_util.insert_into_postgis(gadm28_shp, boundary_fields)
        
        conn, cursor = pg_util.conn_to_postgis()
        
        pg_util.fix_geom(table_name, cursor, False)
        
        conn.close()
        
    l = Layer(table_name, [])
    l.tile_list = [Tile(l.input_dataset, boundary_fields, None, None, l.input_dataset)]

    return l

    
def download_gadm28(s3_src):

    logging.info('loading {} into postGIS'.format(s3_src))

    zip_name = s3_src.split("/")[-1:][0] # this would be like s3://gfw-files/source.zip -> source.zip
    unzipped_path = zip_name.replace('zip', 'shp') # this will work as long as shapefile is same name as zip file

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



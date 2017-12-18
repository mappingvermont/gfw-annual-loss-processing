import os
# import psycopg2
# import subprocess
# import logging

# import util
# from layer import Layer
# from tile import Tile


def load(zip_source, unzipname):

    creds = util.get_creds()

    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    boundary_fields = [{'boundary_field1': 'boundary_field1'}, {'boundary_field2': 'boundary_field2'}]

    if util.check_table_exists(cursor, 'adm2_final'):
        logging.info('GADM 28 data already in PostGIS')
    
    else:
        gadm28_shp = download_gadm28(zip_source, unzipnam)

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
    for field_dict in dummy_fields:
        field_name = field_dict.keys()[0]
        cursor.execute('ALTER TABLE {} ADD COLUMN {} varchar(30)'.format(table_name, field_name))
        cursor.execute("UPDATE {} SET {} = '1'".format(table_name, field_name))

    conn.commit()
    conn.close()

    return table_name


def fix_geometry(cursor, table_name):

    sql = "UPDATE {} SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) <> '1'".format(table_name)
    logging.info(sql)

    cursor.execute(sql)


def download_gadm28(zip_source, unzipname):
    
    if zip_source:
        #logging.info('loading custom source table into postGIS')
        zip_name = zip_source.split("/")[-1:][0]
        s3_src = zip_source
        unzipped_path = unzipname
    else:
        #logging.info('loading gadm28 table into postGIS')
        zip_name = 'gadm28_adm2_final.zip'
        s3_src = r's3://gfw2-data/alerts-tsv/gis_source/' + zip_name
        unzipped_path = 'adm2_final.shp'
    
    out_dir = r'/tmp/'
    out_file = os.path.join(out_dir, zip_name)

    download_cmd = ['aws', 's3', 'cp', s3_src, out_file]
    print download_cmd
    #subprocess.check_call(download_cmd)

    unzip_cmd = ['unzip', out_file]
    print unzip_cmd
    #subprocess.check_call(unzip_cmd, cwd=out_dir)
    print os.path.join(out_dir + unzipped_path)
    return os.path.join(out_dir + unzipped_path)


download_gadm28('s3://gfw-files/sam/test.zip', 'thisisatest.shp')
# download_gadm28(None, None)
import os
import uuid
import multiprocessing
from threading import Thread
from Queue import Queue
import psycopg2
import logging
import math
import numpy as np

import fiona
import rasterio
from shapely.geometry import shape

from tile import Tile


def get_creds():
    try:
        creds = {'host': os.environ['PG_HOST'],
                 'user': os.environ['PG_USER'],
                 'password': os.environ['PG_PASS'],
                 'dbname': os.environ['PG_DBNAME']}

    except KeyError:
        creds = {'host': 'localhost', 'password': 'gis', 'dbname': 'gis', 'user': 'gis'}

    return creds


def build_ogr_pg_conn():
    creds = get_creds()

    return 'PG:user={user} password={password} dbname={dbname} host={host}'.format(**creds)

def create_temp_dir():

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, 'data')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    guid = str(uuid.uuid4())
    layer_dir = os.path.join(data_dir, guid)

    os.mkdir(layer_dir)

    return layer_dir


def fiod_admin_columns(cursor, tile1, tile2):

    tile_with_cols = None
    iso_col_list = ['iso', 'id_1', 'id_2']

    for tile_id, t in zip(('a', 'b'), (tile1, tile2)):
        cursor.execute("select column_name from information_schema.columns WHERE table_name=%s", (t.postgis_table,))
        col_names = [row[0] for row in cursor]

        # check if iso col list is part of the larger table column list
        if set(iso_col_list).issubset(set(col_names)):
            tile_with_cols = tile_id

    if not tile_with_cols:
        raise ValueError('Neither {} or {} have iso, id_1, id_2 columns'.format(tile1.postgis_table, tile2.postgis_table))

    # a.ISO, a.ID_1, a.ID_2
    return [tile_with_cols + '.' + x for x in iso_col_list]


def drop_table(tablename):

    creds = get_creds()
    conn = psycopg2.connect(**creds)

    cursor = conn.cursor()
    cursor.execute('DROP TABLE {}'.format(tablename))

    conn.commit()
    conn.close()


def boundary_field_dict_to_sql_str(field_list):
    out_list = []

    for field_dict in field_list:
        out_list += ['{} AS {}'.format(k, v) for k, v in field_dict.iteritems()]

    return ', '.join(out_list)


def table_has_rows(cursor, table_name):
    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows


def exec_multiprocess(input_func, input_list, is_test=False):

    # create queue
    q = Queue()

    for i in input_list:
        q.put(i)

    # start our threads
    if is_test:
        mp_count = 1
    else:
        mp_count = multiprocessing.cpu_count() - 1

    for i in range(mp_count):
        worker = Thread(target=input_func, args=(q,))
        worker.setDaemon(True)
        worker.start()

    # process all jobs in the queue
    q.join()


def build_gadm28_tile_list(source_layer, is_test):

    logging.info('Building tile list')
    logging.info('checking extent of input geometry {}'.format(source_layer.input_dataset))

    # shapefile of tiles used to tsv aoi
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tiles = fiona.open(os.path.join(root_dir, 'grid', 'lossdata_footprint_filter.geojson'), 'r')

    # aoi we want to tsv
    if os.path.splitext(source_layer.input_dataset)[1] == '.tif':
        with rasterio.open(source_layer.input_dataset) as src:
            aoi_bounds = src.bounds
    else:
        aoi_bounds = fiona.open(source_layer.input_dataset).bounds

    # select tiles that are inside of the bounding box of the aoi
    tiles_in_aoi = tiles.filter(bbox=aoi_bounds)

    for feat in tiles_in_aoi:
        # get the bounding box of the 1deg tile
        bbox = shape(feat['geometry']).bounds

        # get the tile id- used for naming
        # tile_id = feat['properties']['ulx_uly']
        tile_id = feat['properties']['ID']

        # build the tile object
        t = Tile(source_layer.input_dataset, source_layer.col_list, tile_id, bbox)

        # add the tile bbox to the tile_list
        source_layer.tile_list.append(t)

    # if this is a test, only do one tile
    if is_test:
        source_layer.tile_list = source_layer.tile_list[0:1]


def start_logging():

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file = os.path.join(root_dir, 'output.log')
    logging.basicConfig(filename=log_file, level=logging.INFO)

    logging.getLogger('botocore').setLevel(logging.ERROR)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


def check_table_exists(cursor, table_name):

    # source: https://stackoverflow.com/questions/1874113/
    sql = "select exists(select * from information_schema.tables where table_name=%s)"
    cursor.execute(sql, (table_name,))

    return cursor.fetchone()[0]


def create_area_table():

    creds = get_creds()
    conn = psycopg2.connect(**creds)

    cursor = conn.cursor()

    if not check_table_exists(cursor, 'aoi_area'):

        sql = ("CREATE TABLE aoi_area ( "
               "polyname character varying, "
               "boundary_field1 character varying, "
               "boundary_field2 character varying, "
               "boundary_field3 character varying, "
               "boundary_field4 character varying, "
               "iso character varying, "
               "id_1 character varying, "
               "id_2 character varying, "
               "area_ha double precision "
               ");")

        cursor.execute(sql)
        conn.commit()

    conn.close()


def get_pixel_area(lat):
    """
    Calculate geodesic area for Hansen data, assuming a fix pixel size of 0.00025 * 0.00025 degree
    using WGS 1984 as spatial reference.
    Pixel size various with latitude, which is it the only input parameter
    """
    a = 6378137.0  # Semi major axis of WGS 1984 ellipsoid
    b = 6356752.314245179  # Semi minor axis of WGS 1984 ellipsoid

    d_lat = 0.00025  # pixel hight
    d_lon = 0.00025  # pixel width

    pi = math.pi

    q = d_lon / 360
    e = math.sqrt(1 - (b / a) ** 2)

    area = abs(
        (pi * b ** 2 * (
            2 * np.arctanh(e * np.sin(np.radians(lat + d_lat))) /
            (2 * e) +
            np.sin(np.radians(lat + d_lat)) /
            ((1 + e * np.sin(np.radians(lat + d_lat))) * (1 - e * np.sin(np.radians(lat + d_lat)))))) -
        (pi * b ** 2 * (
            2 * np.arctanh(e * np.sin(np.radians(lat))) / (2 * e) +
            np.sin(np.radians(lat)) / ((1 + e * np.sin(np.radians(lat))) * (1 - e * np.sin(np.radians(lat))))))) * q

    return area

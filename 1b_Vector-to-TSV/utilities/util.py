import os
import multiprocessing
from threading import Thread
from Queue import Queue
import psycopg2

import fiona
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


def drop_table(tablename):

    creds = get_creds()
    conn = psycopg2.connect(**creds)

    cursor = conn.cursor()
    cursor.execute('DROP TABLE {}'.format(tablename))

    conn.commit()
    conn.close()


def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings


def boundary_field_to_sql(field_name):

    if 'boundary_field' in field_name:
        field_name = '1 AS ' + field_name

    return field_name


def table_has_rows(cursor, table_name):
    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows


def exec_multiprocess(input_func, input_list):

    # create queue
    q = Queue()

    for i in input_list:
        q.put(i)

    # start our threads
    # mp_count = multiprocessing.cpu_count() - 1
    mp_count = 1

    for i in range(mp_count):
        worker = Thread(target=input_func, args=(q,))
        worker.setDaemon(True)
        worker.start()

    # process all jobs in the queue
    q.join()


def build_gadm28_tile_list(source_layer, is_test):

    print 'Building tile list'
    print 'checking extent of input geometry {}'.format(source_layer.input_dataset)

    # shapefile of tiles used to tsv aoi
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tiles = fiona.open(os.path.join(root_dir, 'grid', 'lossdata_footprint_filter.geojson'), 'r')

    # aoi we want to tsv
    aoi = fiona.open(source_layer.input_dataset)

    # select tiles that are inside of the bounding box of the aoi
    tiles_in_aoi = tiles.filter(bbox=aoi.bounds)

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
